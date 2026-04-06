"""Sportsbook bias update job.

Feeds settled outcomes back into the bias estimator so that consensus
probabilities are corrected for systematic per-book errors over time.

Flow:
1. Find outcomes settled since the last bias update
2. For each settled market, find each book's closing quote per side
3. Create BiasUpdateInput (book_prob, outcome_hit) for each observation
4. Run BiasEstimator.compute_batch_updates()
5. Persist updated bias states to sportsbook_bias

This job should run after OutcomeScoreHandler in the scoring pipeline.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List

from sqlalchemy import text

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params
from sparket.validator.scoring.ground_truth.bias import (
    BiasEstimator,
    BiasKey,
    BiasState,
    BiasUpdateInput,
    make_bias_key,
)
from .base import ScoringJob


# Find outcomes settled since last bias update, joined with market metadata.
_SELECT_NEW_SETTLED_OUTCOMES = text("""
    SELECT
        o.outcome_id,
        o.market_id,
        o.result,
        o.settled_at,
        m.kind AS market_kind,
        l.sport_id,
        e.start_time_utc
    FROM outcome o
    JOIN market m ON m.market_id = o.market_id
    JOIN event e ON e.event_id = m.event_id
    JOIN league l ON l.league_id = e.league_id
    WHERE o.settled_at > :since
    ORDER BY o.settled_at
""")

# For each settled market, get the latest provider quote per (sportsbook, side)
# within 24h before event start (matches closing snapshot lookback window).
_SELECT_CLOSING_BOOK_QUOTES = text("""
    SELECT
        COALESCE(s.sportsbook_id, 0) AS sportsbook_id,
        pq.side,
        pq.imp_prob
    FROM provider_quote pq
    JOIN market m ON m.market_id = pq.market_id
    JOIN event e ON e.event_id = m.event_id
    LEFT JOIN sportsbook s ON s.provider_id = pq.provider_id
        AND s.code = pq.raw->>'sportsbook'
    WHERE pq.market_id = :market_id
      AND pq.ts >= e.start_time_utc - INTERVAL '24 hours'
      AND pq.ts < e.start_time_utc
    ORDER BY pq.ts DESC
""")

# Load current bias states.
_SELECT_CURRENT_BIAS = text("""
    SELECT
        sportsbook_id, sport_id, market_kind,
        bias_factor, variance, mean_squared_error, sample_count, version
    FROM sportsbook_bias
""")

# Upsert updated bias state.
_UPSERT_BIAS = text("""
    INSERT INTO sportsbook_bias (
        sportsbook_id, sport_id, market_kind,
        bias_factor, variance, mean_squared_error, sample_count, version, computed_at
    ) VALUES (
        :sportsbook_id, :sport_id, :market_kind,
        :bias_factor, :variance, :mse, :sample_count, :version, :computed_at
    )
    ON CONFLICT (sportsbook_id, sport_id, market_kind) DO UPDATE SET
        bias_factor = EXCLUDED.bias_factor,
        variance = EXCLUDED.variance,
        mean_squared_error = EXCLUDED.mean_squared_error,
        sample_count = EXCLUDED.sample_count,
        version = EXCLUDED.version,
        computed_at = EXCLUDED.computed_at
""")

# Side mapping: outcome.result → list of (side, hit) pairs.
# For each side of a market, determine if the outcome hit that side.
_SIDES_BY_KIND: Dict[str, Dict[str, str]] = {
    "MONEYLINE": {"HOME": "HOME", "AWAY": "AWAY", "DRAW": "DRAW"},
    "SPREAD": {"HOME": "HOME", "AWAY": "AWAY"},
    "TOTAL": {"OVER": "OVER", "UNDER": "UNDER"},
    "DRAW_NO_BET": {"HOME": "HOME", "AWAY": "AWAY"},
}


class BiasUpdateJob(ScoringJob):
    """Update sportsbook bias estimates from settled outcomes.

    Runs after outcome scoring in the main pipeline. Collects each
    book's closing probability for settled markets and feeds them
    into the EMA bias estimator.
    """

    JOB_ID = "bias_update_v1"
    CHECKPOINT_INTERVAL = 200

    def __init__(self, db: Any, logger: Any, *, job_id_override: str | None = None):
        super().__init__(db, logger, job_id_override=job_id_override)
        self.params = get_scoring_params()
        self.estimator = BiasEstimator(self.params)

    async def execute(self) -> None:
        """Execute the bias update job."""
        # Determine lookback: use last computed_at from any bias row,
        # or fall back to 60 days.
        since = await self._get_last_update_time()

        self.logger.info(f"BiasUpdateJob: looking for outcomes settled since {since}")

        # Load current bias states
        current_bias = await self._load_current_bias()

        # Find settled outcomes
        outcomes = await self.db.read(
            _SELECT_NEW_SETTLED_OUTCOMES,
            params={"since": since},
            mappings=True,
        )

        if not outcomes:
            self.logger.info("BiasUpdateJob: no new settled outcomes")
            return

        self.items_total = len(outcomes)
        self.logger.info(f"BiasUpdateJob: processing {self.items_total} settled outcomes")

        # Collect all bias observations
        all_observations: List[BiasUpdateInput] = []

        for idx, outcome in enumerate(outcomes):
            obs = await self._collect_observations(outcome)
            all_observations.extend(obs)

            self.items_processed = idx + 1
            await self.checkpoint_if_due()

        if not all_observations:
            self.logger.info("BiasUpdateJob: no book quotes found for settled outcomes")
            return

        self.logger.info(
            f"BiasUpdateJob: collected {len(all_observations)} observations "
            f"from {self.items_total} outcomes"
        )

        # Run bias estimator
        updates = self.estimator.compute_batch_updates(all_observations, current_bias)

        # Persist
        now = datetime.now(timezone.utc)
        for key, state in updates.items():
            await self.db.write(
                _UPSERT_BIAS,
                params={
                    "sportsbook_id": state.sportsbook_id,
                    "sport_id": state.sport_id,
                    "market_kind": state.market_kind,
                    "bias_factor": float(state.bias_factor),
                    "variance": float(state.variance),
                    "mse": float(state.mse),
                    "sample_count": state.sample_count,
                    "version": state.version,
                    "computed_at": now,
                },
            )

        self.logger.info(
            f"BiasUpdateJob: updated {len(updates)} bias entries "
            f"from {len(all_observations)} observations"
        )

    async def _collect_observations(
        self, outcome: Dict[str, Any]
    ) -> List[BiasUpdateInput]:
        """Collect bias observations for a single settled outcome.

        For each book that quoted this market near closing, creates a
        BiasUpdateInput comparing the book's probability to the outcome.
        """
        market_id = outcome["market_id"]
        result = str(outcome["result"])  # e.g. "HOME", "AWAY", "OVER", "UNDER"
        market_kind = str(outcome["market_kind"])  # e.g. "MONEYLINE", "TOTAL"
        sport_id = outcome["sport_id"]

        # Get each book's closing quotes for this market
        quotes = await self.db.read(
            _SELECT_CLOSING_BOOK_QUOTES,
            params={"market_id": market_id},
            mappings=True,
        )

        if not quotes:
            return []

        # Deduplicate: keep latest quote per (sportsbook, side)
        # Query is ordered by ts DESC, so first seen wins.
        seen: set[tuple[int, str]] = set()
        deduped = []
        for q in quotes:
            key = (q["sportsbook_id"], str(q["side"]))
            if key not in seen:
                seen.add(key)
                deduped.append(q)

        observations = []
        for q in deduped:
            book_id = q["sportsbook_id"]
            side = str(q["side"])
            book_prob = q["imp_prob"]

            if book_prob is None or book_id == 0:
                continue

            # Did this side win?
            outcome_hit = 1 if side == result else 0

            observations.append(
                BiasUpdateInput(
                    sportsbook_id=book_id,
                    sport_id=sport_id,
                    market_kind=market_kind,
                    book_prob=Decimal(str(book_prob)),
                    outcome_hit=outcome_hit,
                )
            )

        return observations

    async def _load_current_bias(self) -> Dict[BiasKey, BiasState]:
        """Load all current bias states from the database."""
        rows = await self.db.read(_SELECT_CURRENT_BIAS, mappings=True)

        states = {}
        for row in rows:
            key = make_bias_key(
                row["sportsbook_id"],
                row["sport_id"],
                str(row["market_kind"]),
            )
            states[key] = BiasState(
                sportsbook_id=row["sportsbook_id"],
                sport_id=row["sport_id"],
                market_kind=str(row["market_kind"]),
                bias_factor=Decimal(str(row["bias_factor"])),
                variance=Decimal(str(row["variance"])),
                mse=Decimal(str(row.get("mean_squared_error") or "0")),
                sample_count=row["sample_count"],
                version=row["version"],
            )

        return states

    async def _get_last_update_time(self) -> datetime:
        """Get the timestamp of the last bias update, or a default lookback."""
        rows = await self.db.read(
            text("SELECT MAX(computed_at) AS last FROM sportsbook_bias WHERE sample_count > 0"),
            mappings=True,
        )

        if rows and rows[0]["last"]:
            return rows[0]["last"]

        # No prior updates — look back 60 days
        return datetime.now(timezone.utc) - timedelta(days=60)


__all__ = ["BiasUpdateJob"]
