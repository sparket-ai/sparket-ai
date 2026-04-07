"""Shapley Contribution scoring job.

Computes the Marginal dimension for the Cobb-Douglas SkillScore by:
1. Fetching recently settled markets (hourly batches)
2. For each market: Monte Carlo Shapley estimation via log-pool aggregation
3. Time-decaying Shapley values into rolling shapley_mean
4. Normalising via z-score logistic → marginal_dim

Depends on: Log-pool module, previous epoch's emission weights
Feeds into: SkillScoreJob (provides marginal_dim)
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

import numpy as np
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params

from ..aggregation.decay import compute_decay_weights, weighted_mean
from ..aggregation.normalization import normalize_zscore_logistic, normalize_percentile
from ..aggregation.shapley import monte_carlo_shapley
from ..determinism import get_canonical_window_bounds
from .base import ScoringJob


_SELECT_RECENTLY_SETTLED = text(
    """
    SELECT DISTINCT ms.market_id, sos.settled_at
    FROM submission_outcome_score sos
    JOIN miner_submission ms ON ms.submission_id = sos.submission_id
    WHERE sos.settled_at > :since
      AND sos.settled_at <= :until
      AND NOT EXISTS (
          SELECT 1 FROM shapley_contribution sc
          WHERE sc.market_id = ms.market_id
      )
    ORDER BY sos.settled_at
    """
)

_SELECT_MARKET_SUBMISSIONS = text(
    """
    SELECT
        ms.miner_id,
        ms.side,
        ms.imp_prob
    FROM miner_submission ms
    JOIN submission_outcome_score sos
      ON sos.submission_id = ms.submission_id
    WHERE ms.market_id = :market_id
    ORDER BY ms.miner_id, ms.side
    """
)

_SELECT_MARKET_OUTCOMES = text(
    """
    SELECT side, result
    FROM outcome
    WHERE market_id = :market_id
    """
)

_SELECT_PREVIOUS_WEIGHTS = text(
    """
    SELECT miner_id, skill_score
    FROM miner_rolling_score
    WHERE as_of = (
        SELECT MAX(as_of) FROM miner_rolling_score
        WHERE as_of < :current_as_of
    )
      AND window_days = :window_days
    ORDER BY miner_id
    """
)

_INSERT_SHAPLEY = text(
    """
    INSERT INTO shapley_contribution
        (miner_id, market_id, shapley_value, settled_at, k_permutations)
    VALUES (:miner_id, :market_id, :shapley_value, :settled_at, :k_permutations)
    ON CONFLICT (miner_id, market_id) DO UPDATE
        SET shapley_value = :shapley_value,
            settled_at = :settled_at,
            k_permutations = :k_permutations
    """
)

_SELECT_SHAPLEY_HISTORY = text(
    """
    SELECT miner_id, shapley_value, settled_at
    FROM shapley_contribution
    WHERE settled_at >= :window_start
      AND settled_at <= :window_end
    ORDER BY miner_id, settled_at
    """
)

_SELECT_MINERS_IN_ROLLING = text(
    """
    SELECT miner_id, miner_hotkey
    FROM miner_rolling_score
    WHERE as_of = :as_of
      AND window_days = :window_days
    ORDER BY miner_id
    """
)

_UPDATE_SHAPLEY_SCORES = text(
    """
    UPDATE miner_rolling_score
    SET shapley_mean = :shapley_mean,
        shapley_ws = :shapley_ws,
        shapley_wt = :shapley_wt,
        marginal_dim = :marginal_dim
    WHERE miner_id = :miner_id
      AND miner_hotkey = :miner_hotkey
      AND as_of = :as_of
      AND window_days = :window_days
    """
)


def _deterministic_seed(seed_prefix: int, market_id: int) -> int:
    """Generate a deterministic seed from prefix + market_id."""
    h = hashlib.sha256(f"{seed_prefix}:{market_id}".encode()).hexdigest()
    return int(h[:8], 16)


class ShapleyContributionJob(ScoringJob):
    """Compute Monte Carlo Shapley values on settled markets."""

    JOB_ID = "shapley"

    def __init__(
        self,
        db: Any,
        logger: logging.Logger,
        *,
        as_of: datetime | None = None,
        job_id_override: str | None = None,
    ):
        super().__init__(db, logger, job_id_override=job_id_override)
        self.params = get_scoring_params()
        self.as_of = as_of

    async def execute(self) -> None:
        """Compute Shapley values for recently settled markets."""
        shapley_params = self.params.shapley
        window_days = self.params.windows.rolling_window_days

        if self.as_of is not None:
            as_of = self.as_of
        else:
            _, as_of = get_canonical_window_bounds(window_days)

        window_start, _ = get_canonical_window_bounds(window_days, reference_time=as_of)

        # Look back one batch interval for newly settled markets
        from datetime import timedelta
        since = as_of - timedelta(minutes=shapley_params.batch_interval_minutes)

        self.logger.info(
            f"Computing Shapley contributions for markets settled since {since}"
        )

        # 1. Find settled markets not yet scored
        settled_markets = await self.db.read(
            _SELECT_RECENTLY_SETTLED,
            params={"since": since, "until": as_of},
            mappings=True,
        )

        if not settled_markets:
            self.logger.info("No new settled markets to process")
            # Still update rolling aggregates from existing history
            await self._update_rolling_shapley(as_of, window_start, window_days)
            return

        self.items_total = len(settled_markets)
        self.logger.info(f"Found {self.items_total} settled markets to score")

        # 2. Get previous epoch weights for log-pool
        prev_weights = await self.db.read(
            _SELECT_PREVIOUS_WEIGHTS,
            params={"current_as_of": as_of, "window_days": window_days},
            mappings=True,
        )
        weight_map: Dict[int, float] = {}
        for row in prev_weights:
            ss = float(row["skill_score"]) if row["skill_score"] is not None else 0.0
            if ss > 0:
                weight_map[row["miner_id"]] = ss

        # 3. Process each settled market
        k = shapley_params.k_permutations
        seed_prefix = shapley_params.seed_prefix
        min_miners = shapley_params.min_miners_for_shapley

        for market_row in settled_markets:
            market_id = market_row["market_id"]
            settled_at = market_row["settled_at"]

            # Fetch submissions for this market
            subs = await self.db.read(
                _SELECT_MARKET_SUBMISSIONS,
                params={"market_id": market_id},
                mappings=True,
            )

            if not subs:
                continue

            # Fetch outcomes
            outcomes = await self.db.read(
                _SELECT_MARKET_OUTCOMES,
                params={"market_id": market_id},
                mappings=True,
            )
            outcome_map: Dict[str, int] = {}
            for row in outcomes:
                outcome_map[row["side"]] = int(row["result"])

            if not outcome_map:
                continue

            # Build per-side arrays: miner_id -> prob per side
            miner_ids_set: set[int] = set()
            miner_side_probs: Dict[str, Dict[int, float]] = defaultdict(dict)

            for sub in subs:
                mid = sub["miner_id"]
                side = sub["side"]
                miner_ids_set.add(mid)
                # Take last submission per miner per side
                miner_side_probs[side][mid] = float(sub["imp_prob"])

            miner_ids_list = sorted(miner_ids_set)
            if len(miner_ids_list) < min_miners:
                continue

            # Build weight array aligned with miner_ids_list
            weights = np.array(
                [weight_map.get(mid, 1.0 / len(miner_ids_list)) for mid in miner_ids_list],
                dtype=np.float64,
            )

            # Process each side that has an outcome
            seed = _deterministic_seed(seed_prefix, market_id)
            all_shapley = np.zeros(len(miner_ids_list), dtype=np.float64)
            n_sides = 0

            for side_idx, (side, side_outcome) in enumerate(sorted(outcome_map.items())):
                if side not in miner_side_probs:
                    continue

                probs = np.array(
                    [miner_side_probs[side].get(mid, 0.5) for mid in miner_ids_list],
                    dtype=np.float64,
                )

                side_seed = seed + side_idx * 7919
                sv = monte_carlo_shapley(
                    probs, weights, side_outcome,
                    k_permutations=k,
                    seed=side_seed,
                    epsilon=float(shapley_params.prob_clamp_epsilon),
                )
                all_shapley += sv
                n_sides += 1

            if n_sides > 0:
                all_shapley /= n_sides

            # Persist per-miner Shapley values
            for i, mid in enumerate(miner_ids_list):
                await self.db.write(
                    _INSERT_SHAPLEY,
                    params={
                        "miner_id": mid,
                        "market_id": market_id,
                        "shapley_value": float(all_shapley[i]),
                        "settled_at": settled_at,
                        "k_permutations": k,
                    },
                )

            self.items_processed += 1
            self.state["last_market_id"] = market_id
            await self.checkpoint_if_due()

        # 4. Update rolling Shapley aggregates for all miners
        await self._update_rolling_shapley(as_of, window_start, window_days)

        self.logger.info(
            f"Shapley computed for {self.items_processed} markets"
        )

    async def _update_rolling_shapley(
        self,
        as_of: datetime,
        window_start: datetime,
        window_days: int,
    ) -> None:
        """Time-decay Shapley values and update marginal_dim for all miners."""
        half_life = self.params.decay.half_life_days

        # Fetch all Shapley history within window
        history = await self.db.read(
            _SELECT_SHAPLEY_HISTORY,
            params={"window_start": window_start, "window_end": as_of},
            mappings=True,
        )

        if not history:
            return

        # Group by miner
        miner_shapley: Dict[int, List[tuple]] = defaultdict(list)
        for row in history:
            miner_shapley[row["miner_id"]].append(
                (row["settled_at"], float(row["shapley_value"]))
            )

        # Compute rolling means with decay
        miner_means: Dict[int, tuple] = {}  # miner_id -> (mean, ws, wt)
        for mid, entries in miner_shapley.items():
            timestamps = np.array([e[0].timestamp() for e in entries])
            values = np.array([e[1] for e in entries])
            decay_weights = compute_decay_weights(
                timestamps,
                as_of.timestamp(),
                half_life * 86400,  # convert days to seconds
            )
            wt = float(decay_weights.sum())
            if wt > 0:
                ws = float((values * decay_weights).sum())
                mean = ws / wt
            else:
                ws, wt, mean = 0.0, 0.0, 0.0
            miner_means[mid] = (mean, ws, wt)

        # Normalise Shapley means via z-score logistic → [0, 1]
        miner_ids = sorted(miner_means.keys())
        raw_means = np.array([miner_means[mid][0] for mid in miner_ids])

        min_count = int(self.params.normalization.min_count_for_zscore)
        if len(raw_means) >= min_count:
            marginal_norm = normalize_zscore_logistic(raw_means)
        else:
            marginal_norm = normalize_percentile(raw_means)

        # Clamp to [epsilon, 1.0]
        eps = float(self.params.cobb_douglas.epsilon)
        marginal_norm = np.clip(marginal_norm, eps, 1.0)

        # Fetch miner hotkeys for update
        miners_in_rolling = await self.db.read(
            _SELECT_MINERS_IN_ROLLING,
            params={"as_of": as_of, "window_days": window_days},
            mappings=True,
        )
        hotkey_map = {row["miner_id"]: row["miner_hotkey"] for row in miners_in_rolling}

        # Persist
        for i, mid in enumerate(miner_ids):
            hotkey = hotkey_map.get(mid)
            if hotkey is None:
                continue
            mean, ws, wt = miner_means[mid]
            await self.db.write(
                _UPDATE_SHAPLEY_SCORES,
                params={
                    "miner_id": mid,
                    "miner_hotkey": hotkey,
                    "as_of": as_of,
                    "window_days": window_days,
                    "shapley_mean": mean,
                    "shapley_ws": ws,
                    "shapley_wt": wt,
                    "marginal_dim": float(marginal_norm[i]),
                },
            )
