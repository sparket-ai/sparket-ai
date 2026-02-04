"""Handler for scoring individual odds submissions.

This handler computes CLV/CLE metrics for miner submissions by comparing
against ground truth consensus closing lines.

Flow:
1. Receive MinerOddsPushed event or submission batch
2. Look up ground truth closing for each (market_id, side)
3. Compute CLV, CLE, MES metrics
4. Persist to submission_vs_close table
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional

import bittensor as bt
from sqlalchemy import text

from sparket.validator.events.miner_events import MinerOddsPushed
from sparket.validator.scoring.determinism import round_decimal, to_decimal
from sparket.validator.scoring.metrics.clv import compute_clv, compute_mes
from sparket.validator.scoring.types import ValidationError
from sparket.validator.config.scoring_params import get_scoring_params


# SQL queries
_SELECT_GROUND_TRUTH = text(
    """
    SELECT
        gtc.market_id,
        gtc.side,
        gtc.prob_consensus,
        gtc.odds_consensus,
        gtc.contributing_books,
        gtc.bias_version,
        gtc.computed_at,
        e.start_time_utc
    FROM ground_truth_closing gtc
    JOIN market m ON gtc.market_id = m.market_id
    JOIN event e ON m.event_id = e.event_id
    WHERE gtc.market_id = :market_id
      AND gtc.side = :side
    """
)

_SELECT_UNSCORED_SUBMISSIONS = text(
    """
    SELECT
        ms.submission_id,
        ms.miner_id,
        ms.miner_hotkey,
        ms.market_id,
        ms.side,
        ms.submitted_at,
        ms.odds_eu,
        ms.imp_prob
    FROM miner_submission ms
    LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
    WHERE svc.submission_id IS NULL
      AND ms.submitted_at >= :since
    ORDER BY ms.submission_id
    LIMIT :limit
    """
)

_INSERT_SUBMISSION_VS_CLOSE = text(
    """
    INSERT INTO submission_vs_close (
        submission_id, provider_basis, close_ts, close_odds_eu,
        close_imp_prob, close_imp_prob_norm, clv_odds, clv_prob,
        cle, minutes_to_close, computed_at, ground_truth_version
    ) VALUES (
        :submission_id, :provider_basis, :close_ts, :close_odds_eu,
        :close_imp_prob, :close_imp_prob_norm, :clv_odds, :clv_prob,
        :cle, :minutes_to_close, :computed_at, :ground_truth_version
    )
    ON CONFLICT (submission_id) DO UPDATE SET
        clv_odds = EXCLUDED.clv_odds,
        clv_prob = EXCLUDED.clv_prob,
        cle = EXCLUDED.cle,
        minutes_to_close = EXCLUDED.minutes_to_close,
        computed_at = EXCLUDED.computed_at,
        ground_truth_version = EXCLUDED.ground_truth_version
    WHERE submission_vs_close.ground_truth_version < EXCLUDED.ground_truth_version
       OR submission_vs_close.ground_truth_version IS NULL
    """
)


class OddsScoreHandler:
    """Handler for computing CLV/CLE scores on odds submissions."""

    def __init__(self, database: Any):
        """Initialize the handler.

        Args:
            database: Database manager (DBM instance)
        """
        self.database = database
        self._min_books_for_consensus = int(get_scoring_params().ground_truth.min_books_for_consensus)

    async def score_event(self, event: MinerOddsPushed) -> None:
        """Score a single miner odds submission event.

        Event-driven scoring for immediate feedback.

        Args:
            event: MinerOddsPushed event with submission data
        """
        bt.logging.debug({"odds_score_event": "received", "event_id": event.event_id})

        try:
            payload = event.event_data.get("payload", {})
            rows = payload.get("rows", [])

            if not rows:
                return

            scored = 0
            for row in rows:
                success = await self._score_single_submission(row)
                if success:
                    scored += 1

            bt.logging.info({"odds_score_event": "completed", "scored": scored, "total": len(rows)})

        except Exception as e:
            bt.logging.warning({"odds_score_event_error": str(e)})

    async def score_batch(
        self,
        since: datetime,
        limit: int = 1000,
    ) -> int:
        """Score a batch of unscored submissions.

        Batch processing for efficiency in worker jobs.

        Args:
            since: Only score submissions after this time
            limit: Maximum submissions to process

        Returns:
            Number of submissions scored
        """
        # Fetch unscored submissions
        rows = await self.database.read(
            _SELECT_UNSCORED_SUBMISSIONS,
            params={"since": since, "limit": limit},
            mappings=True,
        )

        if not rows:
            return 0

        scored = 0
        for row in rows:
            try:
                success = await self._score_submission_row(row)
                if success:
                    scored += 1
            except Exception as e:
                bt.logging.warning({
                    "score_batch_error": str(e),
                    "submission_id": row.get("submission_id"),
                })

        return scored

    async def _score_single_submission(self, submission: Dict[str, Any]) -> bool:
        """Score a single submission from event data.

        Args:
            submission: Submission dict from event payload

        Returns:
            True if scored successfully
        """
        market_id = submission.get("market_id")
        side = submission.get("side")

        if not market_id or not side:
            return False

        # Normalize side to uppercase for enum compatibility
        side = str(side).upper()

        # Look up ground truth
        gt = await self._get_ground_truth(market_id, side)
        if gt is None:
            return False
        if gt["contributing_books"] < self._min_books_for_consensus:
            return False

        # Compute CLV/CLE
        try:
            miner_odds = to_decimal(submission.get("odds_eu"), "odds_eu")
            miner_prob = to_decimal(submission.get("imp_prob"), "imp_prob")
            submitted_at = submission.get("submitted_at")

            if not isinstance(submitted_at, datetime):
                return False

            clv_result = compute_clv(
                miner_odds=miner_odds,
                miner_prob=miner_prob,
                truth_odds=gt["odds_consensus"],
                truth_prob=gt["prob_consensus"],
                submitted_at=submitted_at,
                event_start=gt["start_time_utc"],
            )

            if clv_result is None:
                return False

            # We don't have submission_id in event data, so skip DB write
            # This is just for immediate feedback; batch scoring will persist
            return True

        except (ValidationError, KeyError):
            return False

    async def _score_submission_row(self, row: Dict[str, Any]) -> bool:
        """Score a submission from database row.

        Args:
            row: Database row dict

        Returns:
            True if scored and persisted successfully
        """
        submission_id = row["submission_id"]
        market_id = row["market_id"]
        side = row["side"]

        # Normalize side to uppercase for enum compatibility
        if isinstance(side, str):
            side = side.upper()

        # Look up ground truth
        gt = await self._get_ground_truth(market_id, side)
        if gt is None:
            return False
        if gt["contributing_books"] < self._min_books_for_consensus:
            return False

        try:
            miner_odds = to_decimal(row["odds_eu"], "odds_eu")
            miner_prob = to_decimal(row["imp_prob"], "imp_prob")
            submitted_at = row["submitted_at"]

            clv_result = compute_clv(
                miner_odds=miner_odds,
                miner_prob=miner_prob,
                truth_odds=gt["odds_consensus"],
                truth_prob=gt["prob_consensus"],
                submitted_at=submitted_at,
                event_start=gt["start_time_utc"],
            )

            if clv_result is None:
                return False

            # Persist to submission_vs_close
            await self.database.write(
                _INSERT_SUBMISSION_VS_CLOSE,
                params={
                    "submission_id": submission_id,
                    "provider_basis": "consensus",
                    "close_ts": gt["computed_at"],
                    "close_odds_eu": float(gt["odds_consensus"]),
                    "close_imp_prob": float(gt["prob_consensus"]),
                    "close_imp_prob_norm": float(gt["prob_consensus"]),  # Already normalized
                    "clv_odds": float(clv_result.clv_odds),
                    "clv_prob": float(clv_result.clv_prob),
                    "cle": float(clv_result.cle),
                    "minutes_to_close": clv_result.minutes_to_close,
                    "computed_at": datetime.now(timezone.utc),
                    "ground_truth_version": gt["bias_version"],
                },
            )

            return True

        except (ValidationError, KeyError) as e:
            bt.logging.debug({"score_submission_error": str(e), "submission_id": submission_id})
            return False

    async def _get_ground_truth(
        self,
        market_id: int,
        side: str,
    ) -> Optional[Dict[str, Any]]:
        """Fetch ground truth closing for a market/side.

        Args:
            market_id: Market identifier
            side: Price side (home, away, etc.)

        Returns:
            Ground truth dict or None if not found
        """
        rows = await self.database.read(
            _SELECT_GROUND_TRUTH,
            params={"market_id": market_id, "side": side},
            mappings=True,
        )

        if not rows:
            return None

        row = rows[0]
        return {
            "prob_consensus": to_decimal(row["prob_consensus"], "prob_consensus"),
            "odds_consensus": to_decimal(row["odds_consensus"], "odds_consensus"),
            "contributing_books": int(row["contributing_books"] or 0),
            "bias_version": row["bias_version"],
            "computed_at": row["computed_at"],
            "start_time_utc": row["start_time_utc"],
        }


__all__ = ["OddsScoreHandler"]
