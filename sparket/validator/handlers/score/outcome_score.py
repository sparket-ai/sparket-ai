"""Handler for scoring submissions after outcome settlement.

This handler computes Brier score, log-loss, and PSS for submissions
after the underlying market outcome is known.

Flow:
1. Receive settled outcome or poll for settled markets
2. Look up all submissions for that market
3. Get ground truth probabilities at closing
4. Compute proper scoring rules (Brier, log-loss, PSS)
5. Persist to submission_outcome_score table
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import bittensor as bt
import numpy as np
from sqlalchemy import text

from sparket.validator.events.miner_events import MinerOutcomePushed
from sparket.validator.scoring.determinism import round_decimal, to_decimal
from sparket.validator.scoring.metrics.proper_scoring import (
    brier_score,
    log_loss,
    pss,
)
from sparket.validator.scoring.types import ValidationError
from sparket.validator.config.scoring_params import get_scoring_params


def outcome_to_vector(result: str, sides: List[str]) -> List[int]:
    """Convert outcome string to one-hot vector.
    
    Args:
        result: Outcome string (e.g. "home", "away", "draw", "over", "under")
        sides: Ordered list of valid sides
        
    Returns:
        One-hot list (e.g. [1, 0, 0] for "home" in ["home", "away", "draw"])
    """
    vec = [0] * len(sides)
    if result in sides:
        vec[sides.index(result)] = 1
    return vec


def probs_for_market(gt_probs: Dict[str, Decimal], sides: List[str]) -> List[Decimal]:
    """Build probability vector from ground truth dict.
    
    Args:
        gt_probs: Dict mapping side -> probability
        sides: Ordered list of sides
        
    Returns:
        List of probabilities in sides order
    """
    return [gt_probs.get(s, Decimal("0")) for s in sides]


# SQL queries
_SELECT_SETTLED_MARKETS = text(
    """
    SELECT
        o.outcome_id,
        o.market_id,
        o.result,
        o.settled_at,
        m.kind
    FROM outcome o
    JOIN market m ON o.market_id = m.market_id
    LEFT JOIN (
        SELECT DISTINCT ms.market_id as scored_market_id 
        FROM submission_outcome_score sos
        JOIN miner_submission ms ON sos.submission_id = ms.submission_id
    ) scored ON o.market_id = scored.scored_market_id
    WHERE o.result IS NOT NULL
      AND o.settled_at IS NOT NULL
      AND scored.scored_market_id IS NULL
      AND o.settled_at >= :since
      AND EXISTS (
          SELECT 1 FROM ground_truth_closing gtc
          WHERE gtc.market_id = o.market_id
      )
    ORDER BY o.settled_at DESC
    LIMIT :limit
    """
)

_SELECT_MARKET_SUBMISSIONS = text(
    """
    SELECT
        ms.submission_id,
        ms.miner_id,
        ms.miner_hotkey,
        ms.market_id,
        ms.side,
        ms.imp_prob,
        ms.submitted_at
    FROM miner_submission ms
    WHERE ms.market_id = :market_id
    ORDER BY ms.submission_id
    """
)

_SELECT_GROUND_TRUTH_BY_MARKET = text(
    """
    SELECT
        gtc.side,
        gtc.prob_consensus,
        gtc.contributing_books
    FROM ground_truth_closing gtc
    WHERE gtc.market_id = :market_id
    """
)

_INSERT_OUTCOME_SCORE = text(
    """
    INSERT INTO submission_outcome_score (
        submission_id, brier, logloss, provider_brier, provider_logloss,
        pss, pss_brier, pss_log, outcome_vector, settled_at
    ) VALUES (
        :submission_id, :brier, :logloss, :provider_brier, :provider_logloss,
        :pss, :pss_brier, :pss_log, :outcome_vector, :settled_at
    )
    ON CONFLICT (submission_id) DO UPDATE SET
        brier = EXCLUDED.brier,
        logloss = EXCLUDED.logloss,
        pss = EXCLUDED.pss,
        pss_brier = EXCLUDED.pss_brier,
        pss_log = EXCLUDED.pss_log
    """
)


# Side ordering by market kind
SIDES_BY_KIND: Dict[str, List[str]] = {
    "moneyline": ["home", "away", "draw"],
    "spread": ["home", "away"],
    "total": ["over", "under"],
    "draw_no_bet": ["home", "away"],
}


def get_sides_for_kind(kind: str) -> List[str]:
    """Get ordered sides for a market kind."""
    return SIDES_BY_KIND.get(kind.lower(), ["home", "away"])


class OutcomeScoreHandler:
    """Handler for computing outcome-based scores on submissions."""

    def __init__(self, database: Any):
        """Initialize the handler.

        Args:
            database: Database manager (DBM instance)
        """
        self.database = database
        self._min_books_for_consensus = int(get_scoring_params().ground_truth.min_books_for_consensus)

    async def score_event(self, event: MinerOutcomePushed) -> None:
        """Score submissions for a settled outcome event.

        Event-driven scoring when outcome is pushed.

        Args:
            event: MinerOutcomePushed event
        """
        bt.logging.debug({"outcome_score_event": "received", "event_id": event.event_id})

        # Note: Outcome scoring is typically done in batch after settlement
        # This event handler is for logging/immediate processing if needed
        try:
            payload = event.event_data.get("payload", {})
            accepted = payload.get("accepted", False)

            if not accepted:
                return

            bt.logging.info({"outcome_score_event": "logged_for_batch"})

        except Exception as e:
            bt.logging.warning({"outcome_score_event_error": str(e)})

    async def score_batch(
        self,
        since: datetime,
        limit: int = 10_000,
    ) -> int:
        """Score submissions for a batch of settled markets.

        This is the main batch processing method called by the worker.

        Args:
            since: Only process outcomes settled after this time
            limit: Maximum markets to process

        Returns:
            Number of submissions scored
        """
        # Find settled markets without scores
        settled = await self.database.read(
            _SELECT_SETTLED_MARKETS,
            params={"since": since, "limit": limit},
            mappings=True,
        )

        if not settled:
            return 0

        total_scored = 0
        skipped = 0
        reason_counts: Dict[str, int] = {}

        for market_row in settled:
            try:
                scored, skip_reason = await self._score_market_submissions(market_row)
                total_scored += scored
                if scored == 0 and skip_reason:
                    skipped += 1
                    reason_counts[skip_reason] = reason_counts.get(skip_reason, 0) + 1
            except Exception as e:
                skipped += 1
                reason_counts["error"] = reason_counts.get("error", 0) + 1
                bt.logging.warning({
                    "score_market_error": str(e),
                    "market_id": market_row.get("market_id"),
                })

        bt.logging.info({
            "score_batch_summary": "outcome",
            "scored": total_scored,
            "skipped": skipped,
            "reason_counts": reason_counts,
        })

        return total_scored

    async def _score_market_submissions(
        self, market_row: Dict[str, Any],
    ) -> Tuple[int, Optional[str]]:
        """Score all submissions for a single market.

        Args:
            market_row: Market outcome row from database

        Returns:
            Tuple of (submissions scored, skip reason if skipped)
        """
        market_id = market_row["market_id"]
        raw_result = market_row.get("result")
        settled_at = market_row["settled_at"]
        kind = market_row["kind"]

        if raw_result is None:
            return 0, "no_result"
        result = str(raw_result).strip().lower()
        if not result:
            return 0, "no_result"

        # Get sides for this market kind
        sides = get_sides_for_kind(kind)

        # Filter sides that are valid for the result
        # For moneyline with draw, we might have only home/away submissions
        if result == "draw" and "draw" not in sides:
            # Can't score if draw wasn't a valid outcome
            return 0, "invalid_draw"

        # Get ground truth probabilities
        gt_probs, min_books = await self._get_ground_truth_probs(market_id)
        if not gt_probs:
            return 0, "no_gt"
        if min_books is None or min_books < self._min_books_for_consensus:
            return 0, "insufficient_books"

        # Filter GT to only sides valid for this market kind (fixes
        # ground_truth_closing contamination where all 5 sides are stored
        # regardless of market kind).
        gt_probs = {s: p for s, p in gt_probs.items() if s in sides}

        # Filter to sides we have ground truth for
        valid_sides = [s for s in sides if s in gt_probs]
        if len(valid_sides) < 2:
            return 0, "insufficient_sides"

        # Build outcome vector
        try:
            outcome_vec = outcome_to_vector(result, valid_sides)
        except ValidationError:
            return 0, "invalid_outcome"

        # Build ground truth probability vector
        try:
            truth_probs = probs_for_market(gt_probs, valid_sides)
        except ValidationError:
            return 0, "invalid_gt_probs"

        # Get all submissions for this market
        submissions = await self.database.read(
            _SELECT_MARKET_SUBMISSIONS,
            params={"market_id": market_id},
            mappings=True,
        )

        if not submissions:
            return 0, "no_submissions"

        # Group submissions by logical submission key (miner + market + time)
        grouped = self._group_submissions_by_key(submissions)

        scored = 0
        for submission_key, grouped_data in grouped.items():
            try:
                scored_count = await self._score_single_submission(
                    submission_key=submission_key,
                    sides_data=grouped_data["sides_data"],
                    submission_ids=grouped_data["submission_ids"],
                    valid_sides=valid_sides,
                    outcome_vec=outcome_vec,
                    truth_probs=truth_probs,
                    settled_at=settled_at,
                )
                scored += scored_count
            except Exception as e:
                bt.logging.debug({
                    "score_submission_error": str(e),
                    "submission_key": submission_key,
                })

        return scored, None

    def _group_submissions_by_key(
        self,
        submissions: List[Dict[str, Any]],
    ) -> Dict[tuple[int, str, int, datetime], Dict[str, Any]]:
        """Group submission rows into logical multi-side submissions.

        Grouping key uses (miner_id, miner_hotkey, market_id, submitted_at)
        to reconstruct full vectors without schema changes.
        """
        result: Dict[tuple[int, str, int, datetime], Dict[str, Any]] = {}

        for sub in submissions:
            key = (
                int(sub["miner_id"]),
                str(sub["miner_hotkey"]),
                int(sub["market_id"]),
                sub["submitted_at"],
            )
            raw_side = sub.get("side")
            if raw_side is None:
                continue
            side = str(raw_side).strip().lower()
            prob = to_decimal(sub["imp_prob"], "imp_prob")

            if key not in result:
                result[key] = {"sides_data": {}, "submission_ids": []}
            result[key]["sides_data"][side] = prob
            result[key]["submission_ids"].append(int(sub["submission_id"]))

        return result

    async def _score_single_submission(
        self,
        submission_key: tuple[int, str, int, datetime],
        sides_data: Dict[str, Decimal],
        submission_ids: List[int],
        valid_sides: List[str],
        outcome_vec: List[int],
        truth_probs: List[Decimal],
        settled_at: datetime,
    ) -> int:
        """Score a grouped submission set.

        Note: For submissions that only cover one side (most common case),
        we need to handle partial probability vectors.
        """
        miner_probs = self._build_miner_probs(valid_sides, sides_data)
        if miner_probs is None:
            return 0

        # Compute scores
        try:
            # Convert to numpy arrays for scoring functions
            miner_arr = np.array([float(p) for p in miner_probs], dtype=np.float64)
            truth_arr = np.array([float(p) for p in truth_probs], dtype=np.float64)
            outcome_arr = np.array(outcome_vec, dtype=np.int8)
            
            brier_m = brier_score(miner_arr, outcome_arr)
            brier_t = brier_score(truth_arr, outcome_arr)
            ll_m = log_loss(miner_arr, outcome_arr)
            ll_t = log_loss(truth_arr, outcome_arr)

            pss_b = pss(brier_m, brier_t)
            pss_l = pss(ll_m, ll_t)

            # PSS average (legacy field)
            pss_avg = (pss_b + pss_l) / 2.0

        except ValidationError:
            return 0

        # Persist
        for submission_id in submission_ids:
            await self.database.write(
                _INSERT_OUTCOME_SCORE,
                params={
                    "submission_id": submission_id,
                    "brier": float(brier_m),
                    "logloss": float(ll_m),
                    "provider_brier": float(brier_t),
                    "provider_logloss": float(ll_t),
                    "pss": float(pss_avg),
                    "pss_brier": float(pss_b),
                    "pss_log": float(pss_l),
                    "outcome_vector": json.dumps(outcome_vec),
                    "settled_at": settled_at,
                },
            )

        return len(submission_ids)

    def _build_miner_probs(
        self,
        valid_sides: List[str],
        sides_data: Dict[str, Decimal],
    ) -> Optional[List[Decimal]]:
        """Construct a valid probability vector for scoring.

        - For 3-way markets: require full vector, no implicit fill.
        - For 2-way markets: allow complement if only one side is submitted.
        """
        if not valid_sides:
            return None

        # Three-way or more: require full coverage
        if len(valid_sides) >= 3:
            if any(side not in sides_data for side in valid_sides):
                return None
            probs = [sides_data[side] for side in valid_sides]
            return self._normalize_probs(probs)

        # Two-way: allow complement when only one side submitted
        if len(valid_sides) == 2:
            provided = [side for side in valid_sides if side in sides_data]
            if len(provided) == 2:
                probs = [sides_data[side] for side in valid_sides]
                return self._normalize_probs(probs)
            if len(provided) == 1:
                p = sides_data[provided[0]]
                if p <= Decimal("0") or p >= Decimal("1"):
                    return None
                complement = Decimal("1") - p
                if complement <= Decimal("0") or complement >= Decimal("1"):
                    return None
                probs = [
                    p if side == provided[0] else complement for side in valid_sides
                ]
                return self._normalize_probs(probs)
            return None

        return None

    def _normalize_probs(self, probs: List[Decimal]) -> Optional[List[Decimal]]:
        prob_sum = sum(probs)
        if prob_sum <= Decimal("0"):
            return None
        return [p / prob_sum for p in probs]

    async def _get_ground_truth_probs(self, market_id: int) -> Tuple[Dict[str, Decimal], Optional[int]]:
        """Get ground truth probabilities for a market.

        Args:
            market_id: Market identifier

        Returns:
            Dict mapping side -> consensus probability
        """
        rows = await self.database.read(
            _SELECT_GROUND_TRUTH_BY_MARKET,
            params={"market_id": market_id},
            mappings=True,
        )

        result: Dict[str, Decimal] = {}
        min_books: Optional[int] = None
        for row in rows:
            side = str(row["side"]).strip().lower()
            prob = to_decimal(row["prob_consensus"], "prob_consensus")
            result[side] = prob
            books = int(row["contributing_books"] or 0)
            if min_books is None or books < min_books:
                min_books = books

        return result, min_books


__all__ = ["OutcomeScoreHandler"]
