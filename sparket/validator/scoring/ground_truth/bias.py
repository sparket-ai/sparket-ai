"""Iterative sportsbook bias estimation.

This module estimates per-(sportsbook, sport, market_kind) bias factors
by comparing each book's probabilities to realized outcomes over time.

Algorithm:
1. Bootstrap with equal weights (bias_factor = 1.0)
2. After outcomes settle, compute each book's error vs consensus
3. Update bias factors using exponential moving average
4. Track variance for inverse-variance weighting

The bias factor represents how much a book's probabilities need to be
adjusted to match true probabilities. A bias_factor > 1 means the book
underestimates probabilities; < 1 means overestimates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Tuple

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params

from ..determinism import (
    compute_hash,
    round_decimal,
    safe_divide,
    sort_by_id,
    to_decimal,
)


@dataclass
class BiasUpdateInput:
    """Input data for a single bias update observation."""

    sportsbook_id: int
    sport_id: int
    market_kind: str
    book_prob: Decimal  # Book's probability for the realized outcome
    outcome_hit: int  # 1 if this side won, 0 otherwise


@dataclass
class BiasState:
    """Current bias state for a (sportsbook, sport, market_kind) combination."""

    sportsbook_id: int
    sport_id: int
    market_kind: str
    bias_factor: Decimal
    variance: Decimal
    mse: Decimal
    sample_count: int
    version: int


BiasKey = Tuple[int, int, str]  # (sportsbook_id, sport_id, market_kind)


def make_bias_key(sportsbook_id: int, sport_id: int, market_kind: str) -> BiasKey:
    """Create a bias lookup key."""
    return (sportsbook_id, sport_id, market_kind)


def get_initial_bias() -> BiasState:
    """Get initial bias state for a new (book, sport, market) combo."""
    return BiasState(
        sportsbook_id=0,
        sport_id=0,
        market_kind="",
        bias_factor=Decimal("1.0"),
        variance=Decimal("0.01"),
        mse=Decimal("0"),
        sample_count=0,
        version=1,
    )


class BiasEstimator:
    """Estimates sportsbook bias from settled outcomes.

    The estimator maintains bias state per (sportsbook, sport, market_kind)
    and updates iteratively as outcomes settle.
    """

    def __init__(self, params: ScoringParams | None = None):
        """Initialize the bias estimator.

        Args:
            params: Scoring parameters (uses defaults if None)
        """
        self.params = params or get_scoring_params()
        self.gt_params = self.params.ground_truth

    def compute_batch_updates(
        self,
        observations: List[BiasUpdateInput],
        current_bias: Dict[BiasKey, BiasState],
    ) -> Dict[BiasKey, BiasState]:
        """Compute bias updates from a batch of settled outcomes.

        This is the main entry point for batch bias estimation.
        Processes observations in deterministic order.

        Args:
            observations: List of settled outcome observations
            current_bias: Current bias state by key

        Returns:
            Updated bias states (only for keys that changed)
        """
        # Group observations by key for aggregation
        grouped: Dict[BiasKey, List[BiasUpdateInput]] = {}
        for obs in observations:
            key = make_bias_key(obs.sportsbook_id, obs.sport_id, obs.market_kind)
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(obs)

        updates: Dict[BiasKey, BiasState] = {}

        # Process keys in deterministic order
        for key in sorted(grouped.keys()):
            obs_list = grouped[key]
            current = current_bias.get(key)

            if current is None:
                # Initialize new bias state
                current = get_initial_bias()
                current = BiasState(
                    sportsbook_id=key[0],
                    sport_id=key[1],
                    market_kind=key[2],
                    bias_factor=current.bias_factor,
                    variance=current.variance,
                    mse=current.mse,
                    sample_count=current.sample_count,
                    version=current.version,
                )

            # Sort observations by a deterministic attribute
            sorted_obs = sorted(obs_list, key=lambda o: (o.book_prob, o.outcome_hit))

            updated = self._update_single_key(current, sorted_obs)
            updates[key] = updated

        return updates

    def _update_single_key(
        self,
        current: BiasState,
        observations: List[BiasUpdateInput],
    ) -> BiasState:
        """Update bias for a single (book, sport, market) key.

        Uses exponential moving average to update bias and variance estimates.

        Args:
            current: Current bias state
            observations: New observations for this key

        Returns:
            Updated bias state
        """
        alpha = self.gt_params.bias_learning_rate

        # Running values
        bias = current.bias_factor
        mse = current.mse
        sample_count = current.sample_count

        for obs in observations:
            # Error is book_prob - outcome_hit
            # If outcome hit and prob was 0.6, error = 0.6 - 1 = -0.4
            # If outcome missed and prob was 0.6, error = 0.6 - 0 = 0.6
            error = obs.book_prob - Decimal(str(obs.outcome_hit))
            error_sq = error * error

            # Update MSE with EMA
            mse = (Decimal("1") - alpha) * mse + alpha * error_sq

            # Update bias factor
            # If book consistently overestimates (positive errors), reduce bias
            # If book consistently underestimates (negative errors), increase bias
            # bias_factor = p_true / p_book, so if book overestimates, factor < 1
            if obs.book_prob > Decimal("0"):
                implied_adjustment = Decimal(str(obs.outcome_hit)) / obs.book_prob
                # Clamp upper bound only — the lower bound must stay at 0
                # to preserve unbiasedness.  Clamping the floor to 0.5
                # introduced systematic upward drift in bias_factor for
                # probabilities near 0.5 (e.g. totals markets).
                implied_adjustment = min(implied_adjustment, Decimal("2.0"))
                bias = (Decimal("1") - alpha) * bias + alpha * implied_adjustment

            sample_count += 1

        # Compute variance from MSE (simplified)
        variance = max(mse, self.gt_params.min_variance)

        # Clamp bias to valid range
        bias = max(
            self.gt_params.min_bias_factor,
            min(bias, self.gt_params.max_bias_factor),
        )

        return BiasState(
            sportsbook_id=current.sportsbook_id,
            sport_id=current.sport_id,
            market_kind=current.market_kind,
            bias_factor=round_decimal(bias, 6),
            variance=round_decimal(variance, 6),
            mse=round_decimal(mse, 8),
            sample_count=sample_count,
            version=current.version + 1,
        )

    def is_bias_trusted(self, state: BiasState) -> bool:
        """Check if a bias estimate has enough samples to be trusted.

        Args:
            state: Bias state to check

        Returns:
            True if sample count >= min_samples
        """
        return state.sample_count >= self.gt_params.bias_min_samples


def compute_bias_input_hash(observations: List[BiasUpdateInput]) -> str:
    """Compute deterministic hash of bias update inputs.

    Used for consensus verification.

    Args:
        observations: List of observations

    Returns:
        SHA256 hex hash
    """
    data = [
        {
            "sportsbook_id": o.sportsbook_id,
            "sport_id": o.sport_id,
            "market_kind": o.market_kind,
            "book_prob": str(o.book_prob),
            "outcome_hit": o.outcome_hit,
        }
        for o in sorted(
            observations,
            key=lambda x: (x.sportsbook_id, x.sport_id, x.market_kind, str(x.book_prob)),
        )
    ]
    return compute_hash({"observations": data})


__all__ = [
    "BiasUpdateInput",
    "BiasState",
    "BiasKey",
    "make_bias_key",
    "get_initial_bias",
    "BiasEstimator",
    "compute_bias_input_hash",
]

