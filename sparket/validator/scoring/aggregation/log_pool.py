"""Weighted logarithmic opinion pool.

Implements the log-pool aggregation used for:
1. Shapley subset evaluations (value function V(S))
2. Leave-One-Out contribution scoring
3. Future consensus line product

The log-pool is the arithmetic mean of log-odds (equivalently, geometric mean
of odds), which is theoretically optimal under Bayesian independence assumptions
(Genest & Zidek, 1986).

All functions are pure NumPy — no side effects, no database access.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def _logit(p: float | NDArray, epsilon: float = 0.005) -> float | NDArray:
    """Compute logit(p) = log(p / (1 - p)) with clamping."""
    p_clamped = np.clip(p, epsilon, 1.0 - epsilon)
    return np.log(p_clamped / (1.0 - p_clamped))


def _sigmoid(x: float | NDArray) -> float | NDArray:
    """Compute sigmoid(x) = 1 / (1 + exp(-x))."""
    return 1.0 / (1.0 + np.exp(-x))


def build_log_pool(
    probs: NDArray,
    weights: NDArray,
    epsilon: float = 0.005,
) -> float:
    """Build a weighted log-pool aggregate from miner probabilities.

    Args:
        probs: Shape (N,) array of miner probabilities.
        weights: Shape (N,) array of miner weights (must sum to 1, or will be normalised).
        epsilon: Clamping bound for probabilities before logit.

    Returns:
        Aggregated probability.
    """
    if len(probs) == 0:
        return 0.5
    logits = _logit(probs, epsilon)
    w = weights / weights.sum() if weights.sum() > 0 else np.ones_like(weights) / len(weights)
    agg_logit = np.dot(w, logits)
    return float(_sigmoid(agg_logit))


def loo_log_pool(
    full_logit_sum: float,
    full_weight_sum: float,
    miner_logit: float,
    miner_weight: float,
    epsilon: float = 0.005,
) -> float:
    """Compute leave-one-out log-pool aggregate in O(1).

    Given the cached full aggregate logit sum and total weight,
    remove one miner's contribution and return the LOO probability.

    Args:
        full_logit_sum: Sum of w_i * logit(p_i) across all miners.
        full_weight_sum: Sum of w_i across all miners.
        miner_logit: logit(p_i) for the miner to remove.
        miner_weight: w_i for the miner to remove.
        epsilon: Probability clamping bound.

    Returns:
        LOO aggregate probability (with miner removed).
    """
    remaining_weight = full_weight_sum - miner_weight
    if remaining_weight < 1e-12:
        return 0.5  # No other miners
    loo_logit = (full_logit_sum - miner_weight * miner_logit) / remaining_weight
    return float(_sigmoid(loo_logit))


def evaluate_subset(
    miner_logits: NDArray,
    miner_weights: NDArray,
    subset_mask: NDArray,
    epsilon: float = 0.005,
) -> float:
    """Evaluate the log-pool aggregate for an arbitrary subset of miners.

    Used by Shapley permutation walks.

    Args:
        miner_logits: Shape (N,) pre-computed logit(p_i) for all miners.
        miner_weights: Shape (N,) weights for all miners.
        subset_mask: Shape (N,) boolean mask — True for miners in subset.
        epsilon: Probability clamping bound.

    Returns:
        Aggregate probability for the subset.
    """
    if not subset_mask.any():
        return 0.5  # Empty subset → uninformative prior
    sub_logits = miner_logits[subset_mask]
    sub_weights = miner_weights[subset_mask]
    w_sum = sub_weights.sum()
    if w_sum < 1e-12:
        return 0.5
    agg_logit = np.dot(sub_weights, sub_logits) / w_sum
    return float(_sigmoid(agg_logit))


def brier_score(prob: float, outcome: int) -> float:
    """Compute Brier score for a single binary event.

    Args:
        prob: Predicted probability of outcome = 1.
        outcome: Actual outcome (0 or 1).

    Returns:
        Brier score (lower is better, range [0, 1]).
    """
    return (prob - outcome) ** 2
