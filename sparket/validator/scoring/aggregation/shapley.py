"""Monte Carlo Shapley value estimation for miner contribution scoring.

Estimates each miner's marginal contribution to the aggregate forecast quality
by sampling random permutations and measuring the Brier score improvement when
each miner joins the growing coalition.

The value function V(S) = -Brier(log_pool(S), outcome), so positive Shapley
values indicate the miner improved the aggregate.

All functions are pure NumPy — no side effects, no database access.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray

from sparket.validator.scoring.aggregation.log_pool import _logit, _sigmoid, brier_score


def monte_carlo_shapley(
    miner_probs: NDArray,
    miner_weights: NDArray,
    outcome: int,
    k_permutations: int = 500,
    seed: int = 42,
    epsilon: float = 0.005,
) -> NDArray:
    """Estimate Shapley values for all miners on a single binary market.

    Uses the permutation-based Monte Carlo approximation: for each random
    permutation, walk through miners in order, building the log-pool
    incrementally, and record the marginal Brier improvement when each
    miner joins.

    Args:
        miner_probs: Shape (N,) array of miner predicted probabilities.
        miner_weights: Shape (N,) array of miner weights (emission weights).
            Will be normalised internally.
        outcome: Binary outcome (0 or 1).
        k_permutations: Number of random permutations to sample.
        seed: RNG seed for deterministic results.
        epsilon: Probability clamping bound for logit.

    Returns:
        Shape (N,) array of Shapley values. Positive = miner improves aggregate.
        Negative = miner degrades aggregate. Near-zero = redundant.
    """
    n = len(miner_probs)
    if n == 0:
        return np.array([])
    if n == 1:
        # Single miner: Shapley = full aggregate quality improvement over prior
        prob = float(np.clip(miner_probs[0], epsilon, 1.0 - epsilon))
        baseline_brier = brier_score(0.5, outcome)  # uninformative prior
        miner_brier = brier_score(prob, outcome)
        return np.array([baseline_brier - miner_brier])

    # Normalise weights
    w_sum = miner_weights.sum()
    if w_sum < 1e-12:
        weights = np.ones(n) / n
    else:
        weights = miner_weights / w_sum

    # Pre-compute logits
    logits = _logit(miner_probs, epsilon)

    rng = np.random.default_rng(seed)
    shapley_accum = np.zeros(n, dtype=np.float64)

    # Baseline Brier for empty coalition (uninformative prior = 0.5)
    baseline_brier = brier_score(0.5, outcome)

    for _ in range(k_permutations):
        perm = rng.permutation(n)

        # Walk the permutation, building log-pool incrementally
        running_weighted_logit_sum = 0.0
        running_weight_sum = 0.0
        prev_brier = baseline_brier

        for idx in perm:
            w_i = weights[idx]
            l_i = logits[idx]

            running_weighted_logit_sum += w_i * l_i
            running_weight_sum += w_i

            # Current aggregate probability
            agg_logit = running_weighted_logit_sum / running_weight_sum
            agg_prob = float(_sigmoid(agg_logit))
            curr_brier = brier_score(agg_prob, outcome)

            # Marginal contribution = improvement in Brier (lower is better,
            # so positive delta means the miner helped)
            shapley_accum[idx] += prev_brier - curr_brier
            prev_brier = curr_brier

    shapley_accum /= k_permutations
    return shapley_accum


def shapley_for_multi_outcome_market(
    miner_probs_per_side: dict[str, NDArray],
    miner_weights: NDArray,
    outcomes: dict[str, int],
    k_permutations: int = 500,
    seed: int = 42,
    epsilon: float = 0.005,
) -> NDArray:
    """Compute Shapley values averaged across all sides of a market.

    For markets with multiple sides (e.g., moneyline_home, moneyline_away,
    spread_home, total), compute Shapley independently per side and average.

    Args:
        miner_probs_per_side: Dict mapping side name to shape (N,) prob arrays.
        miner_weights: Shape (N,) emission weights.
        outcomes: Dict mapping side name to binary outcome (0 or 1).
        k_permutations: Monte Carlo permutations per side.
        seed: Base RNG seed (combined with side index for per-side determinism).
        epsilon: Probability clamping.

    Returns:
        Shape (N,) array of averaged Shapley values across all sides.
    """
    n = len(miner_weights)
    if n == 0:
        return np.array([])

    total_shapley = np.zeros(n, dtype=np.float64)
    n_sides = 0

    for i, (side, probs) in enumerate(sorted(miner_probs_per_side.items())):
        if side not in outcomes:
            continue
        side_seed = seed + i * 7919  # Deterministic per-side seed offset (prime)
        sv = monte_carlo_shapley(
            probs, miner_weights, outcomes[side],
            k_permutations=k_permutations,
            seed=side_seed,
            epsilon=epsilon,
        )
        total_shapley += sv
        n_sides += 1

    if n_sides > 0:
        total_shapley /= n_sides

    return total_shapley
