"""Tests for Monte Carlo Shapley contribution scoring.

Covers:
- Informative vs noise miners → positive vs non-positive Shapley
- Identical miners → Shapley value splitting
- Deterministic seeding → reproducibility
- Convergence: higher K → lower variance
- Single miner edge case
- Performance: 256 miners × K=500 < 30s
"""

import time

import numpy as np
import pytest

from sparket.validator.scoring.aggregation.shapley import (
    monte_carlo_shapley,
    shapley_for_multi_outcome_market,
)


class TestMonteCarloShapley:
    """Core Shapley estimation tests."""

    def test_informative_vs_noise(self):
        """Informative miners get positive Shapley, noise gets ≤ 0."""
        rng = np.random.default_rng(123)
        n_informative = 5
        n_noise = 5
        n = n_informative + n_noise
        outcome = 1

        # Informative miners: predict ~0.7-0.9 (correct direction)
        informative_probs = 0.7 + 0.2 * rng.random(n_informative)
        # Noise miners: random around 0.5
        noise_probs = 0.3 + 0.4 * rng.random(n_noise)

        probs = np.concatenate([informative_probs, noise_probs])
        weights = np.ones(n)

        sv = monte_carlo_shapley(probs, weights, outcome, k_permutations=1000, seed=42)

        # Informative miners should have positive Shapley on average
        informative_mean = sv[:n_informative].mean()
        noise_mean = sv[n_informative:].mean()

        assert informative_mean > 0, f"Informative Shapley mean should be positive, got {informative_mean}"
        assert informative_mean > noise_mean, (
            f"Informative ({informative_mean:.4f}) should exceed noise ({noise_mean:.4f})"
        )

    def test_identical_miners_split_value(self):
        """Two identical miners each get ~half the value of a unique miner."""
        outcome = 1
        unique_prob = 0.8
        weights = np.ones(3)

        # Scenario 1: one unique miner + one other
        probs_unique = np.array([unique_prob, 0.6])
        sv_unique = monte_carlo_shapley(probs_unique, weights[:2], outcome, k_permutations=2000, seed=42)

        # Scenario 2: two identical miners + one other
        probs_dup = np.array([unique_prob, unique_prob, 0.6])
        sv_dup = monte_carlo_shapley(probs_dup, weights[:3], outcome, k_permutations=2000, seed=42)

        # Each duplicate should get roughly half what the unique miner got
        unique_value = sv_unique[0]
        dup_value_each = sv_dup[0]  # Either duplicate (they're symmetric)

        # Allow 30% tolerance due to Monte Carlo variance
        ratio = dup_value_each / unique_value if unique_value > 0 else 0
        assert 0.3 < ratio < 0.7, (
            f"Duplicate should get ~50% of unique value. "
            f"Unique={unique_value:.4f}, Dup={dup_value_each:.4f}, ratio={ratio:.3f}"
        )

    def test_deterministic_seed(self):
        """Same seed produces identical results."""
        probs = np.array([0.6, 0.7, 0.8, 0.5, 0.9])
        weights = np.ones(5)

        sv1 = monte_carlo_shapley(probs, weights, outcome=1, k_permutations=100, seed=999)
        sv2 = monte_carlo_shapley(probs, weights, outcome=1, k_permutations=100, seed=999)

        np.testing.assert_array_equal(sv1, sv2)

    def test_different_seeds_differ(self):
        """Different seeds produce different results."""
        probs = np.array([0.6, 0.7, 0.8, 0.5, 0.9])
        weights = np.ones(5)

        sv1 = monte_carlo_shapley(probs, weights, outcome=1, k_permutations=100, seed=1)
        sv2 = monte_carlo_shapley(probs, weights, outcome=1, k_permutations=100, seed=2)

        assert not np.allclose(sv1, sv2), "Different seeds should give different values"

    def test_convergence_with_more_permutations(self):
        """Higher K gives lower variance (closer estimates across runs with different seeds)."""
        probs = np.array([0.6, 0.7, 0.8, 0.5, 0.9])
        weights = np.ones(5)

        # Low K: high variance
        low_k_results = [
            monte_carlo_shapley(probs, weights, outcome=1, k_permutations=50, seed=i)
            for i in range(10)
        ]
        low_k_std = np.std([r[0] for r in low_k_results])

        # High K: low variance
        high_k_results = [
            monte_carlo_shapley(probs, weights, outcome=1, k_permutations=500, seed=i + 100)
            for i in range(10)
        ]
        high_k_std = np.std([r[0] for r in high_k_results])

        assert high_k_std < low_k_std, (
            f"Higher K should reduce variance. Low K std={low_k_std:.6f}, High K std={high_k_std:.6f}"
        )

    def test_single_miner(self):
        """Single miner gets full improvement over uninformative prior."""
        prob = 0.8
        outcome = 1
        sv = monte_carlo_shapley(np.array([prob]), np.array([1.0]), outcome, seed=42)

        # Expected: Brier(0.5, 1) - Brier(0.8, 1) = 0.25 - 0.04 = 0.21
        expected = (0.5 - outcome) ** 2 - (prob - outcome) ** 2
        assert abs(sv[0] - expected) < 1e-10, f"Expected {expected}, got {sv[0]}"

    def test_empty_miners(self):
        """Empty input returns empty array."""
        sv = monte_carlo_shapley(np.array([]), np.array([]), outcome=1)
        assert len(sv) == 0

    def test_harmful_miner_gets_negative(self):
        """A miner whose prediction hurts the aggregate gets negative Shapley."""
        outcome = 1
        # 4 good miners predicting ~0.8, 1 bad miner predicting 0.1
        probs = np.array([0.8, 0.8, 0.8, 0.8, 0.1])
        weights = np.ones(5)

        sv = monte_carlo_shapley(probs, weights, outcome, k_permutations=1000, seed=42)

        assert sv[4] < 0, f"Harmful miner should have negative Shapley, got {sv[4]:.4f}"
        assert all(sv[:4] > 0), "Good miners should have positive Shapley"

    def test_shapley_values_sum_to_coalition_value(self):
        """Shapley values should approximately sum to V(N) - V(empty).

        This is the efficiency property of Shapley values.
        """
        probs = np.array([0.6, 0.7, 0.8, 0.75])
        weights = np.ones(4)
        outcome = 1

        sv = monte_carlo_shapley(probs, weights, outcome, k_permutations=2000, seed=42)

        # V(N) = -Brier(full_agg)
        from sparket.validator.scoring.aggregation.log_pool import build_log_pool, brier_score
        full_agg = build_log_pool(probs, weights)
        v_full = -brier_score(full_agg, outcome)
        v_empty = -brier_score(0.5, outcome)  # V(empty) = -Brier(0.5)

        expected_sum = v_full - v_empty
        actual_sum = sv.sum()

        # Allow 5% tolerance due to Monte Carlo
        assert abs(actual_sum - expected_sum) < abs(expected_sum) * 0.05 + 0.001, (
            f"Shapley sum ({actual_sum:.4f}) should ≈ V(N)-V(∅) ({expected_sum:.4f})"
        )

    @pytest.mark.slow
    def test_performance_256_miners(self):
        """256 miners × K=500 should complete in < 30 seconds."""
        rng = np.random.default_rng(42)
        n = 256
        probs = rng.beta(2, 2, size=n)  # Realistic spread
        weights = rng.random(n)
        outcome = 1

        start = time.time()
        sv = monte_carlo_shapley(probs, weights, outcome, k_permutations=500, seed=42)
        elapsed = time.time() - start

        assert elapsed < 30.0, f"256 miners × K=500 took {elapsed:.1f}s, should be < 30s"
        assert len(sv) == n
        # Sanity: not all zeros
        assert np.any(sv != 0)


class TestMultiOutcomeShapley:
    """Tests for multi-side market Shapley averaging."""

    def test_single_side(self):
        """Single-side market matches monte_carlo_shapley directly."""
        probs = np.array([0.6, 0.7, 0.8])
        weights = np.ones(3)

        sv_direct = monte_carlo_shapley(probs, weights, outcome=1, k_permutations=200, seed=42)
        sv_multi = shapley_for_multi_outcome_market(
            {"home": probs},
            weights,
            {"home": 1},
            k_permutations=200,
            seed=42,
        )

        np.testing.assert_array_almost_equal(sv_direct, sv_multi, decimal=10)

    def test_two_sides_averaged(self):
        """Two-side market averages Shapley across sides."""
        n = 5
        rng = np.random.default_rng(42)
        probs_home = rng.beta(2, 3, size=n)
        probs_away = 1.0 - probs_home  # Complementary
        weights = np.ones(n)

        sv = shapley_for_multi_outcome_market(
            {"home": probs_home, "away": probs_away},
            weights,
            {"home": 1, "away": 0},
            k_permutations=500,
            seed=42,
        )

        assert len(sv) == n
        # Values should be finite
        assert np.all(np.isfinite(sv))
