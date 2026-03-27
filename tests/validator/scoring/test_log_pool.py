"""Tests for weighted logarithmic opinion pool."""

import numpy as np

from sparket.validator.scoring.aggregation.log_pool import (
    build_log_pool,
    loo_log_pool,
    evaluate_subset,
    brier_score,
    _logit,
    _sigmoid,
)


class TestBuildLogPool:
    def test_equal_weights(self):
        """3 miners with equal weights → mean of logits."""
        probs = np.array([0.6, 0.7, 0.8])
        weights = np.array([1.0, 1.0, 1.0])
        agg = build_log_pool(probs, weights)
        # Manual: mean(logit(0.6), logit(0.7), logit(0.8)) → sigmoid
        expected_logit = np.mean(_logit(probs))
        expected = float(_sigmoid(expected_logit))
        assert abs(agg - expected) < 1e-10

    def test_single_miner(self):
        """Single miner → aggregate = that miner's prob."""
        agg = build_log_pool(np.array([0.75]), np.array([1.0]))
        assert abs(agg - 0.75) < 1e-10

    def test_empty(self):
        """Empty input → 0.5 (uninformative prior)."""
        assert build_log_pool(np.array([]), np.array([])) == 0.5

    def test_extreme_probs_clamped(self):
        """Extreme probs (0.01, 0.99) are clamped and don't produce inf."""
        probs = np.array([0.001, 0.999, 0.5])
        weights = np.ones(3)
        agg = build_log_pool(probs, weights, epsilon=0.005)
        assert np.isfinite(agg)
        assert 0 < agg < 1

    def test_weighted(self):
        """Higher weight miner pulls aggregate toward their prediction."""
        probs = np.array([0.3, 0.9])
        agg_equal = build_log_pool(probs, np.array([1.0, 1.0]))
        agg_first_heavy = build_log_pool(probs, np.array([10.0, 1.0]))
        agg_second_heavy = build_log_pool(probs, np.array([1.0, 10.0]))
        assert agg_first_heavy < agg_equal < agg_second_heavy

    def test_zero_weight_miner_excluded(self):
        """A miner with zero weight doesn't affect the aggregate."""
        probs = np.array([0.7, 0.8, 0.1])
        w_with = np.array([1.0, 1.0, 0.0])
        w_without = np.array([1.0, 1.0])
        agg_with = build_log_pool(probs, w_with)
        agg_without = build_log_pool(probs[:2], w_without)
        assert abs(agg_with - agg_without) < 1e-10


class TestLOOLogPool:
    def test_loo_matches_brute_force(self):
        """LOO via subtraction matches recomputing pool from scratch."""
        probs = np.array([0.6, 0.7, 0.8, 0.5, 0.9])
        weights = np.ones(5) / 5
        logits = _logit(probs)
        full_logit_sum = np.dot(weights, logits)
        full_weight_sum = weights.sum()

        for i in range(5):
            loo = loo_log_pool(full_logit_sum, full_weight_sum, logits[i], weights[i])
            # Brute force: remove miner i
            mask = np.ones(5, dtype=bool)
            mask[i] = False
            brute = build_log_pool(probs[mask], weights[mask])
            assert abs(loo - brute) < 1e-10, f"Miner {i}: cached={loo}, brute={brute}"

    def test_loo_single_remaining(self):
        """Removing one of two miners → aggregate = remaining miner."""
        probs = np.array([0.6, 0.8])
        weights = np.array([0.5, 0.5])
        logits = _logit(probs)
        full_logit_sum = np.dot(weights, logits)

        loo_remove_0 = loo_log_pool(full_logit_sum, 1.0, logits[0], weights[0])
        assert abs(loo_remove_0 - 0.8) < 1e-10

    def test_loo_all_removed(self):
        """Removing the only miner → 0.5 (uninformative)."""
        logits = _logit(np.array([0.7]))
        loo = loo_log_pool(logits[0], 1.0, logits[0], 1.0)
        assert loo == 0.5


class TestEvaluateSubset:
    def test_full_subset_matches_build(self):
        """Full subset mask matches build_log_pool."""
        probs = np.array([0.6, 0.7, 0.8])
        weights = np.ones(3)
        logits = _logit(probs)
        mask = np.ones(3, dtype=bool)

        sub = evaluate_subset(logits, weights, mask)
        full = build_log_pool(probs, weights)
        assert abs(sub - full) < 1e-10

    def test_empty_subset(self):
        """Empty subset → 0.5."""
        logits = _logit(np.array([0.6, 0.7]))
        mask = np.zeros(2, dtype=bool)
        assert evaluate_subset(logits, np.ones(2), mask) == 0.5

    def test_single_element_subset(self):
        """Subset with one miner → that miner's prob."""
        probs = np.array([0.6, 0.8])
        logits = _logit(probs)
        mask = np.array([False, True])
        sub = evaluate_subset(logits, np.ones(2), mask)
        assert abs(sub - 0.8) < 1e-10


class TestBrierScore:
    def test_perfect(self):
        assert brier_score(1.0, 1) == 0.0
        assert brier_score(0.0, 0) == 0.0

    def test_worst(self):
        assert brier_score(0.0, 1) == 1.0
        assert brier_score(1.0, 0) == 1.0

    def test_midpoint(self):
        assert brier_score(0.5, 1) == 0.25
        assert brier_score(0.5, 0) == 0.25
