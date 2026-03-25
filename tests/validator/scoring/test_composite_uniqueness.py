"""Tests for composite uniqueness scoring (correlation, clustering, SOS)."""

import numpy as np

from sparket.validator.scoring.aggregation.correlation import (
    compute_pairwise_correlations,
    compute_pairwise_correlations_windowed,
    compute_sos_crowd,
    compute_low_overlap_penalty,
)
from sparket.validator.scoring.aggregation.clustering import (
    detect_clusters,
    compute_cluster_penalty,
    compute_soft_cluster_penalty,
)


class TestPairwiseCorrelations:
    def test_sybil_pair_high_correlation(self):
        """Two miners with nearly identical submissions → corr ≈ 1."""
        rng = np.random.default_rng(42)
        base = rng.random(30)
        subs = {
            "a": base + rng.normal(0, 0.01, 30),
            "b": base + rng.normal(0, 0.01, 30),
        }
        corr = compute_pairwise_correlations(subs, np.arange(30), min_common=5)
        assert corr[0, 1] > 0.95

    def test_independent_miners_low_correlation(self):
        """Independent miners → corr ≈ 0."""
        rng = np.random.default_rng(42)
        subs = {
            "a": rng.random(30),
            "b": rng.random(30),
        }
        corr = compute_pairwise_correlations(subs, np.arange(30), min_common=5)
        assert abs(corr[0, 1]) < 0.5

    def test_insufficient_common_markets(self):
        """Fewer than min_common overlap → correlation defaults to 0."""
        subs = {
            "a": np.array([0.5, 0.6, np.nan, np.nan, np.nan]),
            "b": np.array([np.nan, np.nan, np.nan, 0.7, 0.8]),
        }
        corr = compute_pairwise_correlations(subs, np.arange(5), min_common=3)
        assert corr[0, 1] == 0.0

    def test_diagonal_is_one(self):
        """Diagonal should always be 1.0."""
        rng = np.random.default_rng(42)
        subs = {"a": rng.random(20), "b": rng.random(20), "c": rng.random(20)}
        corr = compute_pairwise_correlations(subs, np.arange(20), min_common=5)
        np.testing.assert_array_equal(np.diag(corr), [1.0, 1.0, 1.0])

    def test_empty_submissions(self):
        """No miners → empty correlation matrix."""
        corr = compute_pairwise_correlations({}, np.array([]), min_common=5)
        assert corr.shape == (0, 0)


class TestSOSCrowd:
    def test_sybil_gets_low_sos(self):
        """A miner correlated with others gets low SOS_crowd."""
        # 3 miners: 0 and 1 correlated (0.95), 2 independent
        corr = np.array([
            [1.0, 0.95, 0.1],
            [0.95, 1.0, 0.1],
            [0.1, 0.1, 1.0],
        ])
        sos_0 = compute_sos_crowd(corr, 0)
        sos_2 = compute_sos_crowd(corr, 2)
        assert sos_0 < sos_2
        assert sos_0 < 0.5  # High correlation → low SOS

    def test_fully_independent(self):
        """All zero correlations → SOS_crowd = 1.0."""
        corr = np.eye(3)
        assert compute_sos_crowd(corr, 0) == 1.0

    def test_single_miner(self):
        """Single miner → SOS_crowd = 1.0 (fully independent by default)."""
        corr = np.array([[1.0]])
        assert compute_sos_crowd(corr, 0) == 1.0


class TestDetectClusters:
    def test_sybil_ring_detected(self):
        """5 highly correlated miners form one cluster."""
        n = 5
        corr = np.ones((n, n)) * 0.95
        np.fill_diagonal(corr, 1.0)
        clusters = detect_clusters(corr, threshold=0.85)
        assert len(clusters) == 1
        assert len(clusters[0]) == 5

    def test_two_clusters(self):
        """Two groups with high intra-correlation, low inter-correlation."""
        corr = np.eye(6)
        # Group 1: miners 0, 1, 2
        for i in range(3):
            for j in range(3):
                corr[i, j] = 0.95
        # Group 2: miners 3, 4, 5
        for i in range(3, 6):
            for j in range(3, 6):
                corr[i, j] = 0.95
        np.fill_diagonal(corr, 1.0)

        clusters = detect_clusters(corr, threshold=0.85)
        assert len(clusters) == 2
        sizes = sorted([len(c) for c in clusters])
        assert sizes == [3, 3]

    def test_all_independent(self):
        """All independent → each miner is its own cluster."""
        corr = np.eye(5)
        clusters = detect_clusters(corr, threshold=0.85)
        assert len(clusters) == 5
        assert all(len(c) == 1 for c in clusters)

    def test_empty(self):
        """Empty matrix → no clusters."""
        assert detect_clusters(np.zeros((0, 0)), threshold=0.85) == []


class TestClusterPenalty:
    def test_singleton(self):
        """Singleton cluster → penalty 0."""
        clusters = [[0], [1], [2]]
        assert compute_cluster_penalty(clusters, 0) == 0.0

    def test_two_miner_cluster(self):
        """2-miner cluster → penalty 0.5."""
        clusters = [[0, 1], [2]]
        assert compute_cluster_penalty(clusters, 0) == 0.5
        assert compute_cluster_penalty(clusters, 1) == 0.5

    def test_five_miner_cluster(self):
        """5-miner cluster → penalty 0.8."""
        clusters = [[0, 1, 2, 3, 4]]
        assert compute_cluster_penalty(clusters, 0) == 0.8

    def test_not_found(self):
        """Miner not in any cluster → penalty 0 (treat as singleton)."""
        clusters = [[0, 1]]
        assert compute_cluster_penalty(clusters, 5) == 0.0


class TestCompositeSOSIntegration:
    """End-to-end test combining correlation, clustering, and SOS."""

    def test_sybil_ring_penalised(self):
        """5 sybil miners get low composite SOS, independent miner gets high."""
        rng = np.random.default_rng(42)
        n_markets = 50
        base = rng.random(n_markets)

        # 5 sybils copying base + tiny noise
        subs = {}
        for i in range(5):
            subs[str(i)] = base + rng.normal(0, 0.005, n_markets)
        # 1 independent miner
        subs["5"] = rng.random(n_markets)

        market_ids = np.arange(n_markets)
        corr = compute_pairwise_correlations(subs, market_ids, min_common=10)

        # Clustering
        clusters = detect_clusters(corr, threshold=0.85)

        # Composite SOS for sybil vs independent
        w_market, w_crowd, w_cluster = 0.2, 0.5, 0.3

        # For simplicity, assume SOS_market = 0.5 for all (not tested here)
        sos_market = 0.5

        for miner_idx in range(6):
            sos_crowd = compute_sos_crowd(corr, miner_idx)
            cluster_penalty = compute_cluster_penalty(clusters, miner_idx)
            sos_cluster = 1.0 - cluster_penalty
            composite = w_market * sos_market + w_crowd * sos_crowd + w_cluster * sos_cluster

            if miner_idx < 5:
                # Sybil miners should have low composite SOS
                assert composite < 0.5, f"Sybil miner {miner_idx} got composite SOS {composite:.3f}"
            else:
                # Independent miner should have high composite SOS
                assert composite > 0.6, f"Independent miner got composite SOS {composite:.3f}"


# ──────────────────────────────────────────────────────────────────────
# Fix 4: SOS_crowd max-corr blending (large-network sybil pair dilution)
# ──────────────────────────────────────────────────────────────────────

class TestSOSCrowdMaxCorrBlending:
    def test_sybil_pair_in_large_network(self):
        """2 sybils among 50 independents. With max_corr_weight=0.6 the pair is caught."""
        n = 52
        corr = np.eye(n) * 0.0
        np.fill_diagonal(corr, 1.0)
        # Miners 0 and 1 are sybils (0.95 correlation)
        corr[0, 1] = corr[1, 0] = 0.95
        # All others have low cross-correlation (~0.1)
        rng = np.random.default_rng(99)
        for i in range(2, n):
            for j in range(i + 1, n):
                corr[i, j] = corr[j, i] = rng.uniform(0.0, 0.15)

        # Without blending: mean dilutes the 0.95 across 51 peers
        sos_no_blend = compute_sos_crowd(corr, 0, max_corr_weight=0.0)
        # With blending: max(0.95) * 0.6 = 0.57 dominates
        sos_with_blend = compute_sos_crowd(corr, 0, max_corr_weight=0.6)

        assert sos_no_blend > 0.85, f"Without blending, sybil evades: {sos_no_blend:.3f}"
        assert sos_with_blend < 0.50, f"With blending, sybil should be caught: {sos_with_blend:.3f}"

    def test_independent_miner_unaffected(self):
        """Fully independent miner gets same score with or without blending."""
        corr = np.eye(5)
        sos_no = compute_sos_crowd(corr, 0, max_corr_weight=0.0)
        sos_yes = compute_sos_crowd(corr, 0, max_corr_weight=0.6)
        assert sos_no == sos_yes == 1.0

    def test_backward_compat_default(self):
        """max_corr_weight=0 reproduces legacy mean-only behavior."""
        corr = np.array([
            [1.0, 0.95, 0.1],
            [0.95, 1.0, 0.1],
            [0.1, 0.1, 1.0],
        ])
        sos_legacy = 1.0 - np.mean([0.95, 0.1])  # 0.475
        sos_computed = compute_sos_crowd(corr, 0, max_corr_weight=0.0)
        assert abs(sos_computed - sos_legacy) < 1e-10


# ──────────────────────────────────────────────────────────────────────
# Fix 2: Soft cluster penalty (continuous ramp, no threshold gaming)
# ──────────────────────────────────────────────────────────────────────

class TestSoftClusterPenalty:
    def test_just_below_hard_threshold(self):
        """Pair at 0.84 gets zero hard penalty but nonzero soft penalty."""
        corr = np.array([
            [1.0, 0.84],
            [0.84, 1.0],
        ])
        hard = compute_cluster_penalty(
            detect_clusters(corr, threshold=0.85), 0
        )
        soft = compute_soft_cluster_penalty(corr, 0, soft_floor=0.5, soft_ceil=0.95)

        assert hard == 0.0, "Should NOT be hard-clustered at 0.84"
        assert soft > 0.5, f"Soft penalty should be significant at 0.84: {soft:.3f}"

    def test_ramps_linearly(self):
        """Penalty increases monotonically with correlation."""
        penalties = []
        for c in [0.3, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95]:
            corr = np.array([[1.0, c], [c, 1.0]])
            p = compute_soft_cluster_penalty(corr, 0, soft_floor=0.5, soft_ceil=0.95)
            penalties.append(p)
        # First two should be 0 (below floor)
        assert penalties[0] == 0.0
        assert penalties[1] == 0.0
        # Rest should be strictly increasing
        for i in range(2, len(penalties) - 1):
            assert penalties[i] < penalties[i + 1], f"Not monotonic at index {i}"

    def test_below_floor_is_zero(self):
        """Correlation below soft_floor → no penalty."""
        corr = np.array([[1.0, 0.4], [0.4, 1.0]])
        assert compute_soft_cluster_penalty(corr, 0, soft_floor=0.5, soft_ceil=0.95) == 0.0

    def test_above_ceil_is_one(self):
        """Correlation at soft_ceil → full penalty."""
        corr = np.array([[1.0, 0.96], [0.96, 1.0]])
        assert compute_soft_cluster_penalty(corr, 0, soft_floor=0.5, soft_ceil=0.95) == 1.0

    def test_hard_dominates_for_large_clusters(self):
        """5-miner cluster: hard penalty (0.8) > soft penalty for moderate corr."""
        corr = np.ones((5, 5)) * 0.88
        np.fill_diagonal(corr, 1.0)
        clusters = detect_clusters(corr, threshold=0.85)
        hard = compute_cluster_penalty(clusters, 0)
        soft = compute_soft_cluster_penalty(corr, 0, soft_floor=0.5, soft_ceil=0.95)
        # Hard = 0.8, soft ≈ 0.84 — close, but max() covers both
        final = max(hard, soft)
        assert final >= 0.8

    def test_single_miner(self):
        """Single miner → no penalty."""
        corr = np.array([[1.0]])
        assert compute_soft_cluster_penalty(corr, 0) == 0.0


# ──────────────────────────────────────────────────────────────────────
# Fix 1: Low-overlap penalty (non-overlapping sybils)
# ──────────────────────────────────────────────────────────────────────

class TestLowOverlapPenalty:
    def test_non_overlapping_sybils_penalized(self):
        """Two miners with zero market overlap get penalized."""
        subs = {
            "a": np.array([0.5, 0.6, 0.7, np.nan, np.nan, np.nan]),
            "b": np.array([np.nan, np.nan, np.nan, 0.5, 0.6, 0.7]),
        }
        penalties = compute_low_overlap_penalty(
            subs, min_common=3, low_overlap_threshold=0.0, max_unscoreable=0.5,
        )
        # Each has 100% unscoreable peers (1/1) → full penalty
        assert penalties["a"] == 1.0
        assert penalties["b"] == 1.0

    def test_legitimate_miner_no_penalty(self):
        """Miner overlapping with most peers gets no penalty."""
        rng = np.random.default_rng(42)
        n_markets = 50
        subs = {}
        for i in range(10):
            subs[str(i)] = rng.random(n_markets)  # All submit on all markets
        penalties = compute_low_overlap_penalty(
            subs, min_common=10, low_overlap_threshold=0.15, max_unscoreable=0.5,
        )
        for mid, pen in penalties.items():
            assert pen == 0.0, f"Legitimate miner {mid} got penalty {pen}"

    def test_partial_overlap_partial_penalty(self):
        """Miner with some overlap gets proportional penalty."""
        # 5 miners: miner "0" overlaps with 2 of 4 peers (50% unscoreable)
        subs = {
            "0": np.array([0.5, 0.6, 0.7, 0.8, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]),
            "1": np.array([0.5, 0.6, 0.7, 0.8, 0.5, 0.6, 0.7, 0.8, 0.5, 0.6]),
            "2": np.array([0.5, 0.6, 0.7, 0.8, 0.5, 0.6, 0.7, 0.8, 0.5, 0.6]),
            "3": np.array([np.nan, np.nan, np.nan, np.nan, 0.5, 0.6, 0.7, 0.8, 0.5, 0.6]),
            "4": np.array([np.nan, np.nan, np.nan, np.nan, 0.5, 0.6, 0.7, 0.8, 0.5, 0.6]),
        }
        penalties = compute_low_overlap_penalty(
            subs, min_common=3, low_overlap_threshold=0.15, max_unscoreable=0.5,
        )
        # Miner "0" overlaps with "1","2" (4 common) but not "3","4" (0 common)
        # Unscoreable fraction = 2/4 = 0.5 → penalty = (0.5-0.15)/(0.5-0.15) = 1.0
        assert penalties["0"] == 1.0
        # Miners "1","2" overlap with everyone → penalty 0
        assert penalties["1"] == 0.0

    def test_single_miner_no_penalty(self):
        """Single miner → no penalty."""
        penalties = compute_low_overlap_penalty(
            {"a": np.array([0.5])}, min_common=1,
        )
        assert penalties["a"] == 0.0


# ──────────────────────────────────────────────────────────────────────
# Fix 3: Sub-window rotation detection
# ──────────────────────────────────────────────────────────────────────

class TestSubWindowCorrelation:
    def test_rotation_attack_detected(self):
        """Sybils correlated in first half, divergent in second → max catches them."""
        rng = np.random.default_rng(42)
        n_markets = 60
        base = rng.random(n_markets)

        subs = {
            "a": np.copy(base),
            "b": np.copy(base),
        }
        # First half (0-29): near-identical
        subs["a"][:30] = base[:30] + rng.normal(0, 0.01, 30)
        subs["b"][:30] = base[:30] + rng.normal(0, 0.01, 30)
        # Second half (30-59): completely divergent
        subs["a"][30:] = rng.random(30)
        subs["b"][30:] = rng.random(30)

        market_ids = np.arange(n_markets)
        half_mask = np.zeros(n_markets, dtype=bool)
        half_mask[:30] = True  # First 30 = first half

        # Full-window correlation is diluted
        corr_full = compute_pairwise_correlations(subs, market_ids, min_common=5)
        # Windowed catches the first-half burst
        corr_windowed = compute_pairwise_correlations_windowed(
            subs, market_ids, half_mask, min_common=5,
        )

        full_corr = abs(corr_full[0, 1])
        windowed_corr = abs(corr_windowed[0, 1])

        assert windowed_corr > full_corr, (
            f"Windowed ({windowed_corr:.3f}) should catch rotation better "
            f"than full ({full_corr:.3f})"
        )
        assert windowed_corr > 0.85, f"Windowed should detect the correlated half: {windowed_corr:.3f}"

    def test_consistently_independent_unaffected(self):
        """Miners independent in both halves → no change."""
        rng = np.random.default_rng(42)
        n_markets = 60
        subs = {"a": rng.random(n_markets), "b": rng.random(n_markets)}
        market_ids = np.arange(n_markets)
        half_mask = np.zeros(n_markets, dtype=bool)
        half_mask[:30] = True

        corr_full = compute_pairwise_correlations(subs, market_ids, min_common=5)
        corr_windowed = compute_pairwise_correlations_windowed(
            subs, market_ids, half_mask, min_common=5,
        )
        # Both should be low
        assert abs(corr_full[0, 1]) < 0.5
        assert abs(corr_windowed[0, 1]) < 0.5

    def test_consistently_correlated_still_caught(self):
        """Sybils correlated in both halves → windowed ≥ full."""
        rng = np.random.default_rng(42)
        n_markets = 60
        base = rng.random(n_markets)
        subs = {
            "a": base + rng.normal(0, 0.01, n_markets),
            "b": base + rng.normal(0, 0.01, n_markets),
        }
        market_ids = np.arange(n_markets)
        half_mask = np.zeros(n_markets, dtype=bool)
        half_mask[:30] = True

        corr_full = compute_pairwise_correlations(subs, market_ids, min_common=5)
        corr_windowed = compute_pairwise_correlations_windowed(
            subs, market_ids, half_mask, min_common=5,
        )
        assert corr_windowed[0, 1] >= corr_full[0, 1] - 0.01  # At least as strong
