"""Tests for composite uniqueness scoring (correlation, clustering, SOS)."""

import numpy as np

from sparket.validator.scoring.aggregation.correlation import (
    compute_pairwise_correlations,
    compute_sos_crowd,
)
from sparket.validator.scoring.aggregation.clustering import (
    detect_clusters,
    compute_cluster_penalty,
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
