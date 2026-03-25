"""Spectral clustering for sybil detection.

Detects clusters of highly correlated miners using connected components
on a thresholded correlation graph. Computes ClusterPenalty for each miner.

All functions are pure NumPy + scipy — no database access.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def detect_clusters(
    corr_matrix: NDArray,
    threshold: float = 0.85,
) -> list[list[int]]:
    """Detect clusters of correlated miners using connected components.

    Builds a graph where edge (i, j) exists if |corr(i, j)| > threshold,
    then finds connected components.

    Args:
        corr_matrix: Shape (N, N) pairwise correlation matrix.
        threshold: Correlation threshold for edge creation.

    Returns:
        List of clusters, where each cluster is a list of miner indices.
        Singletons are included.
    """
    n = corr_matrix.shape[0]
    if n == 0:
        return []

    # Build adjacency via thresholding (excluding diagonal)
    adj = (np.abs(corr_matrix) > threshold).astype(bool)
    np.fill_diagonal(adj, False)

    # Connected components via BFS
    visited = np.zeros(n, dtype=bool)
    clusters: list[list[int]] = []

    for start in range(n):
        if visited[start]:
            continue
        # BFS
        cluster = []
        queue = [start]
        visited[start] = True
        while queue:
            node = queue.pop(0)
            cluster.append(node)
            for neighbor in range(n):
                if adj[node, neighbor] and not visited[neighbor]:
                    visited[neighbor] = True
                    queue.append(neighbor)
        clusters.append(cluster)

    return clusters


def compute_cluster_penalty(
    clusters: list[list[int]],
    miner_idx: int,
) -> float:
    """Compute ClusterPenalty for a miner.

    ClusterPenalty = (cluster_size - 1) / cluster_size.
    Singletons get penalty 0. A 5-miner cluster → penalty 0.8.

    Args:
        clusters: List of clusters (from detect_clusters).
        miner_idx: Index of the target miner.

    Returns:
        ClusterPenalty in [0, 1). 0 = singleton (no penalty).
    """
    for cluster in clusters:
        if miner_idx in cluster:
            size = len(cluster)
            if size <= 1:
                return 0.0
            return (size - 1) / size
    return 0.0  # Not found — treat as singleton


def compute_soft_cluster_penalty(
    corr_matrix: NDArray,
    miner_idx: int,
    soft_floor: float = 0.5,
    soft_ceil: float = 0.95,
) -> float:
    """Continuous cluster penalty based on max pairwise correlation.

    Unlike the hard threshold in detect_clusters (binary: in or out),
    this ramps linearly between soft_floor and soft_ceil, closing the
    gap where attackers calibrate noise to sit just below the threshold.

    Args:
        corr_matrix: Shape (N, N) pairwise correlation matrix.
        miner_idx: Index of the target miner.
        soft_floor: Correlation below which penalty is 0.
        soft_ceil: Correlation at or above which penalty is 1.

    Returns:
        Penalty in [0, 1]. 0 = no suspicious peers. 1 = strong sybil signal.
    """
    n = corr_matrix.shape[0]
    if n <= 1:
        return 0.0

    others = np.abs(np.concatenate([
        corr_matrix[miner_idx, :miner_idx],
        corr_matrix[miner_idx, miner_idx + 1:],
    ]))
    max_corr = float(others.max())

    if max_corr <= soft_floor:
        return 0.0
    if max_corr >= soft_ceil:
        return 1.0
    return (max_corr - soft_floor) / (soft_ceil - soft_floor)
