"""Pairwise miner correlation computation.

Computes the N×N Pearson correlation matrix between miners based on
their submission probabilities over common markets. Used by the
CompositeUniquenessJob to compute SOS_crowd.

All functions are pure NumPy — no side effects, no database access.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def compute_pairwise_correlations(
    submissions: dict[str, NDArray],
    market_ids: NDArray,
    min_common: int = 10,
) -> NDArray:
    """Compute pairwise Pearson correlations between all miners.

    Args:
        submissions: Dict mapping miner_id (str) to shape (M,) array of
            implied probabilities, aligned with market_ids. NaN for markets
            the miner did not submit on.
        market_ids: Shape (M,) array of market identifiers (for alignment).
        min_common: Minimum common non-NaN markets for a valid correlation.

    Returns:
        Shape (N, N) correlation matrix. Diagonal = 1.0. Pairs with
        < min_common overlap = 0.0 (treated as independent).
    """
    miner_ids = sorted(submissions.keys())
    n = len(miner_ids)
    if n == 0:
        return np.zeros((0, 0))

    # Build (N, M) matrix
    mat = np.full((n, len(market_ids)), np.nan)
    for i, mid in enumerate(miner_ids):
        mat[i] = submissions[mid]

    corr = np.eye(n)
    for i in range(n):
        for j in range(i + 1, n):
            mask = ~np.isnan(mat[i]) & ~np.isnan(mat[j])
            n_common = mask.sum()
            if n_common < min_common:
                continue  # Leave as 0.0 (independent)
            vi = mat[i, mask]
            vj = mat[j, mask]
            # Avoid division by zero for constant submissions
            std_i = vi.std()
            std_j = vj.std()
            if std_i < 1e-12 or std_j < 1e-12:
                # One miner submitted identical values — correlation undefined,
                # treat as perfectly correlated (sybil signal)
                corr[i, j] = corr[j, i] = 1.0
            else:
                r = np.corrcoef(vi, vj)[0, 1]
                corr[i, j] = corr[j, i] = r if np.isfinite(r) else 0.0

    return corr


def compute_sos_crowd(corr_matrix: NDArray, miner_idx: int) -> float:
    """Compute SOS_crowd for a single miner.

    SOS_crowd = 1 - mean(|corr(miner_i, miner_j)|) for all j != i.

    Args:
        corr_matrix: Shape (N, N) correlation matrix.
        miner_idx: Index of the target miner.

    Returns:
        SOS_crowd score in [0, 1]. Higher = more independent.
    """
    n = corr_matrix.shape[0]
    if n <= 1:
        return 1.0  # Only miner → fully independent
    # Exclude self-correlation
    others = np.abs(np.concatenate([
        corr_matrix[miner_idx, :miner_idx],
        corr_matrix[miner_idx, miner_idx + 1:],
    ]))
    return float(1.0 - others.mean())
