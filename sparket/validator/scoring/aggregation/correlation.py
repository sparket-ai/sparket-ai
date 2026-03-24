"""Pairwise miner correlation computation.

Computes the N×N Pearson correlation matrix between miners based on
their submission probabilities over common markets. Used by the
CompositeUniquenessJob to compute SOS_crowd.

All functions are pure NumPy — no side effects, no database access.
"""

from __future__ import annotations

import numpy as np
from numpy.typing import NDArray


def compute_pairwise_correlations_windowed(
    submissions: dict[str, NDArray],
    market_ids: NDArray,
    market_half_mask: NDArray,
    min_common: int = 10,
) -> NDArray:
    """Rotation-resistant correlation: max of full-window and per-half correlations.

    Attackers can alternate correlated/divergent behavior across time.
    By computing correlation on each half of the window independently
    and taking the element-wise max, a burst of correlation in either
    half is detected even if the full-window average is moderate.

    Args:
        submissions: Dict mapping miner_id to shape (M,) prob arrays.
        market_ids: Shape (M,) market identifiers.
        market_half_mask: Shape (M,) boolean array. True = first half,
            False = second half. Based on earliest submission timestamp.
        min_common: Minimum common markets for a valid correlation.

    Returns:
        Shape (N, N) correlation matrix where each entry is
        max(|full|, |first_half|, |second_half|).
    """
    full = compute_pairwise_correlations(submissions, market_ids, min_common)

    # Split submissions into halves
    first_mask = market_half_mask
    second_mask = ~market_half_mask

    subs_first = {}
    subs_second = {}
    for mid, arr in submissions.items():
        arr_first = arr.copy()
        arr_first[second_mask] = np.nan
        subs_first[mid] = arr_first

        arr_second = arr.copy()
        arr_second[first_mask] = np.nan
        subs_second[mid] = arr_second

    # Half-window min_common is halved (fewer markets per half)
    half_min = max(3, min_common // 2)
    corr_first = compute_pairwise_correlations(subs_first, market_ids, half_min)
    corr_second = compute_pairwise_correlations(subs_second, market_ids, half_min)

    # Element-wise max of absolute values, preserving sign of the max
    combined = np.where(
        np.abs(full) >= np.maximum(np.abs(corr_first), np.abs(corr_second)),
        full,
        np.where(np.abs(corr_first) >= np.abs(corr_second), corr_first, corr_second),
    )
    np.fill_diagonal(combined, 1.0)
    return combined


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


def compute_sos_crowd(
    corr_matrix: NDArray,
    miner_idx: int,
    max_corr_weight: float = 0.0,
) -> float:
    """Compute SOS_crowd for a single miner.

    Base formula: 1 - mean(|corr(i, j)|) for all j != i.

    When max_corr_weight > 0, blends in the worst-case pairwise
    correlation so that even a single highly-correlated peer in a
    large network triggers a meaningful penalty:

        effective = max(mean_corr, max_corr * max_corr_weight)
        SOS_crowd = 1 - effective

    Args:
        corr_matrix: Shape (N, N) correlation matrix.
        miner_idx: Index of the target miner.
        max_corr_weight: Weight for the max pairwise correlation
            term (0 = legacy mean-only behavior).

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
    mean_corr = float(others.mean())
    if max_corr_weight > 0:
        max_corr = float(others.max())
        effective = max(mean_corr, max_corr * max_corr_weight)
    else:
        effective = mean_corr
    return float(1.0 - effective)


def compute_low_overlap_penalty(
    submissions: dict[str, NDArray],
    min_common: int = 10,
    low_overlap_threshold: float = 0.15,
    max_unscoreable: float = 0.5,
) -> dict[str, float]:
    """Penalize miners with suspiciously low market overlap with peers.

    Sybils can evade correlation detection by submitting on non-overlapping
    market sets (< min_common shared markets → correlation defaults to 0).
    This function detects that pattern: if a miner shares fewer than
    min_common markets with an unusually high fraction of peers, they
    receive a penalty.

    Args:
        submissions: Dict mapping miner_id to shape (M,) array with NaN
            for markets not submitted on.
        min_common: The same min_common threshold used for correlation.
        low_overlap_threshold: Baseline unscoreable fraction below which
            no penalty is applied (some non-overlap is normal).
        max_unscoreable: Fraction of unscoreable peers at which penalty
            reaches 1.0.

    Returns:
        Dict mapping miner_id to penalty in [0, 1]. 0 = no penalty.
    """
    miner_ids = sorted(submissions.keys())
    n = len(miner_ids)
    if n <= 1:
        return {mid: 0.0 for mid in miner_ids}

    # Precompute non-NaN masks
    masks = {mid: ~np.isnan(submissions[mid]) for mid in miner_ids}

    penalties: dict[str, float] = {}
    for mid in miner_ids:
        unscoreable = 0
        for other in miner_ids:
            if other == mid:
                continue
            common = int((masks[mid] & masks[other]).sum())
            if common < min_common:
                unscoreable += 1
        frac = unscoreable / (n - 1)
        # Linear ramp from threshold to max_unscoreable
        if frac <= low_overlap_threshold:
            penalties[mid] = 0.0
        elif max_unscoreable <= low_overlap_threshold:
            penalties[mid] = 1.0 if frac > low_overlap_threshold else 0.0
        else:
            raw = (frac - low_overlap_threshold) / (max_unscoreable - low_overlap_threshold)
            penalties[mid] = float(min(1.0, max(0.0, raw)))

    return penalties
