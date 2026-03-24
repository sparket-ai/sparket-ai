"""Composite Uniqueness scoring job.

Computes the Uniqueness dimension for the Cobb-Douglas SkillScore by:
1. Building the N×N pairwise miner correlation matrix
2. Running connected-component clustering to detect sybil rings
3. Computing composite SOS = w_market * SOS_market + w_crowd * SOS_crowd + w_cluster * SOS_cluster

Depends on: OriginalityLeadLagJob (provides SOS_market via sos_score)
Feeds into: SkillScoreJob (provides uniqueness_dim)
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

import numpy as np
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params

from ..aggregation.correlation import (
    compute_pairwise_correlations,
    compute_pairwise_correlations_windowed,
    compute_sos_crowd,
    compute_low_overlap_penalty,
)
from ..aggregation.clustering import detect_clusters, compute_cluster_penalty, compute_soft_cluster_penalty
from ..determinism import get_canonical_window_bounds
from .base import ScoringJob


_SELECT_ACTIVE_MINERS = text(
    """
    SELECT DISTINCT miner_id, miner_hotkey
    FROM miner_submission
    WHERE submitted_at >= :window_start
      AND submitted_at < :window_end
    ORDER BY miner_id
    """
)

_SELECT_MINER_SUBMISSIONS_FOR_CORR = text(
    """
    SELECT
        miner_id,
        market_id,
        side,
        AVG(imp_prob) AS avg_prob,
        MIN(submitted_at) AS first_seen
    FROM miner_submission
    WHERE submitted_at >= :window_start
      AND submitted_at < :window_end
    GROUP BY miner_id, market_id, side
    ORDER BY miner_id, market_id, side
    """
)

_SELECT_EXISTING_SOS_MARKET = text(
    """
    SELECT miner_id, miner_hotkey, sos_score
    FROM miner_rolling_score
    WHERE as_of = :as_of
      AND window_days = :window_days
    ORDER BY miner_id
    """
)

_UPDATE_COMPOSITE_UNIQUENESS = text(
    """
    UPDATE miner_rolling_score
    SET sos_crowd = :sos_crowd,
        sos_cluster = :sos_cluster,
        sos_composite = :sos_composite,
        uniqueness_dim = :uniqueness_dim,
        cluster_id = :cluster_id,
        cluster_size = :cluster_size
    WHERE miner_id = :miner_id
      AND miner_hotkey = :miner_hotkey
      AND as_of = :as_of
      AND window_days = :window_days
    """
)

_INSERT_PAIRWISE_CORRELATION = text(
    """
    INSERT INTO miner_pairwise_correlation
        (miner_a_id, miner_b_id, as_of, correlation, n_common_markets)
    VALUES (:miner_a_id, :miner_b_id, :as_of, :correlation, :n_common_markets)
    ON CONFLICT (miner_a_id, miner_b_id, as_of) DO UPDATE
        SET correlation = :correlation,
            n_common_markets = :n_common_markets
    """
)


class CompositeUniquenessJob(ScoringJob):
    """Compute composite SOS and Uniqueness dimension for all miners."""

    JOB_ID = "composite_uniqueness"

    def __init__(
        self,
        db: Any,
        logger: logging.Logger,
        *,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        job_id_override: str | None = None,
    ):
        super().__init__(db, logger, job_id_override=job_id_override)
        self.params = get_scoring_params()
        self.window_start = window_start
        self.window_end = window_end

    async def execute(self) -> None:
        """Compute pairwise correlations, clustering, and composite SOS."""
        window_days = self.params.windows.rolling_window_days
        sos_params = self.params.composite_sos

        if self.window_start is not None and self.window_end is not None:
            window_start, window_end = self.window_start, self.window_end
        else:
            window_start, window_end = get_canonical_window_bounds(window_days)

        as_of = window_end
        self.logger.info(
            f"Computing composite uniqueness for {window_days}-day window "
            f"({window_start} to {window_end})"
        )

        # 1. Fetch all miner submissions aggregated by (miner_id, market_id, side)
        rows = await self.db.read(
            _SELECT_MINER_SUBMISSIONS_FOR_CORR,
            params={"window_start": window_start, "window_end": window_end},
            mappings=True,
        )

        if not rows:
            self.logger.info("No submissions found for correlation computation")
            return

        # Build submission matrix: for each miner, one probability per (market, side)
        # Use (market_id, side) as the "market" axis for correlation
        all_market_sides: Dict[tuple, int] = {}  # (market_id, side) -> index
        market_side_first_seen: Dict[tuple, datetime] = {}
        miner_data: Dict[int, Dict[tuple, float]] = defaultdict(dict)
        miner_hotkeys: Dict[int, str] = {}

        for row in rows:
            ms_key = (row["market_id"], row["side"])
            if ms_key not in all_market_sides:
                all_market_sides[ms_key] = len(all_market_sides)
            if ms_key not in market_side_first_seen and row.get("first_seen"):
                market_side_first_seen[ms_key] = row["first_seen"]
            miner_data[row["miner_id"]][ms_key] = float(row["avg_prob"])

        n_market_sides = len(all_market_sides)
        miner_ids = sorted(miner_data.keys())
        n_miners = len(miner_ids)
        miner_idx_map = {mid: i for i, mid in enumerate(miner_ids)}

        self.logger.info(
            f"Building correlation matrix: {n_miners} miners × {n_market_sides} market-sides"
        )

        if n_miners < 2:
            self.logger.info("Fewer than 2 miners, skipping correlation")
            return

        # Build submissions dict for correlation module
        submissions: Dict[str, np.ndarray] = {}
        for mid in miner_ids:
            arr = np.full(n_market_sides, np.nan)
            for ms_key, prob in miner_data[mid].items():
                arr[all_market_sides[ms_key]] = prob
            submissions[str(mid)] = arr

        market_ids_arr = np.arange(n_market_sides)
        min_common = int(sos_params.min_common_markets)

        # 2. Compute pairwise correlations (with sub-window rotation detection)
        if sos_params.enable_sub_window_correlation and market_side_first_seen:
            # Build half-mask: True = first half of window, False = second half
            window_mid = window_start + (window_end - window_start) / 2
            market_half_mask = np.zeros(n_market_sides, dtype=bool)
            for ms_key, idx in all_market_sides.items():
                fs = market_side_first_seen.get(ms_key)
                if fs is not None:
                    market_half_mask[idx] = fs < window_mid
                else:
                    market_half_mask[idx] = True  # Default to first half
            corr_matrix = compute_pairwise_correlations_windowed(
                submissions, market_ids_arr, market_half_mask,
                min_common=min_common,
            )
        else:
            corr_matrix = compute_pairwise_correlations(
                submissions, market_ids_arr, min_common=min_common,
            )

        # 3. Detect clusters (hard threshold, used for logging + hard penalty)
        clusters = detect_clusters(
            corr_matrix,
            threshold=float(sos_params.cluster_correlation_threshold),
        )

        # 3b. Compute low-overlap penalties (Fix 1)
        overlap_penalties = compute_low_overlap_penalty(
            submissions,
            min_common=min_common,
            low_overlap_threshold=float(sos_params.low_overlap_threshold),
            max_unscoreable=float(sos_params.low_overlap_max_unscoreable),
        )

        # 4. Fetch existing SOS_market scores
        existing_sos = await self.db.read(
            _SELECT_EXISTING_SOS_MARKET,
            params={"as_of": as_of, "window_days": window_days},
            mappings=True,
        )
        sos_market_map: Dict[int, float] = {}
        hotkey_map: Dict[int, str] = {}
        for row in existing_sos:
            sos_market_map[row["miner_id"]] = float(row["sos_score"]) if row["sos_score"] is not None else 0.5
            hotkey_map[row["miner_id"]] = row["miner_hotkey"]

        self.items_total = n_miners

        # 5. Compute composite SOS per miner and persist
        w_market = float(sos_params.w_market)
        w_crowd = float(sos_params.w_crowd)
        w_cluster = float(sos_params.w_cluster)
        max_corr_weight = float(sos_params.crowd_max_corr_weight)
        soft_floor = float(sos_params.cluster_soft_floor)
        soft_ceil = float(sos_params.cluster_soft_ceil)

        sorted_miner_ids_str = sorted(submissions.keys())

        for i, mid in enumerate(miner_ids):
            idx_in_corr = sorted_miner_ids_str.index(str(mid))

            # SOS_crowd with max-corr blending (Fix 4) and overlap penalty (Fix 1)
            sos_crowd_val = compute_sos_crowd(corr_matrix, idx_in_corr, max_corr_weight)
            overlap_pen = overlap_penalties.get(str(mid), 0.0)
            sos_crowd_val *= (1.0 - overlap_pen)

            # SOS_cluster: max of hard (connected component) and soft (continuous ramp) penalties (Fix 2)
            hard_penalty = compute_cluster_penalty(clusters, idx_in_corr)
            soft_penalty = compute_soft_cluster_penalty(corr_matrix, idx_in_corr, soft_floor, soft_ceil)
            sos_cluster_val = 1.0 - max(hard_penalty, soft_penalty)

            sos_market_val = sos_market_map.get(mid, 0.5)
            sos_composite = (
                w_market * sos_market_val
                + w_crowd * sos_crowd_val
                + w_cluster * sos_cluster_val
            )

            # Find cluster assignment
            cluster_id_val = None
            cluster_size_val = 1
            for ci, cluster in enumerate(clusters):
                if idx_in_corr in cluster:
                    cluster_id_val = ci
                    cluster_size_val = len(cluster)
                    break

            hotkey = hotkey_map.get(mid)
            if hotkey is None:
                continue  # Miner not in rolling score table yet

            await self.db.write(
                _UPDATE_COMPOSITE_UNIQUENESS,
                params={
                    "miner_id": mid,
                    "miner_hotkey": hotkey,
                    "as_of": as_of,
                    "window_days": window_days,
                    "sos_crowd": sos_crowd_val,
                    "sos_cluster": sos_cluster_val,
                    "sos_composite": sos_composite,
                    "uniqueness_dim": sos_composite,
                    "cluster_id": cluster_id_val,
                    "cluster_size": cluster_size_val,
                },
            )
            self.items_processed = i + 1

        # 6. Persist pairwise correlations (upper triangle only)
        for i in range(len(sorted_miner_ids_str)):
            for j in range(i + 1, len(sorted_miner_ids_str)):
                corr_val = corr_matrix[i, j]
                if abs(corr_val) < 0.01:
                    continue  # Skip near-zero correlations to save space
                mid_a = int(sorted_miner_ids_str[i])
                mid_b = int(sorted_miner_ids_str[j])
                # Count common markets
                mask_a = ~np.isnan(submissions[sorted_miner_ids_str[i]])
                mask_b = ~np.isnan(submissions[sorted_miner_ids_str[j]])
                n_common = int((mask_a & mask_b).sum())

                await self.db.write(
                    _INSERT_PAIRWISE_CORRELATION,
                    params={
                        "miner_a_id": mid_a,
                        "miner_b_id": mid_b,
                        "as_of": as_of,
                        "correlation": float(corr_val),
                        "n_common_markets": n_common,
                    },
                )

        self.logger.info(
            f"Composite uniqueness computed for {self.items_processed} miners, "
            f"{len(clusters)} clusters detected"
        )
