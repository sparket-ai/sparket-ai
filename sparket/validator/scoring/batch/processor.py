"""Vectorized batch processing for scoring.

Designed for:
1. Maximum vectorization using NumPy
2. Parallel worker compatibility with database-backed work queue
3. Chunked processing to handle memory constraints

Work is partitioned into chunks that can be claimed by multiple workers.
Each chunk is processed using vectorized NumPy operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from numpy.typing import NDArray
from sqlalchemy import text

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params


class WorkType(str, Enum):
    """Types of batch work."""
    SNAPSHOT = "snapshot"                       # Compute ground truth snapshot
    OUTCOME = "outcome"                         # Score settled outcomes
    ROLLING = "rolling"                         # Rolling aggregates
    CALIBRATION = "calibration"                 # Calibration + sharpness
    ORIGINALITY = "originality"                 # Originality + lead-lag
    COMPOSITE_UNIQUENESS = "composite_uniqueness"  # Pairwise correlation + clustering + composite SOS
    SHAPLEY = "shapley"                         # Monte Carlo Shapley contribution scoring
    SKILL = "skill"                             # Final skill score (Cobb-Douglas)


@dataclass
class WorkChunk:
    """A chunk of work that can be claimed by a worker."""
    chunk_id: str
    work_type: WorkType
    params: Dict[str, Any]
    created_at: datetime
    claimed_by: Optional[str] = None
    claimed_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class BatchSubmissionData:
    """Vectorized container for submission data.
    
    Stores all submission data in NumPy arrays for fast processing.
    Data is indexed by submission_id for efficient lookups.
    """
    
    def __init__(
        self,
        submission_ids: NDArray[np.int64],
        miner_ids: NDArray[np.int64],
        market_ids: NDArray[np.int64],
        submitted_ts: NDArray[np.float64],
        odds: NDArray[np.float64],
        probs: NDArray[np.float64],
        minutes_to_close: NDArray[np.int64],
        cle: NDArray[np.float64],
        clv_prob: NDArray[np.float64],
        pss_brier: NDArray[np.float64],
        pss_log: NDArray[np.float64],
        brier: NDArray[np.float64],
        miner_hotkeys: List[str],  # Keep as list for string handling
    ):
        self.submission_ids = submission_ids
        self.miner_ids = miner_ids
        self.market_ids = market_ids
        self.submitted_ts = submitted_ts
        self.odds = odds
        self.probs = probs
        self.minutes_to_close = minutes_to_close
        self.cle = cle
        self.clv_prob = clv_prob
        self.pss_brier = pss_brier
        self.pss_log = pss_log
        self.brier = brier
        self.miner_hotkeys = miner_hotkeys
        
        # Build index for fast miner lookups
        self._miner_index: Dict[Tuple[int, str], NDArray[np.bool_]] = {}
        self._build_miner_index()
    
    def _build_miner_index(self) -> None:
        """Build miner -> submission mask index."""
        unique_miners = set(zip(self.miner_ids.tolist(), self.miner_hotkeys))
        for miner_id, hotkey in unique_miners:
            mask = (self.miner_ids == miner_id)
            # Also check hotkey matches
            hotkey_mask = np.array([h == hotkey for h in self.miner_hotkeys])
            self._miner_index[(miner_id, hotkey)] = mask & hotkey_mask
    
    def get_miner_mask(self, miner_id: int, hotkey: str) -> NDArray[np.bool_]:
        """Get boolean mask for a specific miner's submissions."""
        return self._miner_index.get((miner_id, hotkey), np.zeros(len(self.submission_ids), dtype=bool))
    
    def get_unique_miners(self) -> List[Tuple[int, str]]:
        """Get list of unique (miner_id, hotkey) pairs."""
        return list(self._miner_index.keys())
    
    @property
    def n_submissions(self) -> int:
        return len(self.submission_ids)
    
    @property
    def n_miners(self) -> int:
        return len(self._miner_index)


class VectorizedAggregator:
    """Vectorized aggregation operations.
    
    All operations use NumPy for maximum performance.
    Designed to process all miners in a single pass where possible.
    """
    
    def __init__(self, params: ScoringParams | None = None):
        self.params = params or get_scoring_params()
    
    def compute_decay_weights(
        self,
        timestamps: NDArray[np.float64],
        ref_ts: float,
        half_life_days: float,
    ) -> NDArray[np.float64]:
        """Vectorized decay weight computation."""
        if half_life_days <= 0:
            return np.zeros_like(timestamps)
        
        age_seconds = ref_ts - timestamps
        age_days = np.maximum(0, age_seconds / 86400.0)
        ln_half = np.log(0.5)
        return np.exp(age_days * ln_half / half_life_days)
    
    def compute_time_factors(
        self,
        minutes_to_close: NDArray[np.int64],
    ) -> NDArray[np.float64]:
        """Vectorized time-to-close factor computation."""
        tp = self.params.time_weight
        min_min = tp.min_minutes
        max_min = tp.max_minutes
        floor = float(tp.floor_factor)
        
        minutes = np.asarray(minutes_to_close, dtype=np.float64)
        factors = np.full_like(minutes, floor, dtype=np.float64)
        
        # Max factor for early submissions
        factors = np.where(minutes >= max_min, 1.0, factors)
        
        # Log-scaled for middle range
        in_range = (minutes > min_min) & (minutes < max_min)
        if np.any(in_range):
            log_min = np.log(min_min)
            log_max = np.log(max_min)
            log_val = np.log(np.maximum(minutes[in_range], 1))
            normalized = (log_val - log_min) / (log_max - log_min)
            factors[in_range] = floor + normalized * (1.0 - floor)
        
        return factors
    
    def apply_asymmetric_time_bonus(
        self,
        scores: NDArray[np.float64],
        minutes_to_close: NDArray[np.int64],
    ) -> NDArray[np.float64]:
        """Apply asymmetric time bonus to scores."""
        tp = self.params.time_weight
        time_factors = self.compute_time_factors(minutes_to_close)
        penalty_clip = float(tp.early_penalty_clip)
        
        # Penalty factors for negative scores
        penalty_factors = penalty_clip + (1.0 - penalty_clip) * (1.0 - time_factors)
        
        return np.where(
            scores >= 0,
            scores * time_factors,
            scores * penalty_factors,
        )
    
    def grouped_weighted_mean(
        self,
        values: NDArray[np.float64],
        weights: NDArray[np.float64],
        group_ids: NDArray[np.int64],
    ) -> Tuple[NDArray[np.int64], NDArray[np.float64]]:
        """Compute weighted mean per group using vectorized operations.
        
        Returns:
            (unique_group_ids, weighted_means)
        """
        # Get unique groups
        unique_groups = np.unique(group_ids)
        n_groups = len(unique_groups)
        
        # Create group mapping
        group_to_idx = {g: i for i, g in enumerate(unique_groups)}
        group_indices = np.array([group_to_idx[g] for g in group_ids])
        
        # Accumulate weighted sums and weight sums per group
        weighted_sums = np.zeros(n_groups, dtype=np.float64)
        weight_sums = np.zeros(n_groups, dtype=np.float64)
        
        # Use np.add.at for unbuffered in-place addition
        np.add.at(weighted_sums, group_indices, values * weights)
        np.add.at(weight_sums, group_indices, weights)
        
        # Compute means (avoid division by zero)
        with np.errstate(divide='ignore', invalid='ignore'):
            means = np.where(weight_sums > 0, weighted_sums / weight_sums, 0.0)
        
        return unique_groups, means
    
    def grouped_weighted_std(
        self,
        values: NDArray[np.float64],
        weights: NDArray[np.float64],
        group_ids: NDArray[np.int64],
        means: NDArray[np.float64],
        group_id_to_mean_idx: Dict[int, int],
    ) -> NDArray[np.float64]:
        """Compute weighted std per group."""
        unique_groups = np.unique(group_ids)
        n_groups = len(unique_groups)
        
        group_to_idx = {g: i for i, g in enumerate(unique_groups)}
        group_indices = np.array([group_to_idx[g] for g in group_ids])
        
        # Get mean for each value's group
        value_means = means[np.array([group_id_to_mean_idx[g] for g in group_ids])]
        
        # Squared deviations
        sq_dev = (values - value_means) ** 2
        
        # Accumulate
        weighted_sq_sums = np.zeros(n_groups, dtype=np.float64)
        weight_sums = np.zeros(n_groups, dtype=np.float64)
        
        np.add.at(weighted_sq_sums, group_indices, sq_dev * weights)
        np.add.at(weight_sums, group_indices, weights)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            variances = np.where(weight_sums > 0, weighted_sq_sums / weight_sums, 0.0)
        
        return np.sqrt(variances)
    
    def effective_sample_size_per_group(
        self,
        weights: NDArray[np.float64],
        group_ids: NDArray[np.int64],
    ) -> Tuple[NDArray[np.int64], NDArray[np.float64]]:
        """Compute effective sample size per group."""
        unique_groups = np.unique(group_ids)
        n_groups = len(unique_groups)
        
        group_to_idx = {g: i for i, g in enumerate(unique_groups)}
        group_indices = np.array([group_to_idx[g] for g in group_ids])
        
        weight_sums = np.zeros(n_groups, dtype=np.float64)
        weight_sq_sums = np.zeros(n_groups, dtype=np.float64)
        
        np.add.at(weight_sums, group_indices, weights)
        np.add.at(weight_sq_sums, group_indices, weights ** 2)
        
        with np.errstate(divide='ignore', invalid='ignore'):
            n_eff = np.where(weight_sq_sums > 0, weight_sums ** 2 / weight_sq_sums, 0.0)
        
        return unique_groups, n_eff


class BatchRollingProcessor:
    """Process rolling aggregates for all miners in vectorized batches.
    
    Designed for parallel execution:
    - Each worker can claim a chunk of miners
    - All operations within a chunk are vectorized
    """
    
    def __init__(self, db: Any, logger: Any, params: ScoringParams | None = None):
        self.db = db
        self.logger = logger
        self.params = params or get_scoring_params()
        self.aggregator = VectorizedAggregator(self.params)
    
    async def fetch_batch_data(
        self,
        window_start: datetime,
        window_end: datetime,
        miner_ids: Optional[List[int]] = None,
    ) -> BatchSubmissionData:
        """Fetch all submission data for the window in a single query.
        
        Optionally filter by miner_ids for chunked parallel processing.
        """
        query = text("""
            SELECT
                ms.submission_id,
                ms.miner_id,
                ms.miner_hotkey,
                ms.market_id,
                EXTRACT(EPOCH FROM ms.submitted_at) as submitted_ts,
                ms.odds_eu,
                ms.imp_prob,
                COALESCE(svc.minutes_to_close, 0) as minutes_to_close,
                COALESCE(svc.cle, 0) as cle,
                COALESCE(svc.clv_prob, 0) as clv_prob,
                sos.pss_brier as pss_brier,
                sos.pss_log as pss_log,
                COALESCE(sos.brier, 0.5) as brier
            FROM miner_submission ms
            LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
            LEFT JOIN submission_outcome_score sos ON ms.submission_id = sos.submission_id
            WHERE ms.submitted_at >= :window_start
              AND ms.submitted_at < :window_end
              {miner_filter}
            ORDER BY ms.miner_id, ms.submission_id
        """.format(
            miner_filter="AND ms.miner_id = ANY(:miner_ids)" if miner_ids else ""
        ))
        
        params = {"window_start": window_start, "window_end": window_end}
        if miner_ids:
            params["miner_ids"] = miner_ids
        
        rows = await self.db.read(query, params=params, mappings=True)
        
        if not rows:
            return BatchSubmissionData(
                submission_ids=np.array([], dtype=np.int64),
                miner_ids=np.array([], dtype=np.int64),
                market_ids=np.array([], dtype=np.int64),
                submitted_ts=np.array([], dtype=np.float64),
                odds=np.array([], dtype=np.float64),
                probs=np.array([], dtype=np.float64),
                minutes_to_close=np.array([], dtype=np.int64),
                cle=np.array([], dtype=np.float64),
                clv_prob=np.array([], dtype=np.float64),
                pss_brier=np.array([], dtype=np.float64),
                pss_log=np.array([], dtype=np.float64),
                brier=np.array([], dtype=np.float64),
                miner_hotkeys=[],
            )
        
        # Convert to NumPy arrays in single pass
        n = len(rows)
        submission_ids = np.empty(n, dtype=np.int64)
        miner_ids_arr = np.empty(n, dtype=np.int64)
        market_ids = np.empty(n, dtype=np.int64)
        submitted_ts = np.empty(n, dtype=np.float64)
        odds = np.empty(n, dtype=np.float64)
        probs = np.empty(n, dtype=np.float64)
        minutes_to_close = np.empty(n, dtype=np.int64)
        cle = np.empty(n, dtype=np.float64)
        clv_prob = np.empty(n, dtype=np.float64)
        pss_brier = np.empty(n, dtype=np.float64)
        pss_log = np.empty(n, dtype=np.float64)
        brier = np.empty(n, dtype=np.float64)
        miner_hotkeys = []
        
        for i, row in enumerate(rows):
            submission_ids[i] = row["submission_id"]
            miner_ids_arr[i] = row["miner_id"]
            market_ids[i] = row["market_id"]
            submitted_ts[i] = row["submitted_ts"]
            odds[i] = float(row["odds_eu"])
            probs[i] = float(row["imp_prob"])
            minutes_to_close[i] = int(row["minutes_to_close"])
            cle[i] = float(row["cle"])
            clv_prob[i] = float(row["clv_prob"])
            pss_brier[i] = float(row["pss_brier"]) if row["pss_brier"] is not None else np.nan
            pss_log[i] = float(row["pss_log"]) if row["pss_log"] is not None else np.nan
            brier[i] = float(row["brier"])
            miner_hotkeys.append(row["miner_hotkey"])
        
        return BatchSubmissionData(
            submission_ids=submission_ids,
            miner_ids=miner_ids_arr,
            market_ids=market_ids,
            submitted_ts=submitted_ts,
            odds=odds,
            probs=probs,
            minutes_to_close=minutes_to_close,
            cle=cle,
            clv_prob=clv_prob,
            pss_brier=pss_brier,
            pss_log=pss_log,
            brier=brier,
            miner_hotkeys=miner_hotkeys,
        )
    
    def compute_all_miner_metrics(
        self,
        data: BatchSubmissionData,
        ref_ts: float,
    ) -> Dict[Tuple[int, str], Dict[str, float]]:
        """Compute metrics for all miners using vectorized operations.
        
        Returns dict mapping (miner_id, hotkey) -> metrics
        """
        if data.n_submissions == 0:
            return {}
        
        # Compute decay weights for all submissions at once
        half_life = self.params.decay.half_life_days
        decay_weights = self.aggregator.compute_decay_weights(
            data.submitted_ts, ref_ts, half_life
        )
        
        # Apply asymmetric time bonus to blended PSS (brier/log)
        brier_pss = data.pss_brier
        log_pss = data.pss_log
        both = np.isfinite(brier_pss) & np.isfinite(log_pss)
        brier_only = np.isfinite(brier_pss) & ~np.isfinite(log_pss)
        log_only = np.isfinite(log_pss) & ~np.isfinite(brier_pss)
        pss_blend = np.full_like(brier_pss, np.nan, dtype=np.float64)
        pss_blend[both] = (brier_pss[both] + log_pss[both]) / 2.0
        pss_blend[brier_only] = brier_pss[brier_only]
        pss_blend[log_only] = log_pss[log_only]

        pss_adjusted = self.aggregator.apply_asymmetric_time_bonus(
            pss_blend, data.minutes_to_close
        )
        
        # Create composite miner key for grouping
        # Use miner_id as group (hotkey handled separately)
        unique_miners = data.get_unique_miners()
        
        results = {}
        
        # Process per miner (unavoidable for hotkey handling, but inner ops are vectorized)
        for miner_id, hotkey in unique_miners:
            mask = data.get_miner_mask(miner_id, hotkey)
            
            if not np.any(mask):
                continue
            
            # Extract masked arrays
            m_weights = decay_weights[mask]
            m_cle = data.cle[mask]
            m_clv_prob = data.clv_prob[mask]
            m_pss = pss_adjusted[mask]
            m_brier = data.brier[mask]
            
            # Vectorized computations
            weight_sum = np.sum(m_weights)
            
            if weight_sum <= 0:
                continue
            
            # Effective sample size
            n_eff = np.sum(m_weights) ** 2 / np.sum(m_weights ** 2) if np.sum(m_weights ** 2) > 0 else 0
            
            # Weighted means
            es_mean = np.sum(m_cle * m_weights) / weight_sum
            clv_mean = np.sum(m_clv_prob * m_weights) / weight_sum
            pss_valid = np.isfinite(m_pss)
            if np.any(pss_valid):
                pss_weight_sum = np.sum(m_weights[pss_valid])
                pss_mean = np.sum(m_pss[pss_valid] * m_weights[pss_valid]) / pss_weight_sum
            else:
                pss_mean = 0.0
            brier_mean = np.sum(m_brier * m_weights) / weight_sum
            
            # Weighted std for ES
            es_var = np.sum(m_weights * (m_cle - es_mean) ** 2) / weight_sum
            es_std = np.sqrt(es_var)
            
            # Derived metrics
            es_adj = es_mean / es_std if es_std > 0.001 else 0.0
            mes_mean = 1.0 - np.mean(np.abs(m_clv_prob))  # Market efficiency
            fq_raw = 1.0 - 2.0 * brier_mean  # Transform Brier to FQ
            
            results[(miner_id, hotkey)] = {
                "n_submissions": int(np.sum(mask)),
                "n_eff": float(n_eff),
                "es_mean": float(es_mean),
                "es_std": float(es_std),
                "es_adj": float(es_adj),
                "mes_mean": float(mes_mean),
                "pss_mean": float(pss_mean),
                "brier_mean": float(brier_mean),
                "fq_raw": float(fq_raw),
            }
        
        return results


__all__ = [
    "WorkType",
    "WorkChunk",
    "BatchSubmissionData",
    "VectorizedAggregator",
    "BatchRollingProcessor",
]
