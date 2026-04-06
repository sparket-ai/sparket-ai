"""Rolling aggregates job.

Computes time-decayed rolling statistics for all miners:
- FQ (Forecast Quality) from Brier score vs outcome
- ES (Economic Edge) from CLE vs closing line
- MES (Market Efficiency Score)
- PSS with asymmetric time bonus

Key design:
- Decay weights: Used for aggregation (recent submissions count more)
- Time bonus: Applied to PSS scores (early accurate = more credit)
- CLE/CLV: Raw values preserved (no time adjustment to economic metrics)
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List

import numpy as np
from numpy.typing import NDArray
from sqlalchemy import text

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params

from ..aggregation.decay import (
    compute_decay_weights,
    effective_sample_size,
    weighted_mean,
    weighted_std,
)
from ..aggregation.time_weight import apply_time_bonus_batch
from ..aggregation.shrinkage import shrink_toward_mean, compute_population_mean
from ..determinism import get_canonical_window_bounds
from .base import ScoringJob


# SQL queries
_SELECT_ACTIVE_MINERS = text(
    """
    SELECT DISTINCT miner_id, miner_hotkey
    FROM miner_submission
    WHERE submitted_at >= :window_start
      AND submitted_at < :window_end
    ORDER BY miner_id, miner_hotkey
    """
)

_SELECT_MINER_SUBMISSIONS = text(
    """
    SELECT
        ms.submission_id,
        ms.submitted_at,
        svc.cle,
        svc.clv_prob,
        svc.minutes_to_close,
        sos.pss_brier,
        sos.pss_log,
        sos.brier,
        ms.odds_eu AS miner_odds,
        svc.close_odds_eu AS close_odds
    FROM miner_submission ms
    LEFT JOIN submission_vs_close svc ON ms.submission_id = svc.submission_id
    LEFT JOIN submission_outcome_score sos ON ms.submission_id = sos.submission_id
    WHERE ms.miner_id = :miner_id
      AND ms.miner_hotkey = :miner_hotkey
      AND ms.submitted_at >= :window_start
      AND ms.submitted_at < :window_end
    ORDER BY ms.submission_id
    """
)

_UPSERT_ROLLING_SCORE = text(
    """
    INSERT INTO miner_rolling_score (
        miner_id, miner_hotkey, as_of, window_days,
        n_submissions, n_eff, es_mean, es_std, es_adj, mes_mean,
        pss_mean, fq_raw, brier_mean, score_version,
        brier_ws, brier_wt, fq_ws, fq_wt, pss_ws, pss_wt,
        es_ws, es_wt, mes_ws, mes_wt, sos_ws, sos_wt, lead_ws, lead_wt
    ) VALUES (
        :miner_id, :miner_hotkey, :as_of, :window_days,
        :n_submissions, :n_eff, :es_mean, :es_std, :es_adj, :mes_mean,
        :pss_mean, :fq_raw, :brier_mean, :score_version,
        :brier_ws, :brier_wt, :fq_ws, :fq_wt, :pss_ws, :pss_wt,
        :es_ws, :es_wt, :mes_ws, :mes_wt, :sos_ws, :sos_wt, :lead_ws, :lead_wt
    )
    ON CONFLICT (miner_id, miner_hotkey, as_of, window_days) DO UPDATE SET
        n_submissions = EXCLUDED.n_submissions,
        n_eff = EXCLUDED.n_eff,
        es_mean = EXCLUDED.es_mean,
        es_std = EXCLUDED.es_std,
        es_adj = EXCLUDED.es_adj,
        mes_mean = EXCLUDED.mes_mean,
        pss_mean = EXCLUDED.pss_mean,
        fq_raw = EXCLUDED.fq_raw,
        brier_mean = EXCLUDED.brier_mean,
        score_version = EXCLUDED.score_version,
        brier_ws = EXCLUDED.brier_ws,
        brier_wt = EXCLUDED.brier_wt,
        fq_ws = EXCLUDED.fq_ws,
        fq_wt = EXCLUDED.fq_wt,
        pss_ws = EXCLUDED.pss_ws,
        pss_wt = EXCLUDED.pss_wt,
        es_ws = EXCLUDED.es_ws,
        es_wt = EXCLUDED.es_wt,
        mes_ws = EXCLUDED.mes_ws,
        mes_wt = EXCLUDED.mes_wt,
        sos_ws = EXCLUDED.sos_ws,
        sos_wt = EXCLUDED.sos_wt,
        lead_ws = EXCLUDED.lead_ws,
        lead_wt = EXCLUDED.lead_wt
    """
)


class RollingAggregatesJob(ScoringJob):
    """Compute time-decayed rolling statistics for all miners.

    Metrics computed:
    - brier_mean: Weighted mean of Brier score vs outcome (raw accuracy)
    - fq_raw: brier_mean transformed to FQ (lower brier = higher FQ)
    - pss_mean: Weighted mean of time-adjusted PSS (skill vs market)
    - ES_mean: Weighted mean of CLE (economic edge)
    - ES_std: Weighted std of CLE
    - ES_adj: ES_mean / ES_std (Sharpe-like)
    - MES_mean: Weighted mean of (1 - |CLV_prob|)

    Weighting:
    - Decay weights: Recent submissions count more in aggregation
    - Time bonus: Applied to PSS scores before aggregation (asymmetric)
    - CLE/CLV: No time adjustment (preserve economic accuracy)
    """

    JOB_ID = "rolling_aggregates_v2"
    CHECKPOINT_INTERVAL = 50

    def __init__(
        self,
        db: Any,
        logger: Any,
        *,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
        job_id_override: str | None = None,
    ):
        """Initialize the job."""
        super().__init__(db, logger, job_id_override=job_id_override)
        self.params = get_scoring_params()
        self.window_start = window_start
        self.window_end = window_end

    async def execute(self) -> None:
        """Execute the rolling aggregates job."""
        window_days = self.params.windows.rolling_window_days
        if self.window_start is not None and self.window_end is not None:
            window_start, window_end = self.window_start, self.window_end
        else:
            window_start, window_end = get_canonical_window_bounds(
                window_days,
                reference_time=self.window_end,
            )

        self.logger.info(f"Computing rolling aggregates for {window_days} day window")

        # Get all active miners
        miners = await self.db.read(
            _SELECT_ACTIVE_MINERS,
            params={"window_start": window_start, "window_end": window_end},
            mappings=True,
        )

        self.items_total = len(miners)
        self.logger.info(f"Found {self.items_total} active miners")

        if not miners:
            return

        # Compute raw metrics for each miner
        miner_metrics: Dict[str, Dict[str, Any]] = {}

        start_idx = self.state.get("last_miner_idx", 0)

        for idx, miner in enumerate(miners[start_idx:], start=start_idx):
            miner_key = f"{miner['miner_id']}:{miner['miner_hotkey']}"

            metrics = await self._compute_miner_metrics(
                miner_id=miner["miner_id"],
                miner_hotkey=miner["miner_hotkey"],
                window_start=window_start,
                window_end=window_end,
            )

            if metrics:
                miner_metrics[miner_key] = {
                    "miner_id": miner["miner_id"],
                    "miner_hotkey": miner["miner_hotkey"],
                    **metrics,
                }

            self.items_processed = idx + 1
            self.state["last_miner_idx"] = idx + 1
            await self.checkpoint_if_due()

        if not miner_metrics:
            self.logger.info("No metrics to aggregate")
            return

        # Apply shrinkage
        shrunk_metrics = self._apply_shrinkage(miner_metrics)

        # Persist results
        await self._persist_results(
            shrunk_metrics,
            window_end,
            window_days,
        )

    async def _compute_miner_metrics(
        self,
        miner_id: int,
        miner_hotkey: str,
        window_start: datetime,
        window_end: datetime,
    ) -> Dict[str, Any] | None:
        """Compute raw metrics for a single miner.

        Weighting strategy:
        - Decay weights: Used for all aggregations
        - Time bonus: Applied to PSS scores only (asymmetric)
        - CLE/CLV: Raw values, no time adjustment
        """
        submissions = await self.db.read(
            _SELECT_MINER_SUBMISSIONS,
            params={
                "miner_id": miner_id,
                "miner_hotkey": miner_hotkey,
                "window_start": window_start,
                "window_end": window_end,
            },
            mappings=True,
        )

        if not submissions:
            return None

        # Extract timestamps for decay weights
        timestamps = np.array([s["submitted_at"].timestamp() for s in submissions])
        ref_ts = window_end.timestamp()
        half_life = self.params.decay.half_life_days
        decay_weights = compute_decay_weights(timestamps, ref_ts, half_life)

        # Odds-ratio threshold: exclude extreme CLE values from ES aggregation.
        # Submissions with miner_odds / close_odds > threshold are noise,
        # not genuine edge claims.
        max_odds_ratio = float(self.params.ground_truth.max_odds_ratio_for_cle)

        # Extract metrics (handle None values)
        cle_values = []
        cle_weights = []
        clv_prob_values = []
        clv_prob_weights = []
        pss_values = []
        pss_minutes = []
        pss_weights = []
        brier_values = []
        brier_weights = []

        for i, s in enumerate(submissions):
            w = decay_weights[i]

            # Odds-ratio gate: exclude extreme divergence from economic edge
            # metrics (CLE and CLV). Submissions where miner odds are within
            # max_odds_ratio of closing odds are eligible.
            odds_eligible = True
            if s.get("miner_odds") and s.get("close_odds"):
                miner_odds = float(s["miner_odds"])
                close_odds = float(s["close_odds"])
                if close_odds > 0:
                    ratio = miner_odds / close_odds
                    odds_eligible = (1.0 / max_odds_ratio) <= ratio <= max_odds_ratio
                else:
                    odds_eligible = False
            elif s["cle"] is not None or s.get("clv_prob") is not None:
                # Have CLE/CLV but no odds data to check — exclude to be safe
                odds_eligible = False

            # CLE - raw, no time adjustment
            if odds_eligible and s["cle"] is not None:
                cle_values.append(float(s["cle"]))
                cle_weights.append(w)

            # CLV uses the same gate — it measures the same divergence
            if odds_eligible and s["clv_prob"] is not None:
                clv_prob_values.append(float(s["clv_prob"]))
                clv_prob_weights.append(w)

            # PSS - blend brier/log if available, then apply time bonus
            pss_brier = s.get("pss_brier")
            pss_log = s.get("pss_log")
            pss_value = None
            if pss_brier is not None and pss_log is not None:
                pss_value = (float(pss_brier) + float(pss_log)) / 2.0
            elif pss_brier is not None:
                pss_value = float(pss_brier)
            elif pss_log is not None:
                pss_value = float(pss_log)

            if pss_value is not None:
                pss_values.append(pss_value)
                pss_minutes.append(
                    s["minutes_to_close"] if s.get("minutes_to_close") is not None else 0
                )
                pss_weights.append(w)

            # Brier - raw accuracy vs outcome
            if s.get("brier") is not None:
                brier_values.append(float(s["brier"]))
                brier_weights.append(w)

        # Compute effective sample size
        n_eff = effective_sample_size(decay_weights)

        # Compute ES metrics (CLE-based, no time adjustment)
        es_mean = 0.0
        es_std = 0.0
        es_adj = 0.0

        if cle_values:
            cle_arr = np.array(cle_values)
            cle_w_arr = np.array(cle_weights)
            es_mean = weighted_mean(cle_arr, cle_w_arr)
            es_std = weighted_std(cle_arr, cle_w_arr, es_mean)
            if es_std > 0.001:
                es_adj = es_mean / es_std

        # Compute MES (CLV-based, no time adjustment)
        mes_mean = 0.0
        if clv_prob_values:
            mes_arr = 1.0 - np.abs(np.array(clv_prob_values))
            clv_w_arr = np.array(clv_prob_weights)
            mes_mean = weighted_mean(mes_arr, clv_w_arr)

        # Compute PSS with asymmetric time bonus
        pss_mean = 0.0
        if pss_values:
            pss_arr = np.array(pss_values)
            pss_min_arr = np.array(pss_minutes, dtype=np.int64)
            pss_w_arr = np.array(pss_weights)

            # Apply asymmetric time bonus
            time_params = self.params.time_weight
            if time_params.enabled:
                pss_adjusted = apply_time_bonus_batch(
                    pss_arr,
                    pss_min_arr,
                    min_minutes=time_params.min_minutes,
                    max_minutes=time_params.max_minutes,
                    floor_factor=float(time_params.floor_factor),
                    early_penalty_clip=float(time_params.early_penalty_clip),
                )
            else:
                pss_adjusted = pss_arr

            pss_mean = weighted_mean(pss_adjusted, pss_w_arr)

        # Compute Brier mean (raw accuracy)
        brier_mean = 0.5  # Default (random)
        if brier_values:
            brier_arr = np.array(brier_values)
            brier_w_arr = np.array(brier_weights)
            brier_mean = weighted_mean(brier_arr, brier_w_arr)

        # FQ = 1 - 2*brier (transforms 0=perfect, 0.5=random to 1=perfect, 0=random)
        fq_raw = 1.0 - 2.0 * brier_mean

        # Compute accumulator pairs for ledger export
        brier_ws = float(np.sum(np.array(brier_values) * np.array(brier_weights))) if brier_values else 0.0
        brier_wt = float(np.sum(brier_weights)) if brier_weights else 0.0
        fq_ws = float(np.sum((1.0 - 2.0 * np.array(brier_values)) * np.array(brier_weights))) if brier_values else 0.0
        fq_wt = brier_wt
        pss_ws = float(np.sum(np.array(pss_values) * np.array(pss_weights))) if pss_values else 0.0
        pss_wt = float(np.sum(pss_weights)) if pss_weights else 0.0
        es_ws = float(np.sum(np.array(cle_values) * np.array(cle_weights))) if cle_values else 0.0
        es_wt = float(np.sum(cle_weights)) if cle_weights else 0.0
        mes_ws = float(np.sum(mes_arr * clv_w_arr)) if clv_prob_values else 0.0
        mes_wt = float(np.sum(clv_prob_weights)) if clv_prob_values else 0.0

        return {
            "n_submissions": len(submissions),
            "n_eff": n_eff,
            "es_mean": es_mean,
            "es_std": es_std,
            "es_adj": es_adj,
            "mes_mean": mes_mean,
            "pss_mean": pss_mean,
            "brier_mean": brier_mean,
            "fq_raw": fq_raw,
            # Accumulator pairs for ledger checkpoint export
            "brier_ws": brier_ws,
            "brier_wt": brier_wt,
            "fq_ws": fq_ws,
            "fq_wt": fq_wt,
            "pss_ws": pss_ws,
            "pss_wt": pss_wt,
            "es_ws": es_ws,
            "es_wt": es_wt,
            "mes_ws": mes_ws,
            "mes_wt": mes_wt,
        }

    def _apply_shrinkage(
        self,
        miner_metrics: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Apply shrinkage to metrics that benefit from it."""
        k = float(self.params.shrinkage.k)

        keys = list(miner_metrics.keys())
        n = len(keys)

        if n == 0:
            return miner_metrics

        # Extract arrays
        es_arr = np.array([miner_metrics[key]["es_mean"] for key in keys])
        pss_arr = np.array([miner_metrics[key]["pss_mean"] for key in keys])
        mes_arr = np.array([miner_metrics[key]["mes_mean"] for key in keys])
        fq_arr = np.array([miner_metrics[key]["fq_raw"] for key in keys])
        n_eff_arr = np.array([miner_metrics[key]["n_eff"] for key in keys])

        # Apply shrinkage
        es_shrunk = shrink_toward_mean(es_arr, n_eff_arr, k)
        pss_shrunk = shrink_toward_mean(pss_arr, n_eff_arr, k)
        mes_shrunk = shrink_toward_mean(mes_arr, n_eff_arr, k)
        fq_shrunk = shrink_toward_mean(fq_arr, n_eff_arr, k)

        # Write back
        for i, key in enumerate(keys):
            miner_metrics[key]["es_mean"] = float(es_shrunk[i])
            miner_metrics[key]["pss_mean"] = float(pss_shrunk[i])
            miner_metrics[key]["mes_mean"] = float(mes_shrunk[i])
            miner_metrics[key]["fq_raw"] = float(fq_shrunk[i])

        return miner_metrics

    async def _persist_results(
        self,
        miner_metrics: Dict[str, Dict[str, Any]],
        as_of: datetime,
        window_days: int,
    ) -> None:
        """Persist rolling scores to database (including accumulator pairs for ledger)."""
        for metrics in miner_metrics.values():
            await self.db.write(
                _UPSERT_ROLLING_SCORE,
                params={
                    "miner_id": metrics["miner_id"],
                    "miner_hotkey": metrics["miner_hotkey"],
                    "as_of": as_of,
                    "window_days": window_days,
                    "n_submissions": metrics["n_submissions"],
                    "n_eff": float(metrics["n_eff"]),
                    "es_mean": float(metrics["es_mean"]),
                    "es_std": float(metrics["es_std"]),
                    "es_adj": float(metrics["es_adj"]),
                    "mes_mean": float(metrics["mes_mean"]),
                    "pss_mean": float(metrics["pss_mean"]),
                    "fq_raw": float(metrics["fq_raw"]),
                    "brier_mean": float(metrics["brier_mean"]),
                    "score_version": 2,
                    # Accumulator pairs for ledger checkpoint export
                    "brier_ws": metrics.get("brier_ws", 0.0),
                    "brier_wt": metrics.get("brier_wt", 0.0),
                    "fq_ws": metrics.get("fq_ws", 0.0),
                    "fq_wt": metrics.get("fq_wt", 0.0),
                    "pss_ws": metrics.get("pss_ws", 0.0),
                    "pss_wt": metrics.get("pss_wt", 0.0),
                    "es_ws": metrics.get("es_ws", 0.0),
                    "es_wt": metrics.get("es_wt", 0.0),
                    "mes_ws": metrics.get("mes_ws", 0.0),
                    "mes_wt": metrics.get("mes_wt", 0.0),
                    # SOS and lead are computed by OriginalityLeadLagJob, not here
                    "sos_ws": None,
                    "sos_wt": None,
                    "lead_ws": None,
                    "lead_wt": None,
                },
            )


__all__ = ["RollingAggregatesJob"]
