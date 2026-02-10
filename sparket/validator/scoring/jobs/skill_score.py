"""Final skill score computation job.

Combines all dimension scores into the final SkillScore using 4 dimensions:

1. ForecastDim (Accuracy): How accurate are predictions vs outcome?
   - FQ: 1 - 2*brier_mean (transforms Brier to [0,1] scale)
   - CAL: Calibration score

2. SkillDim (Relative Skill): How well does miner beat the market?
   - PSS: Time-adjusted PSS vs matched snapshot

3. EconDim (Economic Edge): Does miner beat the closing line?
   - EDGE: CLE-based edge score
   - MES: Market efficiency score

4. InfoDim (Information Value): Does miner have information advantage?
   - SOS: Source of signal (independence)
   - LEAD: Lead-lag ratio

SkillScore = w_forecast * ForecastDim + w_skill * SkillDim + w_econ * EconDim + w_info * InfoDim
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params

from ..aggregation.normalization import normalize_zscore_logistic, normalize_percentile
from ..determinism import get_canonical_window_bounds
from .base import ScoringJob

from sparket.validator.ledger.models import MinerMetrics, ScoringConfigSnapshot, ChainParamsSnapshot
from sparket.validator.ledger.compute_weights import compute_weights as _shared_compute_weights


# SQL queries
_SELECT_ROLLING_SCORES = text(
    """
    SELECT
        miner_id,
        miner_hotkey,
        fq_raw,
        brier_mean,
        pss_mean,
        cal_score,
        sharp_score,
        es_adj,
        mes_mean,
        sos_score,
        lead_score
    FROM miner_rolling_score
    WHERE as_of = :as_of
      AND window_days = :window_days
    ORDER BY miner_id, miner_hotkey
    """
)

_UPDATE_SKILL_SCORE = text(
    """
    UPDATE miner_rolling_score
    SET fq_score = :fq_score,
        edge_score = :edge_score,
        mes_score = :mes_score,
        sos_score = :sos_score_norm,
        lead_score = :lead_score_norm,
        forecast_dim = :forecast_dim,
        econ_dim = :econ_dim,
        info_dim = :info_dim,
        skill_dim = :skill_dim,
        skill_score = :skill_score,
        score_version = score_version + 1
    WHERE miner_id = :miner_id
      AND miner_hotkey = :miner_hotkey
      AND as_of = :as_of
      AND window_days = :window_days
    """
)


MinerKey = str  # "miner_id:miner_hotkey"


def make_miner_key(miner_id: int, miner_hotkey: str) -> MinerKey:
    """Create a miner lookup key."""
    return f"{miner_id}:{miner_hotkey}"


class SkillScoreJob(ScoringJob):
    """Compute final skill score for all miners.

    Normalizes metrics across all miners and computes 4-dimension composite.
    """

    JOB_ID = "skill_score_v2"

    def __init__(
        self,
        db: Any,
        logger: Any,
        *,
        as_of: datetime | None = None,
        job_id_override: str | None = None,
    ):
        """Initialize the job."""
        super().__init__(db, logger, job_id_override=job_id_override)
        self.params = get_scoring_params()
        self.as_of = as_of

    async def execute(self) -> None:
        """Execute the skill score computation."""
        window_days = self.params.windows.rolling_window_days
        if self.as_of is not None:
            as_of = self.as_of
        else:
            _, as_of = get_canonical_window_bounds(window_days)

        self.logger.info(f"Computing skill scores as of {as_of}")

        # Fetch all rolling scores
        rows = await self.db.read(
            _SELECT_ROLLING_SCORES,
            params={"as_of": as_of, "window_days": window_days},
            mappings=True,
        )

        self.items_total = len(rows)
        self.logger.info(f"Found {self.items_total} miners with rolling scores")

        if not rows:
            return

        # Build MinerMetrics from DB rows (same conversion auditors do from checkpoints)
        keys: List[MinerKey] = []
        miner_info: Dict[MinerKey, Dict[str, Any]] = {}
        metrics_list: List[MinerMetrics] = []

        for row in rows:
            key = make_miner_key(row["miner_id"], row["miner_hotkey"])
            keys.append(key)
            miner_info[key] = {
                "miner_id": row["miner_id"],
                "miner_hotkey": row["miner_hotkey"],
            }
            metrics_list.append(MinerMetrics(
                uid=0,  # UID not needed for normalization, only for weight indexing
                hotkey=row["miner_hotkey"],
                fq_raw=self._to_float_safe(row["fq_raw"], 0.0),
                pss_mean=self._to_float_safe(row["pss_mean"], 0.0),
                es_adj=self._to_float_safe(row["es_adj"], 0.0),
                mes_mean=self._to_float_safe(row["mes_mean"], 0.5),
                cal_score=self._to_float_safe(row["cal_score"], 0.5),
                sharp_score=self._to_float_safe(row.get("sharp_score"), 0.5),
                sos_score=self._to_float_safe(row["sos_score"], 0.5),
                lead_score=self._to_float_safe(row["lead_score"], 0.5),
                brier_mean=self._to_float_safe(row.get("brier_mean"), 0.0),
            ))

        # Use shared normalization logic (same code path as auditors)
        scoring_config = ScoringConfigSnapshot(params=self.params.model_dump(mode="json"))
        # We only need normalization + dimension scores here, not full weight computation.
        # Use the shared normalization arrays directly:
        sorted_metrics = sorted(metrics_list, key=lambda m: m.hotkey)

        fq_raw = np.array([m.fq_raw for m in sorted_metrics], dtype=np.float64)
        pss = np.array([m.pss_mean for m in sorted_metrics], dtype=np.float64)
        cal = np.array([m.cal_score for m in sorted_metrics], dtype=np.float64)
        es_adj = np.array([m.es_adj for m in sorted_metrics], dtype=np.float64)
        mes = np.array([m.mes_mean for m in sorted_metrics], dtype=np.float64)
        sos = np.array([m.sos_score for m in sorted_metrics], dtype=np.float64)
        lead = np.array([m.lead_score for m in sorted_metrics], dtype=np.float64)

        n_miners = len(sorted_metrics)
        min_count = int(self.params.normalization.min_count_for_zscore)

        # Normalization (shared logic with compute_weights)
        fq_norm = np.clip((fq_raw + 1) / 2, 0, 1)
        use_zscore = n_miners >= min_count
        if use_zscore:
            pss_norm = normalize_zscore_logistic(pss)
            es_norm = normalize_zscore_logistic(es_adj)
        else:
            pss_norm = normalize_percentile(pss)
            es_norm = normalize_percentile(es_adj)
        cal_norm = np.clip(cal, 0, 1)
        mes_norm = np.clip(mes, 0, 1)
        sos_norm = np.clip(sos, 0, 1)
        lead_norm = np.clip(lead, 0, 1)

        # Dimension combining
        dim_weights = self.params.dimension_weights
        skill_weights = self.params.skill_score_weights

        forecast_dim = float(dim_weights.w_fq) * fq_norm + float(dim_weights.w_cal) * cal_norm
        skill_dim = pss_norm
        econ_dim = float(dim_weights.w_edge) * es_norm + float(dim_weights.w_mes) * mes_norm
        info_dim = float(dim_weights.w_sos) * sos_norm + float(dim_weights.w_lead) * lead_norm

        skill_score = (
            float(skill_weights.w_outcome_accuracy) * forecast_dim
            + float(skill_weights.w_outcome_relative) * skill_dim
            + float(skill_weights.w_odds_edge) * econ_dim
            + float(skill_weights.w_info_adv) * info_dim
        )

        # Persist results - map back from sorted order to original key order
        hotkey_to_idx = {m.hotkey: i for i, m in enumerate(sorted_metrics)}
        for key_idx, key in enumerate(keys):
            info = miner_info[key]
            i = hotkey_to_idx[info["miner_hotkey"]]
            await self.db.write(
                _UPDATE_SKILL_SCORE,
                params={
                    "miner_id": info["miner_id"],
                    "miner_hotkey": info["miner_hotkey"],
                    "as_of": as_of,
                    "window_days": window_days,
                    "fq_score": float(fq_norm[i]),
                    "edge_score": float(es_norm[i]),
                    "mes_score": float(mes_norm[i]),
                    "sos_score_norm": float(sos_norm[i]),
                    "lead_score_norm": float(lead_norm[i]),
                    "forecast_dim": float(forecast_dim[i]),
                    "econ_dim": float(econ_dim[i]),
                    "info_dim": float(info_dim[i]),
                    "skill_dim": float(skill_dim[i]),
                    "skill_score": float(skill_score[i]),
                },
            )
            self.items_processed += 1

    def _to_float_safe(self, val: Any, default: float) -> float:
        """Safely convert to float, returning default on failure."""
        if val is None:
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default


__all__ = ["SkillScoreJob"]
