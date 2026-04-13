"""Deterministic weight computation shared by primary and auditor.

This is the CRITICAL SHARED CODE PATH. Both the primary validator's
SetWeightsHandler and auditor's WeightVerification plugin call this
function to ensure identical weight outputs from identical inputs.

Extracted from:
- SkillScoreJob.execute() normalization logic
- SetWeightsHandler._emit_weights() weight processing
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
from numpy.typing import NDArray

from sparket.validator.scoring.aggregation.normalization import (
    normalize_percentile,
    normalize_zscore_logistic,
)

from .models import ChainParamsSnapshot, MinerMetrics, ScoringConfigSnapshot


@dataclass
class WeightResult:
    """Output of compute_weights with full audit trail."""

    # Final outputs
    uids: list[int] = field(default_factory=list)
    uint16_weights: list[int] = field(default_factory=list)

    # Intermediate values for audit trail
    skill_scores: dict[int, float] = field(default_factory=dict)  # uid -> skill_score
    raw_weights: dict[int, float] = field(default_factory=dict)  # uid -> normalized weight
    dimension_scores: dict[int, dict[str, float]] = field(default_factory=dict)  # uid -> {dim: score}


U16_MAX = 65535


def _normalize_max_weight(x: np.ndarray, limit: float = 0.1) -> np.ndarray:
    """Normalizes array so sum=1 and max value <= limit.

    Extracted from weights_utils.py for standalone use.
    """
    epsilon = 1e-7
    weights = x.copy()
    values = np.sort(weights)

    if x.sum() == 0 or len(x) * limit <= 1:
        return np.ones_like(x) / x.size

    estimation = values / values.sum()
    if estimation.max() <= limit:
        return weights / weights.sum()

    cumsum = np.cumsum(estimation, 0)
    estimation_sum = np.array(
        [(len(values) - i - 1) * estimation[i] for i in range(len(values))]
    )
    n_values = (
        estimation / (estimation_sum + cumsum + epsilon) < limit
    ).sum()

    cutoff_scale = (limit * cumsum[n_values - 1] - epsilon) / (
        1 - (limit * (len(estimation) - n_values))
    )
    cutoff = cutoff_scale * values.sum()
    weights[weights > cutoff] = cutoff

    return weights / weights.sum()


def _convert_to_uint16(
    uids: np.ndarray, weights: np.ndarray,
) -> tuple[list[int], list[int]]:
    """Convert float weights to uint16 representation.

    Extracted from weights_utils.convert_weights_and_uids_for_emit().
    """
    non_zero_mask = weights > 0
    nz_weights = weights[non_zero_mask]
    nz_uids = uids[non_zero_mask]

    if len(nz_weights) == 0:
        return [], []

    max_weight = float(np.max(nz_weights))
    if max_weight == 0:
        return [], []

    scaled = [float(w) / max_weight for w in nz_weights]

    uid_list = []
    weight_list = []
    for uid_i, w_i in zip(nz_uids, scaled):
        uint16_val = round(float(w_i) * U16_MAX)
        if uint16_val != 0:
            uid_list.append(int(uid_i))
            weight_list.append(uint16_val)

    return uid_list, weight_list


def compute_weights(
    miner_metrics: list[MinerMetrics],
    scoring_config: ScoringConfigSnapshot,
    chain_params: ChainParamsSnapshot,
) -> WeightResult:
    """Deterministic weight computation from derived rolling means.

    Steps:
    1. Normalize metrics across miners (zscore_logistic or percentile)
    2. Combine into 4 dimensions using config weights
    3. Compute final skill_score per miner
    4. L1 normalize
    5. Apply burn rate
    6. Apply max_weight_limit + min_allowed_weights
    7. Convert to uint16

    This function MUST produce identical output on primary and auditor
    given identical inputs. No randomness, no external state.

    Args:
        miner_metrics: Per-miner derived rolling means.
        scoring_config: Scoring hyperparameters.
        chain_params: Chain parameters (burn, weight limits).

    Returns:
        WeightResult with uids, uint16_weights, and intermediate audit trail.
    """
    result = WeightResult()
    n_neurons = chain_params.n_neurons

    if not miner_metrics:
        # No miners - allocate to burn if available
        if chain_params.burn_uid is not None:
            result.uids = [chain_params.burn_uid]
            result.uint16_weights = [U16_MAX]
        return result

    params = scoring_config.params

    # Extract config values
    dim_weights = params.get("dimension_weights", {})
    skill_weights = params.get("skill_score_weights", {})
    norm_params = params.get("normalization", {})
    weight_emission = params.get("weight_emission", {})

    w_fq = float(dim_weights.get("w_fq", 0.6))
    w_cal = float(dim_weights.get("w_cal", 0.4))
    w_edge = float(dim_weights.get("w_edge", 0.7))
    w_mes = float(dim_weights.get("w_mes", 0.3))
    w_sos = float(dim_weights.get("w_sos", 0.6))
    w_lead = float(dim_weights.get("w_lead", 0.4))

    w_outcome_accuracy = float(skill_weights.get("w_outcome_accuracy", 0.10))
    w_outcome_relative = float(skill_weights.get("w_outcome_relative", 0.10))
    w_odds_edge = float(skill_weights.get("w_odds_edge", 0.50))
    w_info_adv = float(skill_weights.get("w_info_adv", 0.30))

    min_count = int(norm_params.get("min_count_for_zscore", 10))
    burn_rate = float(weight_emission.get("burn_rate", 0.9))

    # Build arrays (deterministic order: sorted by uid)
    sorted_metrics = sorted(miner_metrics, key=lambda m: m.uid)
    uids = [m.uid for m in sorted_metrics]

    fq_raw = np.array([m.fq_raw for m in sorted_metrics], dtype=np.float64)
    pss = np.array([m.pss_mean for m in sorted_metrics], dtype=np.float64)
    cal = np.array([m.cal_score for m in sorted_metrics], dtype=np.float64)
    es_adj = np.array([m.es_adj for m in sorted_metrics], dtype=np.float64)
    mes = np.array([m.mes_mean for m in sorted_metrics], dtype=np.float64)
    sos = np.array([m.sos_score for m in sorted_metrics], dtype=np.float64)
    lead = np.array([m.lead_score for m in sorted_metrics], dtype=np.float64)

    n_miners = len(sorted_metrics)

    # Step 1: Normalize
    # FQ: [-1, 1] -> [0, 1]
    fq_norm = np.clip((fq_raw + 1) / 2, 0, 1)

    # PSS and ES: z-score logistic or percentile based on miner count
    use_zscore = n_miners >= min_count
    if use_zscore:
        pss_norm = normalize_zscore_logistic(pss)
        es_norm = normalize_zscore_logistic(es_adj)
    else:
        pss_norm = normalize_percentile(pss)
        es_norm = normalize_percentile(es_adj)

    # Others clip to [0, 1]
    cal_norm = np.clip(cal, 0, 1)
    mes_norm = np.clip(mes, 0, 1)
    sos_norm = np.clip(sos, 0, 1)
    lead_norm = np.clip(lead, 0, 1)

    # Step 2: Combine into dimensions (within-dimension additive, across-dimension multiplicative)
    forecast_dim = w_fq * fq_norm + w_cal * cal_norm      # → Accuracy pillar
    skill_dim = pss_norm                                    # feeds into Accuracy/Edge
    econ_dim = w_edge * es_norm + w_mes * mes_norm          # → Edge pillar
    info_dim = w_sos * sos_norm + w_lead * lead_norm        # → Timeliness pillar

    # Map to Cobb-Douglas 5-pillar names
    accuracy_dim = forecast_dim
    edge_dim_arr = econ_dim
    timeliness_dim = 0.5 * skill_dim + 0.5 * lead_norm

    # Uniqueness and Marginal from new Cobb-Douglas dimensions
    uniqueness_arr = np.array(
        [m.uniqueness_dim if m.uniqueness_dim is not None else float(sos[i])
         for i, m in enumerate(sorted_metrics)],
        dtype=np.float64,
    )
    marginal_arr = np.array(
        [m.marginal_dim if m.marginal_dim is not None else 0.5
         for i, m in enumerate(sorted_metrics)],
        dtype=np.float64,
    )

    # Step 3: Cobb-Douglas scoring
    cd_params = params.get("cobb_douglas", {})
    accuracy_exp = float(cd_params.get("accuracy_exponent", 0.5))
    edge_exp = float(cd_params.get("edge_exponent", 1.0))
    timeliness_exp = float(cd_params.get("timeliness_exponent", 0.5))
    uniqueness_exp = float(cd_params.get("uniqueness_exponent", 1.5))
    marginal_exp = float(cd_params.get("marginal_exponent", 1.0))
    floor_threshold = float(cd_params.get("floor_threshold", 0.30))
    epsilon = float(cd_params.get("epsilon", 0.01))

    brier_arr = np.array([m.brier_mean for m in sorted_metrics], dtype=np.float64)

    # Clamp all dimensions to [epsilon, 1.0]
    accuracy_clamped = np.clip(accuracy_dim, epsilon, 1.0)
    edge_clamped = np.clip(edge_dim_arr, epsilon, 1.0)
    timeliness_clamped = np.clip(timeliness_dim, epsilon, 1.0)
    uniqueness_clamped = np.clip(uniqueness_arr, epsilon, 1.0)
    marginal_clamped = np.clip(marginal_arr, epsilon, 1.0)

    # Cobb-Douglas product
    skill_score = (
        accuracy_clamped ** accuracy_exp
        * edge_clamped ** edge_exp
        * timeliness_clamped ** timeliness_exp
        * uniqueness_clamped ** uniqueness_exp
        * marginal_clamped ** marginal_exp
    )

    # Hard accuracy floor: miners with Brier > threshold get zero
    floor_mask = brier_arr > floor_threshold
    skill_score[floor_mask] = 0.0

    # Record intermediates for audit trail
    for i, uid in enumerate(uids):
        result.skill_scores[uid] = float(skill_score[i])
        result.dimension_scores[uid] = {
            "forecast_dim": float(forecast_dim[i]),
            "skill_dim": float(skill_dim[i]),
            "econ_dim": float(econ_dim[i]),
            "info_dim": float(info_dim[i]),
            "accuracy_dim": float(accuracy_clamped[i]),
            "edge_dim": float(edge_clamped[i]),
            "timeliness_dim": float(timeliness_clamped[i]),
            "uniqueness_dim": float(uniqueness_clamped[i]),
            "marginal_dim": float(marginal_clamped[i]),
        }

    # Step 4: Build weight array indexed by UID (zeros for missing)
    scores = np.zeros(n_neurons, dtype=np.float32)
    for i, uid in enumerate(uids):
        if 0 <= uid < n_neurons:
            scores[uid] = float(skill_score[i])

    # Handle NaN
    scores = np.nan_to_num(scores, nan=0.0)

    # L1 normalize
    norm = np.linalg.norm(scores, ord=1)
    all_zero = norm == 0 or np.isnan(norm)

    if all_zero:
        if chain_params.burn_uid is not None and 0 <= chain_params.burn_uid < n_neurons:
            raw_weights = np.zeros(n_neurons, dtype=np.float32)
            raw_weights[chain_params.burn_uid] = 1.0
        else:
            return result
    else:
        raw_weights = scores / norm

        # Step 5: Apply burn rate
        if burn_rate > 0.0 and chain_params.burn_uid is not None:
            burn_uid = chain_params.burn_uid
            if 0 <= burn_uid < n_neurons:
                raw_weights *= (1.0 - burn_rate)
                raw_weights[burn_uid] = burn_rate

    # Record raw weights
    for uid_val in range(n_neurons):
        if raw_weights[uid_val] > 0:
            result.raw_weights[uid_val] = float(raw_weights[uid_val])

    # Step 6: Process for chain (max weight, min allowed)
    max_weight_limit = chain_params.max_weight_limit
    min_allowed = chain_params.min_allowed_weights
    all_uids = np.arange(n_neurons)

    non_zero_mask = raw_weights > 0
    nz_weights = raw_weights[non_zero_mask]
    nz_uids = all_uids[non_zero_mask]

    if len(nz_weights) == 0:
        return result

    if len(nz_weights) < min_allowed:
        # Not enough non-zero weights, use small uniform base
        padded = np.ones(n_neurons, dtype=np.float32) * 1e-5
        padded[non_zero_mask] += nz_weights
        processed = _normalize_max_weight(padded, limit=max_weight_limit)
        processed_uids = all_uids
    else:
        processed = _normalize_max_weight(nz_weights, limit=max_weight_limit)
        processed_uids = nz_uids

    # Step 7: Convert to uint16
    uid_list, weight_list = _convert_to_uint16(processed_uids, processed)

    result.uids = uid_list
    result.uint16_weights = weight_list
    return result


__all__ = ["WeightResult", "compute_weights"]
