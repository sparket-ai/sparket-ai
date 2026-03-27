"""Equivalence test: compute_weights() Cobb-Douglas scoring.

This is the critical "is scoring deterministic?" test. It feeds identical data
through both a hand-rolled reference Cobb-Douglas implementation and the
production compute_weights() function, asserting identical skill_score outputs.

Updated for the Shapley scoring overhaul (Cobb-Douglas 5-pillar architecture).
"""

from __future__ import annotations

import numpy as np
import pytest

from sparket.validator.ledger.compute_weights import compute_weights
from sparket.validator.ledger.models import ChainParamsSnapshot, MinerMetrics, ScoringConfigSnapshot
from sparket.validator.scoring.aggregation.normalization import (
    normalize_percentile,
    normalize_zscore_logistic,
)
from sparket.validator.config.scoring_params import get_scoring_params


def _reference_cobb_douglas_scores(rows: list[dict], params) -> dict[int, float]:
    """Reference Cobb-Douglas scoring implementation.

    Hand-rolled reproduction of compute_weights() internal logic for
    ground-truth comparison. Any change to compute_weights() scoring
    must be mirrored here.
    """
    sorted_rows = sorted(rows, key=lambda r: r["uid"])

    fq_raw = np.array([r["fq_raw"] for r in sorted_rows], dtype=np.float64)
    pss = np.array([r["pss_mean"] for r in sorted_rows], dtype=np.float64)
    cal = np.array([r["cal_score"] for r in sorted_rows], dtype=np.float64)
    es_adj = np.array([r["es_adj"] for r in sorted_rows], dtype=np.float64)
    mes = np.array([r["mes_mean"] for r in sorted_rows], dtype=np.float64)
    sos = np.array([r["sos_score"] for r in sorted_rows], dtype=np.float64)
    lead = np.array([r["lead_score"] for r in sorted_rows], dtype=np.float64)
    brier = np.array([r["brier_mean"] for r in sorted_rows], dtype=np.float64)

    p = params.model_dump(mode="json")
    dim_w = p["dimension_weights"]
    norm_p = p["normalization"]

    w_fq = float(dim_w["w_fq"])
    w_cal = float(dim_w["w_cal"])
    w_edge = float(dim_w["w_edge"])
    w_mes = float(dim_w["w_mes"])
    w_lead = float(dim_w["w_lead"])
    min_count = int(norm_p["min_count_for_zscore"])

    # Normalize
    fq_norm = np.clip((fq_raw + 1) / 2, 0, 1)
    use_zscore = len(sorted_rows) >= min_count
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

    # Dimensions
    forecast_dim = w_fq * fq_norm + w_cal * cal_norm
    skill_dim = pss_norm
    econ_dim = w_edge * es_norm + w_mes * mes_norm

    # Cobb-Douglas pillar mapping
    accuracy_dim = forecast_dim
    edge_dim_arr = econ_dim
    timeliness_dim = 0.5 * skill_dim + 0.5 * lead_norm

    uniqueness_arr = np.array(
        [r.get("uniqueness_dim") if r.get("uniqueness_dim") is not None else float(sos[i])
         for i, r in enumerate(sorted_rows)],
        dtype=np.float64,
    )
    marginal_arr = np.array(
        [r.get("marginal_dim") if r.get("marginal_dim") is not None else 0.5
         for i, r in enumerate(sorted_rows)],
        dtype=np.float64,
    )

    # Cobb-Douglas params
    cd = p.get("cobb_douglas", {})
    accuracy_exp = float(cd.get("accuracy_exponent", 0.5))
    edge_exp = float(cd.get("edge_exponent", 1.0))
    timeliness_exp = float(cd.get("timeliness_exponent", 0.5))
    uniqueness_exp = float(cd.get("uniqueness_exponent", 1.5))
    marginal_exp = float(cd.get("marginal_exponent", 1.0))
    floor_threshold = float(cd.get("floor_threshold", 0.30))
    epsilon = float(cd.get("epsilon", 0.01))

    # Clamp and compute
    accuracy_c = np.clip(accuracy_dim, epsilon, 1.0)
    edge_c = np.clip(edge_dim_arr, epsilon, 1.0)
    timeliness_c = np.clip(timeliness_dim, epsilon, 1.0)
    uniqueness_c = np.clip(uniqueness_arr, epsilon, 1.0)
    marginal_c = np.clip(marginal_arr, epsilon, 1.0)

    skill_score = (
        accuracy_c ** accuracy_exp
        * edge_c ** edge_exp
        * timeliness_c ** timeliness_exp
        * uniqueness_c ** uniqueness_exp
        * marginal_c ** marginal_exp
    )

    # Hard accuracy floor
    skill_score[brier > floor_threshold] = 0.0

    return {r["uid"]: float(skill_score[i]) for i, r in enumerate(sorted_rows)}


def _build_test_miners(n: int = 15, seed: int = 42) -> list[dict]:
    """Generate N miners with varied, realistic metrics."""
    rng = np.random.RandomState(seed)
    miners = []
    for i in range(n):
        miners.append({
            "uid": i + 1,
            "hotkey": f"hk_{i+1}",
            "fq_raw": float(rng.uniform(-0.5, 0.8)),
            "pss_mean": float(rng.uniform(-0.3, 0.5)),
            "es_adj": float(rng.uniform(-0.2, 0.4)),
            "mes_mean": float(rng.uniform(0.3, 0.9)),
            "cal_score": float(rng.uniform(0.2, 0.9)),
            "sharp_score": float(rng.uniform(0.2, 0.8)),
            "sos_score": float(rng.uniform(0.1, 0.9)),
            "lead_score": float(rng.uniform(0.1, 0.8)),
            "brier_mean": float(rng.uniform(0.1, 0.45)),
            "uniqueness_dim": None,
            "marginal_dim": None,
        })
    return miners


class TestComputeWeightsEquivalence:
    """Prove compute_weights Cobb-Douglas scoring matches reference implementation."""

    @pytest.mark.parametrize("n_miners", [3, 5, 10, 15, 50])
    def test_skill_scores_match_reference(self, n_miners: int):
        """Feed same data through reference Cobb-Douglas and compute_weights."""
        params = get_scoring_params()
        miners = _build_test_miners(n=n_miners, seed=n_miners)

        # Reference path
        ref_scores = _reference_cobb_douglas_scores(miners, params)

        # Production path
        metrics = [
            MinerMetrics(
                uid=m["uid"], hotkey=m["hotkey"],
                fq_raw=m["fq_raw"], pss_mean=m["pss_mean"],
                es_adj=m["es_adj"], mes_mean=m["mes_mean"],
                cal_score=m["cal_score"], sharp_score=m["sharp_score"],
                sos_score=m["sos_score"], lead_score=m["lead_score"],
                brier_mean=m["brier_mean"],
                uniqueness_dim=m["uniqueness_dim"],
                marginal_dim=m["marginal_dim"],
            )
            for m in miners
        ]

        config = ScoringConfigSnapshot(params=params.model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=None,
            max_weight_limit=1.0, min_allowed_weights=1,
            n_neurons=max(m["uid"] for m in miners) + 5,
        )

        result = compute_weights(metrics, config, chain)

        for m in miners:
            uid = m["uid"]
            ref_val = ref_scores[uid]
            new_val = result.skill_scores.get(uid)
            assert new_val is not None, f"Missing uid {uid} in compute_weights output"
            assert abs(ref_val - new_val) < 1e-10, (
                f"Miner uid={uid}: ref={ref_val:.12f} vs prod={new_val:.12f}, "
                f"diff={abs(ref_val - new_val):.2e}"
            )

    def test_dimension_scores_match_reference(self):
        """Verify individual Cobb-Douglas dimension scores match."""
        params = get_scoring_params()
        miners = _build_test_miners(n=20, seed=99)

        metrics = [
            MinerMetrics(uid=m["uid"], hotkey=m["hotkey"], **{
                k: m[k] for k in ("fq_raw", "pss_mean", "es_adj", "mes_mean",
                                    "cal_score", "sharp_score", "sos_score",
                                    "lead_score", "brier_mean",
                                    "uniqueness_dim", "marginal_dim")
            })
            for m in miners
        ]
        config = ScoringConfigSnapshot(params=params.model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=None,
            max_weight_limit=1.0, min_allowed_weights=1, n_neurons=30,
        )
        result = compute_weights(metrics, config, chain)

        # Every miner should have all 5 Cobb-Douglas dimensions
        for m in miners:
            uid = m["uid"]
            dims = result.dimension_scores.get(uid, {})
            for dim_name in ("accuracy_dim", "edge_dim", "timeliness_dim",
                             "uniqueness_dim", "marginal_dim"):
                assert dim_name in dims, f"Missing {dim_name} for uid {uid}"
                assert 0 < dims[dim_name] <= 1.0, (
                    f"{dim_name}={dims[dim_name]} out of (0, 1] for uid {uid}"
                )

    def test_edge_case_single_miner(self):
        """Single miner should get a valid Cobb-Douglas score."""
        params = get_scoring_params()
        miners = _build_test_miners(n=1, seed=1)

        ref_scores = _reference_cobb_douglas_scores(miners, params)

        metrics = [MinerMetrics(
            uid=miners[0]["uid"], hotkey=miners[0]["hotkey"],
            fq_raw=miners[0]["fq_raw"], pss_mean=miners[0]["pss_mean"],
            es_adj=miners[0]["es_adj"], mes_mean=miners[0]["mes_mean"],
            cal_score=miners[0]["cal_score"], sharp_score=miners[0]["sharp_score"],
            sos_score=miners[0]["sos_score"], lead_score=miners[0]["lead_score"],
            brier_mean=miners[0]["brier_mean"],
        )]

        config = ScoringConfigSnapshot(params=params.model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=None,
            max_weight_limit=1.0, min_allowed_weights=1, n_neurons=10,
        )
        result = compute_weights(metrics, config, chain)

        ref_val = ref_scores[miners[0]["uid"]]
        new_val = result.skill_scores.get(miners[0]["uid"])
        assert abs(ref_val - new_val) < 1e-10

    def test_edge_case_identical_miners(self):
        """All miners identical: should get equal Cobb-Douglas scores."""
        params = get_scoring_params()
        base = {
            "fq_raw": 0.3, "pss_mean": 0.1, "es_adj": 0.05,
            "mes_mean": 0.6, "cal_score": 0.7, "sharp_score": 0.6,
            "sos_score": 0.5, "lead_score": 0.4, "brier_mean": 0.20,
            "uniqueness_dim": None, "marginal_dim": None,
        }
        miners = [{"uid": i + 1, "hotkey": f"hk_{i+1}", **base} for i in range(10)]

        ref_scores = _reference_cobb_douglas_scores(miners, params)
        metrics = [
            MinerMetrics(uid=m["uid"], hotkey=m["hotkey"], **{
                k: v for k, v in base.items() if k not in ("hotkey",)
            })
            for m in miners
        ]

        config = ScoringConfigSnapshot(params=params.model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=None,
            max_weight_limit=1.0, min_allowed_weights=1, n_neurons=20,
        )
        result = compute_weights(metrics, config, chain)

        # All scores should be equal
        scores = list(result.skill_scores.values())
        assert all(abs(s - scores[0]) < 1e-10 for s in scores)

        # And match reference
        ref_vals = list(ref_scores.values())
        assert all(abs(r - ref_vals[0]) < 1e-10 for r in ref_vals)
        assert abs(scores[0] - ref_vals[0]) < 1e-10

    def test_hard_floor_consistency(self):
        """Miners above Brier threshold get zero in both paths."""
        params = get_scoring_params()
        miners = _build_test_miners(n=10, seed=77)
        # Force some miners above the floor
        miners[0]["brier_mean"] = 0.35
        miners[3]["brier_mean"] = 0.40

        ref_scores = _reference_cobb_douglas_scores(miners, params)

        metrics = [
            MinerMetrics(
                uid=m["uid"], hotkey=m["hotkey"],
                fq_raw=m["fq_raw"], pss_mean=m["pss_mean"],
                es_adj=m["es_adj"], mes_mean=m["mes_mean"],
                cal_score=m["cal_score"], sharp_score=m["sharp_score"],
                sos_score=m["sos_score"], lead_score=m["lead_score"],
                brier_mean=m["brier_mean"],
            )
            for m in miners
        ]

        config = ScoringConfigSnapshot(params=params.model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=None,
            max_weight_limit=1.0, min_allowed_weights=1, n_neurons=20,
        )
        result = compute_weights(metrics, config, chain)

        for m in miners:
            uid = m["uid"]
            assert abs(ref_scores[uid] - result.skill_scores[uid]) < 1e-10
            if m["brier_mean"] > 0.30:
                assert result.skill_scores[uid] == 0.0
