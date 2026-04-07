"""Verification gate tests G1-G3 for the Sparket scoring system.

G1: Accurate Miner Scoring
G2: Deterministic Weight Computation
G3: Auditor Consensus (auth layer)
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from sparket.validator.ledger.models import (
    AccumulatorEntry,
    ChainParamsSnapshot,
    MetricAccumulator,
    MinerMetrics,
    ScoringConfigSnapshot,
)
from sparket.validator.ledger.compute_weights import compute_weights, WeightResult
from sparket.validator.handlers.score.outcome_score import (
    SIDES_BY_KIND,
    get_sides_for_kind,
)
from sparket.validator.ledger.auth import AccessPolicy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _default_scoring_config() -> ScoringConfigSnapshot:
    """Scoring config matching production defaults."""
    return ScoringConfigSnapshot(params={
        "dimension_weights": {
            "w_fq": 0.6,
            "w_cal": 0.4,
            "w_edge": 0.7,
            "w_mes": 0.3,
            "w_sos": 0.6,
            "w_lead": 0.4,
        },
        "skill_score_weights": {
            "w_outcome_accuracy": 0.10,
            "w_outcome_relative": 0.10,
            "w_odds_edge": 0.50,
            "w_info_adv": 0.30,
        },
        "normalization": {"min_count_for_zscore": 10},
        "weight_emission": {"burn_rate": 0.0},
        "cobb_douglas": {
            "accuracy_exponent": 0.5,
            "edge_exponent": 1.0,
            "timeliness_exponent": 0.5,
            "uniqueness_exponent": 1.5,
            "marginal_exponent": 1.0,
            "floor_threshold": 0.30,
            "epsilon": 0.01,
        },
    })


def _default_chain_params(n_neurons: int = 256) -> ChainParamsSnapshot:
    return ChainParamsSnapshot(
        burn_rate=0.0,
        burn_uid=None,
        max_weight_limit=0.1,
        min_allowed_weights=1,
        n_neurons=n_neurons,
    )


def _make_miner(
    uid: int,
    hotkey: str = "",
    brier_mean: float = 0.0,
    fq_raw: float = 0.0,
    pss_mean: float = 0.0,
    es_adj: float = 0.0,
    mes_mean: float = 0.5,
    cal_score: float = 0.5,
    sos_score: float = 0.5,
    lead_score: float = 0.5,
    uniqueness_dim: float | None = None,
    marginal_dim: float | None = None,
) -> MinerMetrics:
    return MinerMetrics(
        uid=uid,
        hotkey=hotkey or f"hotkey_{uid}",
        brier_mean=brier_mean,
        fq_raw=fq_raw,
        pss_mean=pss_mean,
        es_adj=es_adj,
        mes_mean=mes_mean,
        cal_score=cal_score,
        sos_score=sos_score,
        lead_score=lead_score,
        uniqueness_dim=uniqueness_dim,
        marginal_dim=marginal_dim,
    )


def _mock_metagraph(hotkeys: list[str]) -> MagicMock:
    mg = MagicMock()
    mg.hotkeys = hotkeys
    mg.validator_permit = [True] * len(hotkeys)
    mg.S = [200_000.0] * len(hotkeys)
    return mg


# ===================================================================
# G1: Accurate Miner Scoring
# ===================================================================


class TestG1_BrierFloorZeroOutcomes:
    """G1.2: Miner with n_outcomes=0 gets brier_mean=0.5 which exceeds
    floor_threshold 0.30, so skill_score must be 0.
    """

    def test_accumulator_brier_default_is_half(self):
        """AccumulatorEntry with wt=0 derives brier_mean = 0.5."""
        acc = AccumulatorEntry(
            miner_id=1,
            hotkey="hk_no_outcomes",
            uid=0,
            n_submissions=5,
            n_outcomes=0,
            brier=MetricAccumulator(ws=0, wt=0),
        )
        acc.derive_means()
        assert acc.brier_mean == 0.5

    def test_brier_floor_zero_outcomes(self):
        """Miner with brier_mean=0.5 (> 0.30 floor) gets skill_score=0."""
        miner = _make_miner(uid=0, brier_mean=0.5)
        result = compute_weights(
            [miner],
            _default_scoring_config(),
            _default_chain_params(n_neurons=8),
        )
        assert result.skill_scores.get(0, 0.0) == 0.0

    def test_brier_floor_from_accumulator_path(self):
        """Full path: AccumulatorEntry -> MinerMetrics -> compute_weights -> 0."""
        acc = AccumulatorEntry(
            miner_id=1,
            hotkey="hk_acc",
            uid=0,
            n_submissions=10,
            n_outcomes=0,
            brier=MetricAccumulator(ws=0, wt=0),
        )
        metrics = MinerMetrics.from_accumulator(acc)
        assert metrics.brier_mean == 0.5

        result = compute_weights(
            [metrics],
            _default_scoring_config(),
            _default_chain_params(n_neurons=8),
        )
        assert result.skill_scores.get(0, 0.0) == 0.0


class TestG1_GtSidesMatchMarketKind:
    """G1.4: get_sides_for_kind returns only valid sides per market kind."""

    @pytest.mark.parametrize("kind,expected_sides", [
        ("moneyline", ["home", "away", "draw"]),
        ("spread", ["home", "away"]),
        ("total", ["over", "under"]),
        ("draw_no_bet", ["home", "away"]),
    ])
    def test_sides_for_known_kinds(self, kind: str, expected_sides: list[str]):
        assert get_sides_for_kind(kind) == expected_sides

    @pytest.mark.parametrize("kind", list(SIDES_BY_KIND.keys()))
    def test_all_sides_are_valid_strings(self, kind: str):
        sides = get_sides_for_kind(kind)
        assert len(sides) >= 2
        for side in sides:
            assert isinstance(side, str)
            assert side == side.lower().strip()

    def test_unknown_kind_defaults_to_home_away(self):
        """Unrecognised kind falls back to ['home', 'away']."""
        assert get_sides_for_kind("some_future_kind") == ["home", "away"]

    @pytest.mark.parametrize("kind", list(SIDES_BY_KIND.keys()))
    def test_gt_filtering_removes_invalid_sides(self, kind: str):
        """Simulate the GT filtering logic from OutcomeScoreHandler:
        gt_probs = {s: p for s, p in gt_probs.items() if s in sides}
        Contaminated GT (all 5 sides present) should be filtered down.
        """
        contaminated_gt = {
            "home": 0.40, "away": 0.35, "draw": 0.10,
            "over": 0.55, "under": 0.45,
        }
        valid_sides = get_sides_for_kind(kind)
        filtered = {s: p for s, p in contaminated_gt.items() if s in valid_sides}
        assert set(filtered.keys()) <= set(valid_sides)
        assert len(filtered) == len(valid_sides)

    def test_case_insensitivity(self):
        assert get_sides_for_kind("Moneyline") == ["home", "away", "draw"]
        assert get_sides_for_kind("TOTAL") == ["over", "under"]


class TestG1_SkillScoreRequiresOutcomes:
    """G1.5: Miner with submissions but zero outcomes -> skill_score=0."""

    def test_skill_score_zero_when_brier_above_floor(self):
        """brier_mean=0.5 (no outcomes default) exceeds floor => score 0."""
        miner = _make_miner(uid=0, brier_mean=0.5)
        result = compute_weights(
            [miner],
            _default_scoring_config(),
            _default_chain_params(n_neurons=8),
        )
        assert result.skill_scores[0] == 0.0

    def test_miner_with_good_brier_gets_nonzero(self):
        """Sanity: miner with brier_mean=0.15 (< 0.30) should score > 0."""
        miner = _make_miner(
            uid=0,
            brier_mean=0.15,
            fq_raw=0.3,
            pss_mean=0.4,
            es_adj=0.2,
            cal_score=0.7,
            sos_score=0.6,
            lead_score=0.5,
        )
        result = compute_weights(
            [miner],
            _default_scoring_config(),
            _default_chain_params(n_neurons=8),
        )
        assert result.skill_scores[0] > 0.0


# ===================================================================
# G2: Deterministic Weight Computation
# ===================================================================


class TestG2_ComputeWeightsDeterministic:
    """G2.1: Identical inputs -> identical uint16 output."""

    def test_compute_weights_deterministic(self):
        miners = [
            _make_miner(uid=i, brier_mean=0.10 + 0.02 * i, fq_raw=0.1 * i,
                        pss_mean=0.05 * i, es_adj=0.03 * i, cal_score=0.5 + 0.05 * i,
                        sos_score=0.4 + 0.02 * i, lead_score=0.3 + 0.03 * i)
            for i in range(5)
        ]
        cfg = _default_scoring_config()
        chain = _default_chain_params(n_neurons=16)

        r1 = compute_weights(miners, cfg, chain)
        r2 = compute_weights(miners, cfg, chain)

        assert r1.uids == r2.uids
        assert r1.uint16_weights == r2.uint16_weights

    def test_deterministic_across_list_order(self):
        """compute_weights sorts by uid internally, so order shouldn't matter."""
        m0 = _make_miner(uid=0, brier_mean=0.10, fq_raw=0.2, es_adj=0.1)
        m1 = _make_miner(uid=1, brier_mean=0.15, fq_raw=0.3, es_adj=0.2)

        cfg = _default_scoring_config()
        chain = _default_chain_params(n_neurons=8)

        r_asc = compute_weights([m0, m1], cfg, chain)
        r_desc = compute_weights([m1, m0], cfg, chain)

        assert r_asc.uids == r_desc.uids
        assert r_asc.uint16_weights == r_desc.uint16_weights


class TestG2_AccumulatorDeriveMeans:
    """G2.2: derive_means() produces ws/wt for each metric."""

    def test_derive_means_known_values(self):
        acc = AccumulatorEntry(
            miner_id=1, hotkey="hk_test", uid=0,
            n_submissions=10, n_outcomes=5,
            brier=MetricAccumulator(ws=1.5, wt=5.0),
            fq=MetricAccumulator(ws=2.0, wt=4.0),
            pss=MetricAccumulator(ws=3.0, wt=6.0),
            es=MetricAccumulator(ws=-0.5, wt=2.0),
            mes=MetricAccumulator(ws=1.0, wt=2.0),
            sos=MetricAccumulator(ws=0.8, wt=4.0),
            lead=MetricAccumulator(ws=2.5, wt=5.0),
        )
        acc.derive_means()

        assert acc.brier_mean == pytest.approx(0.3)     # 1.5/5
        assert acc.fq_raw == pytest.approx(0.5)         # 2.0/4
        assert acc.pss_mean == pytest.approx(0.5)        # 3.0/6
        assert acc.es_adj == pytest.approx(-0.25)        # -0.5/2
        assert acc.mes_mean == pytest.approx(0.5)        # 1.0/2
        assert acc.sos_score == pytest.approx(0.2)       # 0.8/4
        assert acc.lead_score == pytest.approx(0.5)      # 2.5/5

    def test_derive_means_zero_weight_defaults(self):
        """When wt=0, derive_means should use safe defaults."""
        acc = AccumulatorEntry(
            miner_id=1, hotkey="hk_zero", uid=0,
        )
        acc.derive_means()

        assert acc.brier_mean == 0.5  # critical: not 0.0
        assert acc.fq_raw == 0.0
        assert acc.pss_mean == 0.0
        assert acc.es_adj == 0.0
        assert acc.mes_mean == 0.5
        assert acc.sos_score == 0.5
        assert acc.lead_score == 0.5


class TestG2_CobbDouglasNoNan:
    """G2.4: Fuzz Cobb-Douglas dimensions -- no NaN/Inf in output."""

    BOUNDARY_VALUES = [0.0, 1e-10, 0.01, 0.25, 0.5, 0.75, 1.0]

    def test_boundary_cases(self):
        """Sweep boundary values across all metric dimensions."""
        cfg = _default_scoring_config()
        chain = _default_chain_params(n_neurons=64)

        uid = 0
        for brier in [0.0, 0.15, 0.29, 0.31, 0.5]:
            for fq in self.BOUNDARY_VALUES:
                miner = _make_miner(
                    uid=uid,
                    brier_mean=brier,
                    fq_raw=fq,
                    pss_mean=fq,
                    es_adj=fq,
                    cal_score=fq,
                    mes_mean=fq,
                    sos_score=fq,
                    lead_score=fq,
                )
                result = compute_weights([miner], cfg, chain)
                score = result.skill_scores.get(uid, 0.0)
                assert np.isfinite(score), (
                    f"NaN/Inf with brier={brier}, fq={fq}: score={score}"
                )

    def test_all_zeros(self):
        """All metrics zero -- should still produce a finite score."""
        miner = _make_miner(uid=0, brier_mean=0.0, fq_raw=0.0, pss_mean=0.0,
                            es_adj=0.0, cal_score=0.0, sos_score=0.0, lead_score=0.0)
        result = compute_weights(
            [miner], _default_scoring_config(), _default_chain_params(n_neurons=8),
        )
        score = result.skill_scores.get(0, 0.0)
        assert np.isfinite(score)

    def test_all_ones(self):
        """All metrics at maximum."""
        miner = _make_miner(uid=0, brier_mean=0.0, fq_raw=1.0, pss_mean=1.0,
                            es_adj=1.0, cal_score=1.0, sos_score=1.0, lead_score=1.0,
                            mes_mean=1.0, uniqueness_dim=1.0, marginal_dim=1.0)
        result = compute_weights(
            [miner], _default_scoring_config(), _default_chain_params(n_neurons=8),
        )
        score = result.skill_scores.get(0, 0.0)
        assert np.isfinite(score)
        assert score > 0.0

    def test_random_fuzz(self):
        """Random values across 50 miners -- no NaN/Inf anywhere."""
        rng = np.random.default_rng(seed=42)
        miners = []
        for uid in range(50):
            miners.append(_make_miner(
                uid=uid,
                brier_mean=float(rng.uniform(0, 0.5)),
                fq_raw=float(rng.uniform(-1, 1)),
                pss_mean=float(rng.uniform(0, 1)),
                es_adj=float(rng.uniform(-1, 1)),
                cal_score=float(rng.uniform(0, 1)),
                mes_mean=float(rng.uniform(0, 1)),
                sos_score=float(rng.uniform(0, 1)),
                lead_score=float(rng.uniform(0, 1)),
                uniqueness_dim=float(rng.uniform(0, 1)),
                marginal_dim=float(rng.uniform(0, 1)),
            ))
        result = compute_weights(
            miners, _default_scoring_config(), _default_chain_params(n_neurons=64),
        )
        for uid, score in result.skill_scores.items():
            assert np.isfinite(score), f"uid={uid} has non-finite skill_score={score}"
        for uid, dims in result.dimension_scores.items():
            for dim_name, dim_val in dims.items():
                assert np.isfinite(dim_val), (
                    f"uid={uid} dim={dim_name} is non-finite: {dim_val}"
                )


class TestG2_L1NormSumOne:
    """G2.5: Float weights (before uint16 conversion) sum to 1.0."""

    def test_l1_norm_sum_one(self):
        miners = [
            _make_miner(uid=i, brier_mean=0.10, fq_raw=0.2 + 0.05 * i,
                        pss_mean=0.3, es_adj=0.1, cal_score=0.6,
                        sos_score=0.5, lead_score=0.4)
            for i in range(5)
        ]
        cfg = _default_scoring_config()
        # No burn so raw_weights should L1-normalize to 1.0
        chain = _default_chain_params(n_neurons=16)

        result = compute_weights(miners, cfg, chain)

        raw_sum = sum(result.raw_weights.values())
        assert raw_sum == pytest.approx(1.0, abs=1e-6), (
            f"raw weight sum = {raw_sum}, expected ~1.0"
        )

    def test_l1_norm_with_floored_miners(self):
        """Even when some miners are floored (brier > 0.30), remaining sum to 1."""
        miners = [
            _make_miner(uid=0, brier_mean=0.10, fq_raw=0.3, pss_mean=0.4,
                        es_adj=0.2, cal_score=0.7, sos_score=0.5, lead_score=0.5),
            _make_miner(uid=1, brier_mean=0.50),  # floored
            _make_miner(uid=2, brier_mean=0.15, fq_raw=0.2, pss_mean=0.3,
                        es_adj=0.15, cal_score=0.6, sos_score=0.4, lead_score=0.4),
        ]
        cfg = _default_scoring_config()
        chain = _default_chain_params(n_neurons=16)

        result = compute_weights(miners, cfg, chain)

        # uid 1 should be floored
        assert result.skill_scores.get(1, 0.0) == 0.0
        # remaining weights still sum to 1
        if result.raw_weights:
            raw_sum = sum(result.raw_weights.values())
            assert raw_sum == pytest.approx(1.0, abs=1e-6)


# ===================================================================
# G3: Auditor Consensus
# ===================================================================


class TestG3_AuthRejectsInvalidSignature:
    """G3.3: Wrong signature -> verify_response returns None."""

    def test_invalid_signature_returns_none(self):
        mg = _mock_metagraph(["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"])
        policy = AccessPolicy(metagraph=mg, min_stake_threshold=0)

        hotkey = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        nonce = policy.issue_challenge(hotkey)

        # Submit garbage signature
        bad_sig = "00" * 64
        result = policy.verify_response(hotkey, nonce, bad_sig)
        assert result is None

    def test_unknown_nonce_returns_none(self):
        mg = _mock_metagraph(["5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"])
        policy = AccessPolicy(metagraph=mg, min_stake_threshold=0)

        result = policy.verify_response(
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            "nonexistent_nonce",
            "00" * 64,
        )
        assert result is None

    def test_hotkey_mismatch_returns_none(self):
        mg = _mock_metagraph([
            "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
        ])
        policy = AccessPolicy(metagraph=mg, min_stake_threshold=0)

        hotkey_a = "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
        hotkey_b = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"
        nonce = policy.issue_challenge(hotkey_a)

        # Try to verify with a different hotkey
        result = policy.verify_response(hotkey_b, nonce, "00" * 64)
        assert result is None


class TestG3_RateLimitEnforcement:
    """G3.4: Exceed rate_limit_per_hour -> check_rate_limit returns False."""

    def test_rate_limit_exceeded(self):
        mg = _mock_metagraph(["hk_test"])
        policy = AccessPolicy(
            metagraph=mg,
            rate_limit_per_hour=5,
        )

        hotkey = "hk_test"
        for i in range(5):
            assert policy.check_rate_limit(hotkey) is True, f"Request {i+1} should pass"

        # 6th request should be rejected
        assert policy.check_rate_limit(hotkey) is False

    def test_rate_limit_allows_within_budget(self):
        mg = _mock_metagraph(["hk_test"])
        policy = AccessPolicy(
            metagraph=mg,
            rate_limit_per_hour=100,
        )

        for _ in range(100):
            assert policy.check_rate_limit("hk_test") is True

        assert policy.check_rate_limit("hk_test") is False

    def test_rate_limit_independent_per_hotkey(self):
        mg = _mock_metagraph(["hk_a", "hk_b"])
        policy = AccessPolicy(metagraph=mg, rate_limit_per_hour=3)

        for _ in range(3):
            assert policy.check_rate_limit("hk_a") is True
        assert policy.check_rate_limit("hk_a") is False

        # hk_b should still be allowed
        assert policy.check_rate_limit("hk_b") is True
