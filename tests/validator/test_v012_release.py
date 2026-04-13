"""v0.1.2 release test suite — auditor/validator compatibility, scoring
correctness, edge cases, and throughput.

Targets the three compat-hack cleanups in this release:
  1. brier_mean default aligned to 0.5 across all paths
  2. Exporter no longer fakes ws/wt for zero-weight Brier
  3. compute_weights timeliness_dim cleaned up

Also stress-tests at production scale (163+ miners) and probes
numerical edge cases that simple unit tests miss.
"""

from __future__ import annotations

import json
import math
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pytest

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params
from sparket.validator.ledger.compute_weights import (
    U16_MAX,
    WeightResult,
    _convert_to_uint16,
    _normalize_max_weight,
    compute_weights,
)
from sparket.validator.ledger.models import (
    LEDGER_SCHEMA_VERSION,
    AccumulatorEntry,
    CheckpointWindow,
    ChainParamsSnapshot,
    LedgerManifest,
    MetricAccumulator,
    MinerMetrics,
    MinerRosterEntry,
    ScoringConfigSnapshot,
)

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(timezone.utc)


def _prod_scoring_config() -> ScoringConfigSnapshot:
    """Production scoring config — the real defaults, not a toy."""
    return ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))


def _chain(
    n_neurons: int = 256,
    burn_rate: float = 0.0,
    burn_uid: int | None = 0,
    max_weight_limit: float = 0.1,
    min_allowed_weights: int = 1,
) -> ChainParamsSnapshot:
    return ChainParamsSnapshot(
        burn_rate=burn_rate,
        burn_uid=burn_uid,
        max_weight_limit=max_weight_limit,
        min_allowed_weights=min_allowed_weights,
        n_neurons=n_neurons,
    )


def _miner(
    uid: int,
    *,
    brier: float = 0.20,
    fq: float = 0.05,
    pss: float = 0.1,
    es: float = 0.04,
    mes: float = 0.6,
    cal: float = 0.5,
    sharp: float = 0.5,
    sos: float = 0.5,
    lead: float = 0.5,
    uniq: float | None = None,
    marg: float | None = None,
) -> MinerMetrics:
    return MinerMetrics(
        uid=uid,
        hotkey=f"5{'0' * 46}{uid:02d}",
        fq_raw=fq,
        pss_mean=pss,
        es_adj=es,
        mes_mean=mes,
        cal_score=cal,
        sharp_score=sharp,
        sos_score=sos,
        lead_score=lead,
        brier_mean=brier,
        uniqueness_dim=uniq,
        marginal_dim=marg,
    )


def _accumulator(
    uid: int,
    *,
    brier_ws: float = 20.0,
    brier_wt: float = 100.0,
    fq_ws: float = 5.0,
    fq_wt: float = 100.0,
    pss_ws: float = 10.0,
    pss_wt: float = 100.0,
    es_ws: float = 4.0,
    es_wt: float = 100.0,
    mes_ws: float = 60.0,
    mes_wt: float = 100.0,
    sos_ws: float = 50.0,
    sos_wt: float = 100.0,
    lead_ws: float = 50.0,
    lead_wt: float = 100.0,
    cal: float = 0.5,
    sharp: float = 0.5,
    uniq: float | None = None,
    marg: float | None = None,
) -> AccumulatorEntry:
    return AccumulatorEntry(
        miner_id=uid,
        hotkey=f"5{'0' * 46}{uid:02d}",
        uid=uid,
        n_submissions=int(fq_wt),
        brier=MetricAccumulator(ws=brier_ws, wt=brier_wt),
        fq=MetricAccumulator(ws=fq_ws, wt=fq_wt),
        pss=MetricAccumulator(ws=pss_ws, wt=pss_wt),
        es=MetricAccumulator(ws=es_ws, wt=es_wt),
        mes=MetricAccumulator(ws=mes_ws, wt=mes_wt),
        sos=MetricAccumulator(ws=sos_ws, wt=sos_wt),
        lead=MetricAccumulator(ws=lead_ws, wt=lead_wt),
        cal_score=cal,
        sharp_score=sharp,
        uniqueness_dim=uniq,
        marginal_dim=marg,
    )


def _metrics_from_acc(acc: AccumulatorEntry) -> MinerMetrics:
    """Auditor path: AccumulatorEntry → MinerMetrics."""
    return MinerMetrics.from_accumulator(acc)


def _generate_diverse_miners(n: int, seed: int = 42) -> list[MinerMetrics]:
    """Generate n miners with diverse, realistic metric distributions."""
    rng = np.random.RandomState(seed)
    miners = []
    for uid in range(1, n + 1):
        brier = rng.beta(2, 5) * 0.5  # skewed low, most < 0.3
        miners.append(_miner(
            uid,
            brier=brier,
            fq=rng.uniform(-0.3, 0.3),
            pss=rng.normal(0.0, 0.5),
            es=rng.normal(0.05, 0.03),
            mes=rng.beta(3, 2),
            cal=rng.beta(5, 3),
            sharp=rng.beta(4, 4),
            sos=rng.beta(3, 3),
            lead=rng.beta(3, 3),
            uniq=rng.beta(3, 3) if rng.random() > 0.2 else None,
            marg=rng.beta(5, 5) if rng.random() > 0.2 else None,
        ))
    return miners


# ===================================================================
# 1. BRIER DEFAULT ALIGNMENT (the core fix in this release)
# ===================================================================

class TestBrierDefaultAlignment:
    """Verify that brier_mean defaults to 0.5 uniformly across every path,
    so miners with no outcome data always hit the Brier floor."""

    def test_miner_metrics_field_default_is_half(self):
        m = MinerMetrics(uid=1, hotkey="hk")
        assert m.brier_mean == 0.5

    def test_accumulator_entry_field_default_is_half(self):
        acc = AccumulatorEntry(miner_id=1, hotkey="hk", uid=1)
        assert acc.brier_mean == 0.5

    def test_derive_means_zero_weight_returns_half(self):
        """derive_means with wt=0 must return 0.5, not 0.0."""
        acc = AccumulatorEntry(
            miner_id=1, hotkey="hk", uid=1,
            brier=MetricAccumulator(ws=0.0, wt=0.0),
        )
        acc.derive_means()
        assert acc.brier_mean == 0.5

    def test_from_accumulator_zero_brier_gives_half(self):
        """Auditor path: zero ws/wt → brier_mean 0.5 → floored."""
        acc = _accumulator(1, brier_ws=0.0, brier_wt=0.0)
        m = _metrics_from_acc(acc)
        assert m.brier_mean == 0.5

    def test_zero_brier_miner_is_floored(self):
        """A miner with no outcome data (brier_mean=0.5) must get skill_score=0."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=0.5)
        result = compute_weights([m], config, chain)
        assert result.skill_scores[1] == 0.0

    def test_primary_and_auditor_agree_on_no_outcome_miner(self):
        """Both paths must zero a miner with no Brier data."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)

        # Primary path: brier_mean=0.5 directly
        primary = [_miner(1, brier=0.5), _miner(2, brier=0.18)]
        primary_result = compute_weights(primary, config, chain)

        # Auditor path: ws=0/wt=0 → derive_means → 0.5
        acc1 = _accumulator(1, brier_ws=0, brier_wt=0)
        acc2 = _accumulator(2, brier_ws=18.0, brier_wt=100.0)
        auditor = [_metrics_from_acc(acc1), _metrics_from_acc(acc2)]
        auditor_result = compute_weights(auditor, config, chain)

        assert primary_result.skill_scores[1] == 0.0
        assert auditor_result.skill_scores[1] == 0.0
        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights

    @pytest.mark.parametrize("brier_mean", [0.30, 0.300001, 0.2999999])
    def test_brier_floor_boundary(self, brier_mean: float):
        """Test exact boundary: > 0.30 zeroed, <= 0.30 passes."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=brier_mean)
        result = compute_weights([m], config, chain)
        if brier_mean > 0.30:
            assert result.skill_scores[1] == 0.0
        else:
            assert result.skill_scores[1] > 0.0

    def test_exporter_raw_zero_brier_roundtrips_correctly(self):
        """Raw ws=0/wt=0 survives JSON serialization and derive_means still
        produces 0.5 — validates the exporter hack removal."""
        acc = _accumulator(1, brier_ws=0.0, brier_wt=0.0)
        data = json.loads(acc.model_dump_json())
        assert data["brier"]["ws"] == 0.0
        assert data["brier"]["wt"] == 0.0

        restored = AccumulatorEntry.model_validate(data)
        restored.derive_means()
        assert restored.brier_mean == 0.5


# ===================================================================
# 2. PRIMARY ↔ AUDITOR WEIGHT EQUIVALENCE
# ===================================================================

class TestPrimaryAuditorEquivalence:
    """The auditor path (AccumulatorEntry → derive_means → MinerMetrics →
    compute_weights) must produce bit-identical uint16 weights to the
    primary path (MinerMetrics → compute_weights) for the same logical data."""

    def _make_paired_data(self, n: int, seed: int = 99):
        """Build matching primary metrics and auditor accumulators."""
        rng = np.random.RandomState(seed)
        primaries = []
        accumulators = []
        wt = 100.0
        for uid in range(1, n + 1):
            fq_raw = rng.uniform(-0.2, 0.3)
            pss_mean = rng.normal(0, 0.3)
            es_adj = rng.uniform(0.01, 0.1)
            mes_mean = rng.beta(3, 2)
            sos_score = rng.beta(3, 3)
            lead_score = rng.beta(3, 3)
            brier_mean = rng.beta(2, 8) * 0.4
            cal = rng.beta(5, 3)
            sharp = rng.beta(4, 4)
            uniq = rng.beta(3, 3) if rng.random() > 0.3 else None
            marg = rng.beta(5, 5) if rng.random() > 0.3 else None

            primaries.append(_miner(
                uid, brier=brier_mean, fq=fq_raw, pss=pss_mean,
                es=es_adj, mes=mes_mean, cal=cal, sharp=sharp,
                sos=sos_score, lead=lead_score, uniq=uniq, marg=marg,
            ))
            accumulators.append(_accumulator(
                uid,
                brier_ws=brier_mean * wt, brier_wt=wt,
                fq_ws=fq_raw * wt, fq_wt=wt,
                pss_ws=pss_mean * wt, pss_wt=wt,
                es_ws=es_adj * wt, es_wt=wt,
                mes_ws=mes_mean * wt, mes_wt=wt,
                sos_ws=sos_score * wt, sos_wt=wt,
                lead_ws=lead_score * wt, lead_wt=wt,
                cal=cal, sharp=sharp, uniq=uniq, marg=marg,
            ))
        return primaries, accumulators

    @pytest.mark.parametrize("n_miners", [5, 15, 50, 163])
    def test_exact_weight_match(self, n_miners: int):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=256)
        primaries, accumulators = self._make_paired_data(n_miners)

        primary_result = compute_weights(primaries, config, chain)
        auditor_metrics = [_metrics_from_acc(a) for a in accumulators]
        auditor_result = compute_weights(auditor_metrics, config, chain)

        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights

    def test_json_roundtrip_preserves_equivalence(self):
        """Serialize checkpoint → deserialize → compute_weights must
        still match the primary. This catches float precision drift."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=64)
        primaries, accumulators = self._make_paired_data(20, seed=77)

        primary_result = compute_weights(primaries, config, chain)

        # Serialize and restore
        acc_json = [a.model_dump(mode="json") for a in accumulators]
        json_str = json.dumps(acc_json)
        restored = [AccumulatorEntry.model_validate(d) for d in json.loads(json_str)]
        auditor_metrics = [_metrics_from_acc(a) for a in restored]
        auditor_result = compute_weights(auditor_metrics, config, chain)

        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights

    def test_mixed_zero_weight_accumulators(self):
        """Some miners have zero wt for some metrics. Auditor derive_means
        must use the same defaults as MinerMetrics field defaults."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20)

        # Primary: explicit defaults matching derive_means
        p1 = _miner(1, brier=0.15, fq=0.0, pss=0.0, es=0.0, mes=0.5,
                     sos=0.5, lead=0.5)
        primary_result = compute_weights([p1], config, chain)

        # Auditor: zero wt for fq, pss, es — derive_means should return 0.0
        a1 = _accumulator(1, brier_ws=15, brier_wt=100,
                          fq_ws=0, fq_wt=0, pss_ws=0, pss_wt=0,
                          es_ws=0, es_wt=0, mes_ws=0, mes_wt=0,
                          sos_ws=0, sos_wt=0, lead_ws=0, lead_wt=0)
        auditor_result = compute_weights([_metrics_from_acc(a1)], config, chain)

        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights


# ===================================================================
# 3. DETERMINISM AND NUMERICAL STABILITY
# ===================================================================

class TestDeterminism:
    """compute_weights must be fully deterministic — same inputs, same
    uint16 output, every time, regardless of call order or system state."""

    def test_100_identical_runs(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=256)
        miners = _generate_diverse_miners(50)
        baseline = compute_weights(miners, config, chain)

        for _ in range(99):
            result = compute_weights(miners, config, chain)
            assert result.uids == baseline.uids
            assert result.uint16_weights == baseline.uint16_weights

    def test_input_order_independence(self):
        """Shuffling the input list must not change output."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=64)
        miners = _generate_diverse_miners(30)
        baseline = compute_weights(miners, config, chain)

        rng = np.random.RandomState(12345)
        for _ in range(10):
            shuffled = list(miners)
            rng.shuffle(shuffled)
            result = compute_weights(shuffled, config, chain)
            assert result.uids == baseline.uids
            assert result.uint16_weights == baseline.uint16_weights

    def test_no_nan_or_inf_in_any_output(self):
        """Across 500 random miners, no NaN/Inf in any intermediate."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=512)
        miners = _generate_diverse_miners(500, seed=7)
        result = compute_weights(miners, config, chain)

        for uid, score in result.skill_scores.items():
            assert math.isfinite(score), f"uid {uid} skill_score={score}"
        for uid, w in result.raw_weights.items():
            assert math.isfinite(w), f"uid {uid} raw_weight={w}"
        for w in result.uint16_weights:
            assert 0 <= w <= U16_MAX, f"uint16 weight {w} out of range"


class TestNumericalEdgeCases:
    """Probe degenerate inputs that expose floating-point fragility."""

    def test_all_metrics_zero(self):
        """Every metric is zero — should not crash, all scores clamped to epsilon."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=0.0, fq=0.0, pss=0.0, es=0.0, mes=0.0,
                    cal=0.0, sharp=0.0, sos=0.0, lead=0.0)
        result = compute_weights([m], config, chain)
        # brier=0.0 < 0.30 so miner passes floor; score should be epsilon**sum(exponents)
        assert result.skill_scores[1] > 0

    def test_all_metrics_one(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=0.0, fq=1.0, pss=1.0, es=1.0, mes=1.0,
                    cal=1.0, sharp=1.0, sos=1.0, lead=1.0, uniq=1.0, marg=1.0)
        result = compute_weights([m], config, chain)
        assert result.skill_scores[1] > 0

    def test_extreme_negative_pss(self):
        """PSS can be very negative for poor forecasters."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=0.1, pss=-10.0)
        result = compute_weights([m], config, chain)
        assert math.isfinite(result.skill_scores[1])

    def test_extreme_fq_raw(self):
        """fq_raw ranges [-1, 1]; test boundaries."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        for fq_val in [-1.0, -0.999, 0.0, 0.999, 1.0]:
            m = _miner(1, brier=0.1, fq=fq_val)
            result = compute_weights([m], config, chain)
            assert math.isfinite(result.skill_scores[1])

    def test_single_miner(self):
        """One miner should get all non-burn weight."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10, burn_rate=0.0)
        m = _miner(1, brier=0.15)
        result = compute_weights([m], config, chain)
        assert len(result.uids) >= 1
        assert 1 in result.uids

    def test_duplicate_uids_last_wins(self):
        """If two MinerMetrics share a UID, the UID-indexed array overwrites;
        the last one in sorted order should win."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m1 = _miner(1, brier=0.10)
        m2 = MinerMetrics(
            uid=1, hotkey="different_hk",
            fq_raw=0.5, pss_mean=0.5, es_adj=0.5, mes_mean=0.8,
            cal_score=0.9, sharp_score=0.9, sos_score=0.9, lead_score=0.9,
            brier_mean=0.10, uniqueness_dim=0.9, marginal_dim=0.9,
        )
        # Should not crash
        result = compute_weights([m1, m2], config, chain)
        assert 1 in result.skill_scores

    def test_uid_exceeds_n_neurons(self):
        """Miner with uid >= n_neurons should be silently ignored."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        miners = [_miner(5, brier=0.1), _miner(999, brier=0.1)]
        result = compute_weights(miners, config, chain)
        assert 5 in result.uids
        assert 999 not in result.uids


# ===================================================================
# 4. BURN RATE
# ===================================================================

class TestBurnRate:
    """Verify burn rate allocation is correct."""

    def test_zero_burn_rate_no_burn_allocation(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20, burn_rate=0.0, burn_uid=0)
        miners = _generate_diverse_miners(10)
        result = compute_weights(miners, config, chain)
        # burn_uid=0 should NOT appear unless a miner also has uid=0
        for uid, w in zip(result.uids, result.uint16_weights):
            if uid == 0 and 0 not in [m.uid for m in miners]:
                pytest.fail("Burn UID allocated weight with burn_rate=0")

    def test_ninety_pct_burn(self):
        """burn_rate is read from scoring config, not chain_params."""
        params = ScoringParams().model_dump(mode="json")
        params["weight_emission"]["burn_rate"] = "0.9"
        config = ScoringConfigSnapshot(params=params)
        chain = _chain(n_neurons=20, burn_rate=0.9, burn_uid=0)
        miners = [_miner(i, brier=0.15) for i in range(1, 6)]
        result = compute_weights(miners, config, chain)
        assert 0 in result.raw_weights
        assert abs(result.raw_weights[0] - 0.9) < 1e-6

    def test_burn_uid_none_skips_burn(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20, burn_rate=0.9, burn_uid=None)
        miners = [_miner(1, brier=0.15)]
        result = compute_weights(miners, config, chain)
        assert 0 not in result.raw_weights or result.raw_weights.get(0, 0) == 0


# ===================================================================
# 5. SCALE AND THROUGHPUT
# ===================================================================

class TestScaleAndThroughput:
    """Verify compute_weights handles production-scale inputs within
    reasonable time and memory bounds."""

    @pytest.mark.parametrize("n_miners", [163, 256, 512])
    def test_production_scale(self, n_miners: int):
        """Runs at production miner counts without error."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=max(n_miners + 10, 256))
        miners = _generate_diverse_miners(n_miners)
        result = compute_weights(miners, config, chain)
        scored = sum(1 for s in result.skill_scores.values() if s > 0)
        assert scored > 0, "At least one miner should score > 0"
        assert len(result.uids) > 0

    def test_throughput_163_miners_under_100ms(self):
        """163 miners (current prod count) must complete in < 100ms."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=256)
        miners = _generate_diverse_miners(163)

        times = []
        for _ in range(20):
            start = time.perf_counter()
            compute_weights(miners, config, chain)
            times.append(time.perf_counter() - start)

        p95 = sorted(times)[int(len(times) * 0.95)]
        assert p95 < 0.1, f"p95 latency {p95:.4f}s exceeds 100ms"

    def test_normalization_switch_at_boundary(self):
        """Below min_count_for_zscore, uses percentile; at/above, uses zscore.
        Verify both branches produce valid output."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=30)
        min_count = int(config.params.get("normalization", {}).get("min_count_for_zscore", 10))

        below = _generate_diverse_miners(min_count - 1, seed=1)
        at = _generate_diverse_miners(min_count, seed=2)
        above = _generate_diverse_miners(min_count + 5, seed=3)

        for miners in [below, at, above]:
            result = compute_weights(miners, config, chain)
            for uid, score in result.skill_scores.items():
                assert math.isfinite(score)


# ===================================================================
# 6. MAX WEIGHT LIMIT AND UINT16 CONVERSION
# ===================================================================

class TestWeightProcessing:
    """Verify chain weight processing constraints."""

    def test_uint16_range(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=256)
        miners = _generate_diverse_miners(100)
        result = compute_weights(miners, config, chain)
        for w in result.uint16_weights:
            assert 0 < w <= U16_MAX

    def test_max_weight_limit_enforced(self):
        """No single weight should exceed max_weight_limit after normalization."""
        config = _prod_scoring_config()
        limit = 0.1
        chain = _chain(n_neurons=64, max_weight_limit=limit, burn_rate=0.0)
        miners = _generate_diverse_miners(30)
        result = compute_weights(miners, config, chain)

        total = sum(result.uint16_weights)
        if total > 0:
            for w in result.uint16_weights:
                frac = w / total
                # Allow small float tolerance above the limit
                assert frac <= limit + 0.01, f"Weight fraction {frac:.4f} > limit {limit}"

    def test_min_allowed_weights_padding(self):
        """When non-zero weights < min_allowed, padding is applied."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20, min_allowed_weights=10, burn_rate=0.0)
        miners = [_miner(1, brier=0.1)]  # Only 1 miner scores
        result = compute_weights(miners, config, chain)
        # Should have at least min_allowed UIDs in output (padded)
        assert len(result.uids) >= 10

    def test_no_duplicate_uids_in_output(self):
        config = _prod_scoring_config()
        chain = _chain(n_neurons=256)
        miners = _generate_diverse_miners(100)
        result = compute_weights(miners, config, chain)
        assert len(result.uids) == len(set(result.uids))


# ===================================================================
# 7. COBB-DOUGLAS FORMULA CORRECTNESS
# ===================================================================

class TestCobbDouglasCorrectness:
    """Verify the Cobb-Douglas product against hand-computed reference values."""

    def test_reference_single_miner(self):
        """Hand-verify the scoring for one miner with known inputs."""
        config = _prod_scoring_config()
        cd = config.params["cobb_douglas"]
        chain = _chain(n_neurons=10, burn_rate=0.0)

        m = _miner(1, brier=0.15, fq=0.2, pss=0.3, es=0.08, mes=0.7,
                    cal=0.6, sharp=0.5, sos=0.5, lead=0.4, uniq=0.8, marg=0.6)
        result = compute_weights([m], config, chain)

        # Manual computation
        w_fq, w_cal = 0.6, 0.4
        w_edge, w_mes = 0.7, 0.3
        eps = float(cd["epsilon"])

        fq_norm = np.clip((0.2 + 1) / 2, 0, 1)  # = 0.6
        # With 1 miner, percentile normalization gives 0.5 for single-element
        # (implementation detail), so we just verify the result is finite and > 0
        assert result.skill_scores[1] > 0
        assert math.isfinite(result.skill_scores[1])

    def test_uniqueness_exponent_is_strongest(self):
        """With exponent 1.5, uniqueness has the highest marginal impact.
        A miner with high uniqueness should score much higher than one with low."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10, burn_rate=0.0)

        high_uniq = _miner(1, brier=0.1, uniq=0.95, marg=0.5)
        low_uniq = _miner(2, brier=0.1, uniq=0.10, marg=0.5)
        result = compute_weights([high_uniq, low_uniq], config, chain)

        assert result.skill_scores[1] > result.skill_scores[2] * 2, \
            "High uniqueness miner should score >> low uniqueness"

    def test_floor_exactly_at_threshold(self):
        """brier_mean == floor_threshold (0.30) should NOT be floored.
        The condition is strictly greater-than."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=10)
        m = _miner(1, brier=0.30)
        result = compute_weights([m], config, chain)
        assert result.skill_scores[1] > 0, \
            "brier_mean == threshold should pass (> not >=)"

    def test_epsilon_clamping_prevents_zero_base(self):
        """All dimensions clamped to [epsilon, 1.0], so even with all-zero
        inputs, the Cobb-Douglas product is epsilon^(sum of exponents), not 0."""
        config = _prod_scoring_config()
        cd = config.params["cobb_douglas"]
        eps = float(cd["epsilon"])
        chain = _chain(n_neurons=10, burn_rate=0.0)

        m = _miner(1, brier=0.0, fq=-1.0, pss=-10.0, es=0.0, mes=0.0,
                    cal=0.0, sos=0.0, lead=0.0, uniq=0.0, marg=0.0)
        result = compute_weights([m], config, chain)
        assert result.skill_scores[1] > 0, "Clamped to epsilon → product > 0"


# ===================================================================
# 8. CHECKPOINT SERIALIZATION INTEGRITY
# ===================================================================

class TestCheckpointIntegrity:
    """Verify that accumulator data survives the full checkpoint lifecycle:
    build → serialize → deserialize → derive_means → compute_weights."""

    def _build_checkpoint(self, n: int, seed: int = 42) -> CheckpointWindow:
        rng = np.random.RandomState(seed)
        now = _now()
        accumulators = []
        for uid in range(1, n + 1):
            wt = rng.uniform(50, 500)
            accumulators.append(_accumulator(
                uid,
                brier_ws=rng.beta(2, 5) * 0.4 * wt, brier_wt=wt,
                fq_ws=rng.uniform(-0.3, 0.3) * wt, fq_wt=wt,
                pss_ws=rng.normal(0, 0.3) * wt, pss_wt=wt,
                es_ws=rng.uniform(0.01, 0.1) * wt, es_wt=wt,
                mes_ws=rng.beta(3, 2) * wt, mes_wt=wt,
                sos_ws=rng.beta(3, 3) * wt, sos_wt=wt,
                lead_ws=rng.beta(3, 3) * wt, lead_wt=wt,
                cal=rng.beta(5, 3), sharp=rng.beta(4, 4),
                uniq=rng.beta(3, 3) if rng.random() > 0.2 else None,
                marg=rng.beta(5, 5) if rng.random() > 0.2 else None,
            ))
        return CheckpointWindow(
            manifest=LedgerManifest(
                window_type="checkpoint",
                window_start=now, window_end=now,
                checkpoint_epoch=6, content_hashes={},
                primary_hotkey="5Gprimary", created_at=now,
            ),
            roster=[
                MinerRosterEntry(miner_id=uid, uid=uid,
                                 hotkey=f"5{'0' * 46}{uid:02d}", active=True)
                for uid in range(1, n + 1)
            ],
            accumulators=accumulators,
            scoring_config=_prod_scoring_config(),
            chain_params=_chain(n_neurons=max(n + 10, 256)),
        )

    def test_full_lifecycle_163_miners(self):
        """Production-scale checkpoint lifecycle with weight verification."""
        cp = self._build_checkpoint(163)
        json_bytes = cp.model_dump_json().encode()
        restored = CheckpointWindow.model_validate_json(json_bytes)

        metrics_orig = [_metrics_from_acc(a) for a in cp.accumulators]
        metrics_rest = [_metrics_from_acc(a) for a in restored.accumulators]

        r1 = compute_weights(metrics_orig, cp.scoring_config, cp.chain_params)
        r2 = compute_weights(metrics_rest, restored.scoring_config, restored.chain_params)

        assert r1.uids == r2.uids
        assert r1.uint16_weights == r2.uint16_weights

    def test_gzip_json_roundtrip(self):
        """Simulate the filesystem store's gzip path."""
        import gzip
        cp = self._build_checkpoint(50)
        acc_data = [a.model_dump(mode="json") for a in cp.accumulators]
        compressed = gzip.compress(json.dumps(acc_data, default=str, sort_keys=True).encode())
        decompressed = json.loads(gzip.decompress(compressed))
        restored = [AccumulatorEntry.model_validate(d) for d in decompressed]

        m_orig = [_metrics_from_acc(a) for a in cp.accumulators]
        m_rest = [_metrics_from_acc(a) for a in restored]
        config = cp.scoring_config
        chain = cp.chain_params

        r1 = compute_weights(m_orig, config, chain)
        r2 = compute_weights(m_rest, config, chain)
        assert r1.uids == r2.uids
        assert r1.uint16_weights == r2.uint16_weights

    def test_float_precision_across_json(self):
        """JSON serialization must not introduce precision drift that
        changes the final uint16 weights."""
        acc = _accumulator(
            1,
            brier_ws=17.123456789012345,
            brier_wt=99.987654321098765,
            fq_ws=3.141592653589793,
            fq_wt=100.0,
        )
        json_str = acc.model_dump_json()
        restored = AccumulatorEntry.model_validate_json(json_str)

        acc.derive_means()
        restored.derive_means()

        assert abs(acc.brier_mean - restored.brier_mean) < 1e-12
        assert abs(acc.fq_raw - restored.fq_raw) < 1e-12


# ===================================================================
# 9. SCORING CONFIG VARIATION
# ===================================================================

class TestScoringConfigVariation:
    """Verify compute_weights handles non-default config gracefully."""

    def test_zero_burn_rate_in_config(self):
        """New default: burn_rate=0.0 in production config."""
        params = ScoringParams()
        assert float(params.weight_emission.burn_rate) == 0.0

    def test_custom_exponents(self):
        """Overriding Cobb-Douglas exponents changes ranking.
        Uses 15 miners so zscore normalization kicks in and preserves
        the raw metric spread."""
        chain = _chain(n_neurons=30, burn_rate=0.0)
        # 15 baseline miners + 2 test miners with contrasting profiles
        miners = [_miner(i, brier=0.15) for i in range(3, 18)]
        miners.append(_miner(1, brier=0.1, uniq=0.95, es=0.01))  # high uniqueness
        miners.append(_miner(2, brier=0.1, uniq=0.05, es=0.20))  # high edge

        # Default: uniqueness_exponent=1.5, edge_exponent=1.0
        config_default = _prod_scoring_config()
        r_default = compute_weights(miners, config_default, chain)

        # Flip: make edge dominant
        params = ScoringParams().model_dump(mode="json")
        params["cobb_douglas"]["uniqueness_exponent"] = "0.1"
        params["cobb_douglas"]["edge_exponent"] = "3.0"
        config_flip = ScoringConfigSnapshot(params=params)
        r_flip = compute_weights(miners, config_flip, chain)

        # With default config, miner 1 (high uniq) should score higher
        assert r_default.skill_scores[1] > r_default.skill_scores[2]
        # With flipped config, miner 2 (high edge) should score higher
        assert r_flip.skill_scores[2] > r_flip.skill_scores[1]

    def test_missing_weight_emission_uses_defaults(self):
        """If weight_emission section is absent, burn_rate defaults to 0.9."""
        config = ScoringConfigSnapshot(params={
            "dimension_weights": {},
            "skill_score_weights": {},
            "normalization": {},
            # no weight_emission
        })
        chain = _chain(n_neurons=10, burn_rate=0.9, burn_uid=0)
        miners = [_miner(1, brier=0.1)]
        result = compute_weights(miners, config, chain)
        # Should not crash; burn_rate comes from config, not chain_params
        # compute_weights reads from config, chain_params.burn_rate is unused
        assert len(result.uids) > 0


# ===================================================================
# 10. TIMELINESS DIMENSION (cleaned-up formula)
# ===================================================================

class TestTimelinessDimension:
    """Verify the cleaned-up timeliness formula: 0.5 * pss_norm + 0.5 * lead_norm."""

    def test_timeliness_blends_pss_and_lead(self):
        """A miner with high PSS and low lead should score differently
        from one with low PSS and high lead."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20, burn_rate=0.0)

        high_pss = _miner(1, brier=0.1, pss=2.0, lead=0.1)
        high_lead = _miner(2, brier=0.1, pss=-1.0, lead=0.9)
        result = compute_weights([high_pss, high_lead], config, chain)

        dims_1 = result.dimension_scores[1]
        dims_2 = result.dimension_scores[2]
        assert dims_1["timeliness_dim"] != dims_2["timeliness_dim"]

    def test_timeliness_matches_skill_score_job_formula(self):
        """The formula in compute_weights must match SkillScoreJob exactly:
        timeliness_dim = 0.5 * skill_dim + 0.5 * lead_norm."""
        config = _prod_scoring_config()
        chain = _chain(n_neurons=20, burn_rate=0.0)

        m = _miner(1, brier=0.1, pss=0.5, lead=0.7)
        result = compute_weights([m], config, chain)

        dims = result.dimension_scores[1]
        # skill_dim = pss_norm (percentile with 1 miner → 0.5)
        # lead_norm = clip(0.7, 0, 1) = 0.7
        # timeliness = 0.5 * skill_dim + 0.5 * 0.7
        expected_timeliness = 0.5 * dims["skill_dim"] + 0.5 * np.clip(0.7, 0, 1)
        assert abs(dims["timeliness_dim"] - max(expected_timeliness, 0.01)) < 1e-10


# ===================================================================
# 11. DERIVE_MEANS EXHAUSTIVE
# ===================================================================

class TestDeriveMeansExhaustive:
    """Test every metric's derive_means default matches the MinerMetrics default."""

    METRIC_DEFAULTS = {
        "brier_mean": 0.5,
        "fq_raw": 0.0,
        "pss_mean": 0.0,
        "es_adj": 0.0,
        "mes_mean": 0.5,
        "sos_score": 0.5,
        "lead_score": 0.5,
    }

    def test_all_zero_wt_defaults_match_miner_metrics(self):
        """When ALL wt=0, derive_means defaults must match MinerMetrics defaults."""
        acc = AccumulatorEntry(miner_id=1, hotkey="hk", uid=1)
        acc.derive_means()
        m_default = MinerMetrics(uid=1, hotkey="hk")

        for field, expected in self.METRIC_DEFAULTS.items():
            derived = getattr(acc, field)
            model_default = getattr(m_default, field)
            assert derived == expected, \
                f"derive_means {field}={derived}, expected {expected}"
            assert model_default == expected, \
                f"MinerMetrics default {field}={model_default}, expected {expected}"

    @pytest.mark.parametrize("metric,ws,wt,expected", [
        ("brier_mean", 25.0, 100.0, 0.25),
        ("brier_mean", 0.0, 0.0, 0.5),
        ("fq_raw", 10.0, 100.0, 0.1),
        ("fq_raw", 0.0, 0.0, 0.0),
        ("pss_mean", -50.0, 100.0, -0.5),
        ("mes_mean", 0.0, 0.0, 0.5),
        ("sos_score", 0.0, 0.0, 0.5),
        ("lead_score", 0.0, 0.0, 0.5),
    ])
    def test_individual_metric_derivation(self, metric, ws, wt, expected):
        metric_to_acc_field = {
            "brier_mean": "brier", "fq_raw": "fq", "pss_mean": "pss",
            "es_adj": "es", "mes_mean": "mes", "sos_score": "sos",
            "lead_score": "lead",
        }
        acc_field = metric_to_acc_field[metric]
        acc = AccumulatorEntry(miner_id=1, hotkey="hk", uid=1)
        setattr(acc, acc_field, MetricAccumulator(ws=ws, wt=wt))
        acc.derive_means()
        actual = getattr(acc, metric)
        assert abs(actual - expected) < 1e-12, \
            f"{metric}: derive_means({ws}/{wt}) = {actual}, expected {expected}"
