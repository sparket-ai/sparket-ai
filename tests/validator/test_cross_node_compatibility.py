"""Cross-node compatibility tests for the v0.1.0 release.

Validates that validators, auditors, and miners can all interoperate
correctly after the Shapley scoring overhaul + observability merge.

Covers:
- Checkpoint serialization round-trip (export → serialize → deserialize → verify)
- Backward compatibility (new code reading old checkpoints without Shapley fields)
- Auditor weight equivalence (auditor path produces identical weights to primary)
- Schema version enforcement
- MinerMetrics field propagation through AccumulatorEntry
- Protocol layer compatibility (synapse types unchanged)
- Scoring config completeness (Cobb-Douglas params present)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import numpy as np
import pytest

from sparket.validator.ledger.models import (
    LEDGER_SCHEMA_VERSION,
    AccumulatorEntry,
    CheckpointWindow,
    ChainParamsSnapshot,
    DeltaWindow,
    LedgerManifest,
    MetricAccumulator,
    MinerMetrics,
    MinerRosterEntry,
    ScoringConfigSnapshot,
)
from sparket.validator.ledger.compute_weights import compute_weights, WeightResult
from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _now():
    return datetime.now(timezone.utc)


def _default_scoring_config() -> ScoringConfigSnapshot:
    return ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))


def _default_chain(n_neurons: int = 20) -> ChainParamsSnapshot:
    return ChainParamsSnapshot(
        burn_rate=0.9, burn_uid=0,
        max_weight_limit=0.5, min_allowed_weights=1,
        n_neurons=n_neurons,
    )


def _make_accumulator(uid: int, uniqueness_dim=None, marginal_dim=None) -> AccumulatorEntry:
    return AccumulatorEntry(
        miner_id=uid, hotkey=f"hk_{uid}", uid=uid,
        n_submissions=50,
        brier=MetricAccumulator(ws=10.0 + uid, wt=50.0),
        fq=MetricAccumulator(ws=25.0 + uid, wt=50.0),
        pss=MetricAccumulator(ws=5.0 + uid, wt=50.0),
        es=MetricAccumulator(ws=2.0 + uid * 0.1, wt=50.0),
        mes=MetricAccumulator(ws=30.0, wt=50.0),
        sos=MetricAccumulator(ws=25.0, wt=50.0),
        lead=MetricAccumulator(ws=20.0, wt=50.0),
        cal_score=0.7, sharp_score=0.6,
        uniqueness_dim=uniqueness_dim,
        marginal_dim=marginal_dim,
    )


def _make_checkpoint(
    n_miners: int = 5,
    uniqueness_dim=None,
    marginal_dim=None,
) -> CheckpointWindow:
    now = _now()
    accumulators = [
        _make_accumulator(i, uniqueness_dim=uniqueness_dim, marginal_dim=marginal_dim)
        for i in range(1, n_miners + 1)
    ]
    return CheckpointWindow(
        manifest=LedgerManifest(
            window_type="checkpoint",
            window_start=now, window_end=now,
            checkpoint_epoch=1,
            content_hashes={},
            primary_hotkey="primary_hk",
            created_at=now,
        ),
        roster=[
            MinerRosterEntry(miner_id=i, uid=i, hotkey=f"hk_{i}", active=True)
            for i in range(1, n_miners + 1)
        ],
        accumulators=accumulators,
        scoring_config=_default_scoring_config(),
        chain_params=_default_chain(),
    )


# ---------------------------------------------------------------------------
# 1. Checkpoint serialization round-trip
# ---------------------------------------------------------------------------

class TestCheckpointSerialization:
    """Verify checkpoints survive JSON round-trip with all fields intact."""

    def test_round_trip_with_shapley_fields(self):
        """Checkpoint with uniqueness/marginal survives serialize → deserialize."""
        cp = _make_checkpoint(n_miners=3, uniqueness_dim=0.8, marginal_dim=0.7)
        json_str = cp.model_dump_json()
        restored = CheckpointWindow.model_validate_json(json_str)

        for orig, rest in zip(cp.accumulators, restored.accumulators):
            assert rest.uniqueness_dim == orig.uniqueness_dim
            assert rest.marginal_dim == orig.marginal_dim

    def test_round_trip_without_shapley_fields(self):
        """Checkpoint without uniqueness/marginal defaults to None."""
        cp = _make_checkpoint(n_miners=3)
        json_str = cp.model_dump_json()
        restored = CheckpointWindow.model_validate_json(json_str)

        for acc in restored.accumulators:
            assert acc.uniqueness_dim is None
            assert acc.marginal_dim is None

    def test_old_checkpoint_json_missing_new_fields(self):
        """Simulates loading a v0.0.6 checkpoint that lacks Shapley fields."""
        cp = _make_checkpoint(n_miners=2)
        data = cp.model_dump(mode="json")

        # Strip new fields to simulate old format
        for acc_data in data["accumulators"]:
            acc_data.pop("uniqueness_dim", None)
            acc_data.pop("marginal_dim", None)

        # Should deserialize without error
        restored = CheckpointWindow.model_validate(data)
        for acc in restored.accumulators:
            assert acc.uniqueness_dim is None
            assert acc.marginal_dim is None

    def test_extra_unknown_fields_ignored(self):
        """Future fields in checkpoint JSON should be silently ignored."""
        acc = _make_accumulator(1)
        data = acc.model_dump(mode="json")
        data["some_future_field"] = 42.0
        data["another_future_field"] = "test"

        restored = AccumulatorEntry.model_validate(data)
        assert restored.uid == 1
        assert not hasattr(restored, "some_future_field")


# ---------------------------------------------------------------------------
# 2. Schema version
# ---------------------------------------------------------------------------

class TestSchemaVersion:
    """Verify schema version is correctly bumped and enforced."""

    def test_schema_version_is_2(self):
        """v0.1.0 must use schema version 2."""
        assert LEDGER_SCHEMA_VERSION == 2

    def test_manifest_default_uses_current_version(self):
        """New manifests should default to the current schema version."""
        manifest = LedgerManifest(
            window_type="checkpoint",
            window_start=_now(), window_end=_now(),
            checkpoint_epoch=1, content_hashes={},
            primary_hotkey="hk", created_at=_now(),
        )
        assert manifest.schema_version == 2


# ---------------------------------------------------------------------------
# 3. Auditor weight equivalence
# ---------------------------------------------------------------------------

class TestAuditorWeightEquivalence:
    """Verify the auditor path (from_accumulator → compute_weights) produces
    identical weights to the primary path (direct MinerMetrics → compute_weights)."""

    def test_auditor_matches_primary_with_shapley_values(self):
        """When Shapley dims are populated, auditor and primary get same weights."""
        config = _default_scoring_config()
        chain = _default_chain(n_neurons=20)

        # Primary path: direct MinerMetrics construction
        primary_metrics = []
        for i in range(1, 6):
            m = MinerMetrics(
                uid=i, hotkey=f"hk_{i}",
                fq_raw=0.5 + i * 0.02, pss_mean=0.1 + i * 0.01,
                es_adj=0.05 + i * 0.01, mes_mean=0.6, cal_score=0.7,
                sharp_score=0.6, sos_score=0.5, lead_score=0.4,
                brier_mean=0.15,
                uniqueness_dim=0.7 + i * 0.02,
                marginal_dim=0.6 + i * 0.01,
            )
            primary_metrics.append(m)
        primary_result = compute_weights(primary_metrics, config, chain)

        # Auditor path: AccumulatorEntry → from_accumulator → MinerMetrics
        accumulators = []
        for i in range(1, 6):
            acc = AccumulatorEntry(
                miner_id=i, hotkey=f"hk_{i}", uid=i,
                n_submissions=100,
                brier=MetricAccumulator(ws=0.15 * 100, wt=100),
                fq=MetricAccumulator(ws=(0.5 + i * 0.02) * 100, wt=100),
                pss=MetricAccumulator(ws=(0.1 + i * 0.01) * 100, wt=100),
                es=MetricAccumulator(ws=(0.05 + i * 0.01) * 100, wt=100),
                mes=MetricAccumulator(ws=0.6 * 100, wt=100),
                sos=MetricAccumulator(ws=0.5 * 100, wt=100),
                lead=MetricAccumulator(ws=0.4 * 100, wt=100),
                cal_score=0.7, sharp_score=0.6,
                uniqueness_dim=0.7 + i * 0.02,
                marginal_dim=0.6 + i * 0.01,
            )
            accumulators.append(acc)

        auditor_metrics = [MinerMetrics.from_accumulator(acc) for acc in accumulators]
        auditor_result = compute_weights(auditor_metrics, config, chain)

        # Both must produce identical weight outputs
        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights

    def test_auditor_matches_primary_without_shapley(self):
        """When Shapley dims are None, both paths use same fallbacks."""
        config = _default_scoring_config()
        chain = _default_chain(n_neurons=20)

        # Primary: MinerMetrics with None dims
        primary_metrics = []
        for i in range(1, 6):
            m = MinerMetrics(
                uid=i, hotkey=f"hk_{i}",
                fq_raw=0.4, pss_mean=0.1, es_adj=0.05,
                mes_mean=0.6, cal_score=0.7, sharp_score=0.6,
                sos_score=0.5, lead_score=0.4, brier_mean=0.18,
                uniqueness_dim=None, marginal_dim=None,
            )
            primary_metrics.append(m)
        primary_result = compute_weights(primary_metrics, config, chain)

        # Auditor: from checkpoint without Shapley fields
        accumulators = []
        for i in range(1, 6):
            acc = AccumulatorEntry(
                miner_id=i, hotkey=f"hk_{i}", uid=i, n_submissions=100,
                brier=MetricAccumulator(ws=0.18 * 100, wt=100),
                fq=MetricAccumulator(ws=0.4 * 100, wt=100),
                pss=MetricAccumulator(ws=0.1 * 100, wt=100),
                es=MetricAccumulator(ws=0.05 * 100, wt=100),
                mes=MetricAccumulator(ws=0.6 * 100, wt=100),
                sos=MetricAccumulator(ws=0.5 * 100, wt=100),
                lead=MetricAccumulator(ws=0.4 * 100, wt=100),
                cal_score=0.7, sharp_score=0.6,
                # No uniqueness_dim or marginal_dim — defaults to None
            )
            accumulators.append(acc)

        auditor_metrics = [MinerMetrics.from_accumulator(acc) for acc in accumulators]
        auditor_result = compute_weights(auditor_metrics, config, chain)

        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights

    def test_weight_determinism_multiple_runs(self):
        """Same inputs must produce byte-identical outputs across 10 runs."""
        cp = _make_checkpoint(n_miners=10, uniqueness_dim=0.7, marginal_dim=0.6)
        metrics = [MinerMetrics.from_accumulator(acc) for acc in cp.accumulators]

        results = []
        for _ in range(10):
            r = compute_weights(metrics, cp.scoring_config, cp.chain_params)
            results.append((r.uids, r.uint16_weights))

        for uids, weights in results[1:]:
            assert uids == results[0][0]
            assert weights == results[0][1]


# ---------------------------------------------------------------------------
# 4. MinerMetrics field propagation
# ---------------------------------------------------------------------------

class TestFieldPropagation:
    """Verify all fields propagate through AccumulatorEntry → MinerMetrics."""

    def test_all_base_fields_propagate(self):
        """Standard scoring fields are correctly derived from accumulators."""
        acc = _make_accumulator(1)
        acc.derive_means()
        m = MinerMetrics.from_accumulator(acc)

        assert m.uid == 1
        assert m.hotkey == "hk_1"
        assert abs(m.brier_mean - acc.brier.ws / acc.brier.wt) < 1e-10
        assert abs(m.fq_raw - acc.fq.ws / acc.fq.wt) < 1e-10
        assert abs(m.pss_mean - acc.pss.ws / acc.pss.wt) < 1e-10

    def test_shapley_fields_propagate_when_set(self):
        """uniqueness_dim and marginal_dim pass through from_accumulator."""
        acc = _make_accumulator(1, uniqueness_dim=0.82, marginal_dim=0.71)
        m = MinerMetrics.from_accumulator(acc)

        assert m.uniqueness_dim == 0.82
        assert m.marginal_dim == 0.71

    def test_shapley_fields_none_when_unset(self):
        """Fields default to None when not populated."""
        acc = _make_accumulator(1)
        m = MinerMetrics.from_accumulator(acc)

        assert m.uniqueness_dim is None
        assert m.marginal_dim is None

    def test_compute_weights_uses_explicit_over_fallback(self):
        """When uniqueness_dim is set, it's used instead of sos_score fallback."""
        config = _default_scoring_config()
        chain = _default_chain(n_neurons=20)

        # With explicit uniqueness_dim
        m_explicit = [MinerMetrics(
            uid=1, hotkey="hk_1", fq_raw=0.5, pss_mean=0.2,
            es_adj=0.1, mes_mean=0.6, cal_score=0.7, sharp_score=0.6,
            sos_score=0.5, lead_score=0.4, brier_mean=0.15,
            uniqueness_dim=0.9, marginal_dim=0.8,
        )]
        r_explicit = compute_weights(m_explicit, config, chain)

        # With None (falls back to sos_score=0.5 and 0.5)
        m_fallback = [MinerMetrics(
            uid=1, hotkey="hk_1", fq_raw=0.5, pss_mean=0.2,
            es_adj=0.1, mes_mean=0.6, cal_score=0.7, sharp_score=0.6,
            sos_score=0.5, lead_score=0.4, brier_mean=0.15,
            uniqueness_dim=None, marginal_dim=None,
        )]
        r_fallback = compute_weights(m_fallback, config, chain)

        # Explicit 0.9 uniqueness should score higher than fallback 0.5
        assert r_explicit.skill_scores[1] > r_fallback.skill_scores[1]


# ---------------------------------------------------------------------------
# 5. Scoring config completeness
# ---------------------------------------------------------------------------

class TestScoringConfigCompleteness:
    """Verify all Cobb-Douglas params exist and have valid defaults."""

    def test_cobb_douglas_params_present(self):
        """ScoringParams must include all 5 Cobb-Douglas exponents."""
        params = get_scoring_params()
        dumped = params.model_dump(mode="json")

        cd = dumped.get("cobb_douglas")
        assert cd is not None, "cobb_douglas section missing from scoring params"

        required_keys = [
            "accuracy_exponent", "edge_exponent", "timeliness_exponent",
            "uniqueness_exponent", "marginal_exponent",
            "floor_threshold", "epsilon",
        ]
        for key in required_keys:
            assert key in cd, f"Missing cobb_douglas param: {key}"
            assert float(cd[key]) > 0, f"cobb_douglas.{key} must be positive"

    def test_scoring_config_snapshot_preserves_cobb_douglas(self):
        """ScoringConfigSnapshot round-trip keeps Cobb-Douglas params."""
        params = get_scoring_params()
        snapshot = ScoringConfigSnapshot(params=params.model_dump(mode="json"))

        cd = snapshot.params.get("cobb_douglas", {})
        assert float(cd["accuracy_exponent"]) == 0.5
        assert float(cd["edge_exponent"]) == 1.0
        assert float(cd["uniqueness_exponent"]) == 1.5

    def test_compute_weights_handles_missing_cobb_douglas_gracefully(self):
        """If cobb_douglas section is absent, defaults are used."""
        config = ScoringConfigSnapshot(params={
            "dimension_weights": {"w_fq": 0.6, "w_cal": 0.4, "w_edge": 0.7, "w_mes": 0.3, "w_sos": 0.6, "w_lead": 0.4},
            "skill_score_weights": {"w_outcome_accuracy": 0.10, "w_outcome_relative": 0.10, "w_odds_edge": 0.50, "w_info_adv": 0.30},
            "normalization": {"min_count_for_zscore": 10},
            # No cobb_douglas section
        })
        chain = _default_chain()
        metrics = [MinerMetrics(
            uid=1, hotkey="hk_1", fq_raw=0.5, pss_mean=0.2,
            es_adj=0.1, mes_mean=0.6, cal_score=0.7, sharp_score=0.6,
            sos_score=0.5, lead_score=0.4, brier_mean=0.15,
        )]

        # Should not crash — uses hardcoded defaults
        result = compute_weights(metrics, config, chain)
        assert result.skill_scores[1] > 0


# ---------------------------------------------------------------------------
# 6. Protocol layer compatibility
# ---------------------------------------------------------------------------

class TestProtocolCompatibility:
    """Verify the miner↔validator protocol is unchanged."""

    def test_synapse_types_unchanged(self):
        """All expected synapse types must exist."""
        from sparket.protocol.protocol import SparketSynapseType

        expected = {"ODDS_PUSH", "OUTCOME_PUSH", "GAME_DATA_REQUEST"}
        actual = {e.name for e in SparketSynapseType}
        assert expected.issubset(actual), f"Missing synapse types: {expected - actual}"

    def test_synapse_creation_unchanged(self):
        """SparketSynapse can be created with standard fields."""
        from sparket.protocol.protocol import SparketSynapse, SparketSynapseType

        synapse = SparketSynapse(
            type=SparketSynapseType.ODDS_PUSH,
            payload={"submissions": []},
        )
        assert synapse.type == SparketSynapseType.ODDS_PUSH
        assert synapse.payload == {"submissions": []}


# ---------------------------------------------------------------------------
# 7. Brier floor consistency
# ---------------------------------------------------------------------------

class TestBrierFloorConsistency:
    """Verify the hard accuracy floor is consistent across paths."""

    def test_floor_applied_identically(self):
        """Miners above Brier threshold get zero weight in both paths."""
        config = _default_scoring_config()
        chain = _default_chain(n_neurons=20)

        # Mix of good and bad miners
        metrics = []
        for i in range(1, 8):
            brier = 0.10 if i <= 4 else 0.35  # 4 good, 3 floored
            metrics.append(MinerMetrics(
                uid=i, hotkey=f"hk_{i}",
                fq_raw=0.5, pss_mean=0.2, es_adj=0.1, mes_mean=0.6,
                cal_score=0.7, sharp_score=0.6, sos_score=0.5,
                lead_score=0.4, brier_mean=brier,
                uniqueness_dim=0.7, marginal_dim=0.6,
            ))

        result = compute_weights(metrics, config, chain)

        for i in range(1, 8):
            if i <= 4:
                assert result.skill_scores[i] > 0, f"Good miner {i} should score > 0"
            else:
                assert result.skill_scores[i] == 0.0, f"Bad miner {i} should be floored"

    def test_all_floored_allocates_to_burn(self):
        """If all miners are above Brier threshold, 100% goes to burn UID."""
        config = _default_scoring_config()
        chain = _default_chain(n_neurons=20)

        metrics = [
            MinerMetrics(
                uid=i, hotkey=f"hk_{i}",
                fq_raw=0.5, pss_mean=0.2, es_adj=0.1, mes_mean=0.6,
                cal_score=0.7, sharp_score=0.6, sos_score=0.5,
                lead_score=0.4, brier_mean=0.40,  # All above floor
            )
            for i in range(1, 4)
        ]

        result = compute_weights(metrics, config, chain)

        # All skill scores should be zero
        for i in range(1, 4):
            assert result.skill_scores[i] == 0.0

        # Burn UID should get all weight
        assert chain.burn_uid in result.uids
