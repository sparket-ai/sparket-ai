"""Tests for ledger Pydantic models."""

import pytest
from datetime import datetime, timezone

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
    OutcomeEntry,
    RecomputeReasonCode,
    RecomputeRecord,
    ScoringConfigSnapshot,
    SettledSubmissionEntry,
)


class TestLedgerManifest:

    def test_serialization_roundtrip(self):
        manifest = LedgerManifest(
            window_type="checkpoint",
            window_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            window_end=datetime(2024, 1, 2, tzinfo=timezone.utc),
            checkpoint_epoch=3,
            content_hashes={"roster": "abc123", "accumulators": "def456"},
            primary_hotkey="5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
            created_at=datetime(2024, 1, 2, 1, 0, tzinfo=timezone.utc),
        )
        data = manifest.model_dump(mode="json")
        restored = LedgerManifest(**data)
        assert restored.window_type == "checkpoint"
        assert restored.checkpoint_epoch == 3
        assert restored.content_hashes["roster"] == "abc123"

    def test_schema_version_present(self):
        manifest = LedgerManifest(
            window_type="delta",
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            checkpoint_epoch=1,
            content_hashes={"x": "y"},
            primary_hotkey="test",
            created_at=datetime.now(timezone.utc),
        )
        assert manifest.schema_version == LEDGER_SCHEMA_VERSION

    def test_content_hashes_required(self):
        """Manifests can have empty content_hashes (valid for empty exports)."""
        manifest = LedgerManifest(
            window_type="checkpoint",
            window_start=datetime.now(timezone.utc),
            window_end=datetime.now(timezone.utc),
            checkpoint_epoch=1,
            content_hashes={},
            primary_hotkey="test",
            created_at=datetime.now(timezone.utc),
        )
        assert manifest.content_hashes == {}


class TestAccumulatorEntry:

    def test_from_db_row(self):
        acc = AccumulatorEntry(
            miner_id=1,
            hotkey="5abc",
            uid=3,
            n_submissions=100,
            brier=MetricAccumulator(ws=20.0, wt=100.0),
            fq=MetricAccumulator(ws=60.0, wt=100.0),
        )
        assert acc.miner_id == 1
        assert acc.brier.ws == 20.0
        assert acc.brier.wt == 100.0

    def test_nan_handling(self):
        acc = AccumulatorEntry(
            miner_id=1, hotkey="5abc", uid=3,
            brier=MetricAccumulator(ws=float('nan'), wt=0.0),
        )
        acc.derive_means()
        assert acc.brier_mean == 0.5  # wt=0 -> fallback (0.5 = unknown accuracy)

    def test_derived_means_match_ws_wt(self):
        acc = AccumulatorEntry(
            miner_id=1, hotkey="5abc", uid=3,
            brier=MetricAccumulator(ws=5.0, wt=25.0),
            fq=MetricAccumulator(ws=15.0, wt=25.0),
            pss=MetricAccumulator(ws=2.5, wt=25.0),
            es=MetricAccumulator(ws=1.0, wt=10.0),
            mes=MetricAccumulator(ws=7.0, wt=10.0),
            sos=MetricAccumulator(ws=6.0, wt=10.0),
            lead=MetricAccumulator(ws=4.0, wt=10.0),
        )
        acc.derive_means()
        assert abs(acc.brier_mean - 0.2) < 1e-9
        assert abs(acc.fq_raw - 0.6) < 1e-9
        assert abs(acc.pss_mean - 0.1) < 1e-9
        assert abs(acc.es_adj - 0.1) < 1e-9
        assert abs(acc.mes_mean - 0.7) < 1e-9
        assert abs(acc.sos_score - 0.6) < 1e-9
        assert abs(acc.lead_score - 0.4) < 1e-9

    def test_zero_weight_sum_uses_fallback(self):
        acc = AccumulatorEntry(
            miner_id=1, hotkey="5abc", uid=3,
            brier=MetricAccumulator(ws=0, wt=0),
            fq=MetricAccumulator(ws=0, wt=0),
            pss=MetricAccumulator(ws=0, wt=0),
            mes=MetricAccumulator(ws=0, wt=0),
            sos=MetricAccumulator(ws=0, wt=0),
            lead=MetricAccumulator(ws=0, wt=0),
        )
        acc.derive_means()
        assert acc.brier_mean == 0.5  # default: unknown accuracy
        assert acc.fq_raw == 0.0
        assert acc.pss_mean == 0.0
        assert acc.mes_mean == 0.5  # default for mes
        assert acc.sos_score == 0.5  # default for sos
        assert acc.lead_score == 0.5  # default for lead


class TestCheckpointWindow:

    def test_contains_all_sections(self):
        now = datetime.now(timezone.utc)
        cp = CheckpointWindow(
            manifest=LedgerManifest(
                window_type="checkpoint",
                window_start=now, window_end=now,
                checkpoint_epoch=1, content_hashes={},
                primary_hotkey="test", created_at=now,
            ),
            roster=[MinerRosterEntry(miner_id=1, uid=1, hotkey="a", active=True)],
            accumulators=[AccumulatorEntry(miner_id=1, hotkey="a", uid=1)],
            scoring_config=ScoringConfigSnapshot(params={"test": True}),
            chain_params=ChainParamsSnapshot(
                burn_rate=0.9, burn_uid=0,
                max_weight_limit=0.1, min_allowed_weights=1, n_neurons=10,
            ),
        )
        assert len(cp.roster) == 1
        assert len(cp.accumulators) == 1
        assert cp.scoring_config.params["test"] is True
        assert cp.chain_params.burn_rate == 0.9

    def test_checkpoint_epoch_present(self):
        now = datetime.now(timezone.utc)
        cp = CheckpointWindow(
            manifest=LedgerManifest(
                window_type="checkpoint",
                window_start=now, window_end=now,
                checkpoint_epoch=5, content_hashes={},
                primary_hotkey="test", created_at=now,
            ),
        )
        assert cp.manifest.checkpoint_epoch == 5


class TestDeltaWindow:

    def test_contains_settled_sections(self):
        now = datetime.now(timezone.utc)
        delta = DeltaWindow(
            manifest=LedgerManifest(
                window_type="delta",
                window_start=now, window_end=now,
                checkpoint_epoch=1, content_hashes={},
                primary_hotkey="test", created_at=now,
            ),
            settled_submissions=[
                SettledSubmissionEntry(
                    miner_id=1, market_id=10, side="home",
                    imp_prob=0.55, brier=0.2, settled_at=now,
                )
            ],
            settled_outcomes=[
                OutcomeEntry(
                    market_id=10, event_id=100, result="home",
                    score_home=3, score_away=1, settled_at=now,
                )
            ],
        )
        assert len(delta.settled_submissions) == 1
        assert len(delta.settled_outcomes) == 1

    def test_epoch_reference_present(self):
        now = datetime.now(timezone.utc)
        delta = DeltaWindow(
            manifest=LedgerManifest(
                window_type="delta",
                window_start=now, window_end=now,
                checkpoint_epoch=7, content_hashes={},
                primary_hotkey="test", created_at=now,
            ),
        )
        assert delta.manifest.checkpoint_epoch == 7


class TestMinerMetrics:

    def test_from_accumulator(self):
        acc = AccumulatorEntry(
            miner_id=1, hotkey="5abc", uid=3,
            brier=MetricAccumulator(ws=5.0, wt=25.0),
            fq=MetricAccumulator(ws=15.0, wt=25.0),
            cal_score=0.8,
            sharp_score=0.7,
        )
        metrics = MinerMetrics.from_accumulator(acc)
        assert metrics.uid == 3
        assert metrics.hotkey == "5abc"
        assert abs(metrics.brier_mean - 0.2) < 1e-9
        assert abs(metrics.fq_raw - 0.6) < 1e-9
        assert metrics.cal_score == 0.8
        assert metrics.sharp_score == 0.7


class TestRecomputeRecord:

    def test_serialization_roundtrip(self):
        record = RecomputeRecord(
            epoch=5,
            previous_epoch=4,
            reason_code=RecomputeReasonCode.SCORING_BUG,
            reason_detail="Fixed rounding error in Brier computation",
            affected_event_ids=[100, 200],
            severity="bugfix",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            code_version="abc1234",
        )
        data = record.model_dump(mode="json")
        restored = RecomputeRecord(**data)
        assert restored.epoch == 5
        assert restored.reason_code == RecomputeReasonCode.SCORING_BUG
        assert len(restored.affected_event_ids) == 2

    def test_requires_reason_detail(self):
        with pytest.raises(Exception):
            RecomputeRecord(
                epoch=2, previous_epoch=1,
                reason_code=RecomputeReasonCode.SCORING_BUG,
                reason_detail="",  # empty - should fail
                severity="bugfix",
                timestamp=datetime.now(timezone.utc),
                code_version="abc",
            )

    def test_all_reason_codes_valid(self):
        for code in RecomputeReasonCode:
            record = RecomputeRecord(
                epoch=2, previous_epoch=1,
                reason_code=code,
                reason_detail="test",
                severity="correction",
                timestamp=datetime.now(timezone.utc),
                code_version="abc",
            )
            assert record.reason_code == code
