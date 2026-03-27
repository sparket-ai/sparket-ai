"""Tests for the weight verification auditor plugin."""

import pytest
from datetime import datetime, timezone

from sparket.validator.auditor.plugin_registry import AuditorContext, TaskResult
from sparket.validator.auditor.plugins.weight_verification import WeightVerificationHandler
from sparket.validator.ledger.models import (
    AccumulatorEntry,
    CheckpointWindow,
    ChainParamsSnapshot,
    DeltaWindow,
    LedgerManifest,
    MetricAccumulator,
    MinerRosterEntry,
    OutcomeEntry,
    ScoringConfigSnapshot,
    SettledSubmissionEntry,
)


def _make_checkpoint(n_miners: int = 3) -> CheckpointWindow:
    now = datetime.now(timezone.utc)
    accumulators = []
    for i in range(1, n_miners + 1):
        accumulators.append(AccumulatorEntry(
            miner_id=i, hotkey=f"hk_{i}", uid=i,
            n_submissions=50,
            brier=MetricAccumulator(ws=10.0 + i, wt=50.0),
            fq=MetricAccumulator(ws=25.0 + i, wt=50.0),
            pss=MetricAccumulator(ws=5.0 + i, wt=50.0),
            es=MetricAccumulator(ws=2.0 + i * 0.1, wt=50.0),
            mes=MetricAccumulator(ws=30.0, wt=50.0),
            sos=MetricAccumulator(ws=25.0, wt=50.0),
            lead=MetricAccumulator(ws=20.0, wt=50.0),
            cal_score=0.7, sharp_score=0.6,
        ))

    return CheckpointWindow(
        manifest=LedgerManifest(
            window_type="checkpoint",
            window_start=now, window_end=now,
            checkpoint_epoch=1,
            content_hashes={},
            primary_hotkey="primary_hk",
            created_at=now,
        ),
        roster=[MinerRosterEntry(miner_id=i, uid=i, hotkey=f"hk_{i}", active=True) for i in range(1, n_miners + 1)],
        accumulators=accumulators,
        scoring_config=ScoringConfigSnapshot(params={
            "dimension_weights": {"w_fq": 0.6, "w_cal": 0.4, "w_edge": 0.7, "w_mes": 0.3, "w_sos": 0.6, "w_lead": 0.4},
            "skill_score_weights": {"w_outcome_accuracy": 0.10, "w_outcome_relative": 0.10, "w_odds_edge": 0.50, "w_info_adv": 0.30},
            "normalization": {"min_count_for_zscore": 10},
            "weight_emission": {"burn_rate": 0.9},
        }),
        chain_params=ChainParamsSnapshot(
            burn_rate=0.9, burn_uid=0,
            max_weight_limit=0.5, min_allowed_weights=1, n_neurons=10,
        ),
    )


def _make_delta_with_correct_brier() -> DeltaWindow:
    now = datetime.now(timezone.utc)
    return DeltaWindow(
        manifest=LedgerManifest(
            window_type="delta",
            window_start=now, window_end=now,
            checkpoint_epoch=1, content_hashes={},
            primary_hotkey="primary_hk", created_at=now,
        ),
        settled_submissions=[
            SettledSubmissionEntry(
                miner_id=1, market_id=10, side="home",
                imp_prob=0.6, brier=(0.6 - 1.0) ** 2,  # correct: outcome is home
                settled_at=now,
            ),
        ],
        settled_outcomes=[
            OutcomeEntry(market_id=10, event_id=100, result="home",
                         score_home=2, score_away=1, settled_at=now),
        ],
    )


def _make_delta_with_fabricated_brier() -> DeltaWindow:
    now = datetime.now(timezone.utc)
    return DeltaWindow(
        manifest=LedgerManifest(
            window_type="delta",
            window_start=now, window_end=now,
            checkpoint_epoch=1, content_hashes={},
            primary_hotkey="primary_hk", created_at=now,
        ),
        settled_submissions=[
            SettledSubmissionEntry(
                miner_id=1, market_id=10, side="home",
                imp_prob=0.6, brier=0.99,  # FABRICATED - should be 0.16
                settled_at=now,
            ),
        ],
        settled_outcomes=[
            OutcomeEntry(market_id=10, event_id=100, result="home",
                         score_home=2, score_away=1, settled_at=now),
        ],
    )


class TestWeightVerificationPlugin:

    @pytest.mark.asyncio
    async def test_computes_weights_from_checkpoint(self):
        handler = WeightVerificationHandler(tolerance=0.01)
        context = AuditorContext(
            checkpoint=_make_checkpoint(),
            deltas=[],
        )
        result = await handler.on_cycle(context)
        assert result.status in ("pass", "fail", "skip")
        assert result.plugin_name == "weight_verification"

    @pytest.mark.asyncio
    async def test_skip_when_no_checkpoint(self):
        handler = WeightVerificationHandler()
        context = AuditorContext(checkpoint=None)
        result = await handler.on_cycle(context)
        assert result.status == "skip"
        assert result.evidence["reason"] == "no_checkpoint"

    @pytest.mark.asyncio
    async def test_brier_recomputation_catches_fabricated_score(self):
        handler = WeightVerificationHandler()
        context = AuditorContext(
            checkpoint=_make_checkpoint(),
            deltas=[_make_delta_with_fabricated_brier()],
        )
        result = await handler.on_cycle(context)
        assert result.evidence["brier_mismatches"] > 0

    @pytest.mark.asyncio
    async def test_brier_recomputation_passes_correct(self):
        handler = WeightVerificationHandler()
        context = AuditorContext(
            checkpoint=_make_checkpoint(),
            deltas=[_make_delta_with_correct_brier()],
        )
        result = await handler.on_cycle(context)
        assert result.evidence["brier_mismatches"] == 0
        assert result.evidence["brier_checks"] == 1

    @pytest.mark.asyncio
    async def test_skip_when_no_miners(self):
        handler = WeightVerificationHandler()
        context = AuditorContext(checkpoint=_make_checkpoint(n_miners=0))
        result = await handler.on_cycle(context)
        assert result.status == "skip"

    @pytest.mark.asyncio
    async def test_shapley_dims_propagate_through_checkpoint(self):
        """AccumulatorEntry uniqueness/marginal dims reach compute_weights."""
        from sparket.validator.ledger.compute_weights import compute_weights
        from sparket.validator.ledger.models import MinerMetrics

        cp = _make_checkpoint(n_miners=3)
        # Simulate Shapley jobs having populated these fields
        for acc in cp.accumulators:
            acc.uniqueness_dim = 0.75
            acc.marginal_dim = 0.65

        # Auditor path: from_accumulator → MinerMetrics
        metrics = [MinerMetrics.from_accumulator(acc) for acc in cp.accumulators]
        for m in metrics:
            assert m.uniqueness_dim == 0.75, "uniqueness_dim not propagated"
            assert m.marginal_dim == 0.65, "marginal_dim not propagated"

        # Verify compute_weights uses the explicit values, not fallbacks
        result = compute_weights(metrics, cp.scoring_config, cp.chain_params)
        for uid in result.dimension_scores:
            dims = result.dimension_scores[uid]
            # 0.75 clamped to [epsilon, 1.0] should stay 0.75
            assert abs(dims["uniqueness_dim"] - 0.75) < 1e-6
            assert abs(dims["marginal_dim"] - 0.65) < 1e-6
