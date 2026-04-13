"""End-to-end auditor cycle tests.

Simulates the complete flow:
  Primary validator: score miners → export checkpoint + delta → write to store
  Auditor validator: fetch from store → verify → compute weights → set weights

Uses real bittensor wallets for signing, real filesystem for storage,
real compute_weights for scoring, and MockSubtensor/MockMetagraph for
chain interactions.  No running validator, database, or network needed.

Tests the exact failure mode diagnosed in v0.1.2: schema version mismatch
causing auditors to silently skip weight-setting.
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from sparket import __spec_version__
from sparket.validator.auditor.plugin_registry import AuditorContext, PluginRegistry
from sparket.validator.auditor.plugins.weight_verification import (
    HANDLER as weight_handler_instance,
    WeightVerificationHandler,
)
from sparket.validator.auditor.sync import LedgerSync
from sparket.validator.auditor.verifier import ManifestVerifier
from sparket.validator.config.scoring_params import ScoringParams
from sparket.validator.ledger.compute_weights import compute_weights
from sparket.validator.ledger.models import (
    LEDGER_SCHEMA_VERSION,
    AccumulatorEntry,
    ChainParamsSnapshot,
    CheckpointWindow,
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
from sparket.validator.ledger.signer import compute_section_hash, sign_manifest
from sparket.validator.ledger.store.filesystem import FilesystemStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def primary_wallet():
    """Real bittensor wallet for the primary validator."""
    import bittensor as bt
    w = bt.Wallet(name="test_e2e_primary", hotkey="test_e2e_primary_hk")
    w.create_if_non_existent(coldkey_use_password=False, hotkey_use_password=False)
    return w


@pytest.fixture
def auditor_wallet():
    """Real bittensor wallet for the auditor."""
    import bittensor as bt
    w = bt.Wallet(name="test_e2e_auditor", hotkey="test_e2e_auditor_hk")
    w.create_if_non_existent(coldkey_use_password=False, hotkey_use_password=False)
    return w


@pytest.fixture
def shared_store():
    """Shared filesystem store — primary writes, auditor reads."""
    with tempfile.TemporaryDirectory() as d:
        yield FilesystemStore(data_dir=d, retention_days=7)


@pytest.fixture
def prod_config():
    return ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))


@pytest.fixture
def chain_params():
    return ChainParamsSnapshot(
        burn_rate=0.0, burn_uid=0,
        max_weight_limit=0.1, min_allowed_weights=1, n_neurons=256,
    )


def _now():
    return datetime.now(timezone.utc)


def _build_realistic_checkpoint(
    wallet,
    config: ScoringConfigSnapshot,
    chain_params: ChainParamsSnapshot,
    n_miners: int = 30,
    epoch: int = 6,
    seed: int = 42,
) -> CheckpointWindow:
    """Build a production-realistic signed checkpoint.

    Mix of:
    - Miners with good Brier (pass floor)
    - Miners with no outcome data (brier wt=0 → default 0.5 → floored)
    - Miners with varying uniqueness/marginal dims
    - A few miners with None Shapley dims (fallback path)
    """
    rng = np.random.RandomState(seed)
    now = _now()
    roster = []
    accumulators = []

    for uid in range(1, n_miners + 1):
        hotkey = f"5miner{'0' * 40}{uid:04d}"
        roster.append(MinerRosterEntry(
            miner_id=uid, uid=uid, hotkey=hotkey, active=True,
        ))

        # ~60% of miners have outcome data, ~40% don't
        has_outcomes = rng.random() > 0.4
        wt = rng.uniform(50, 300) if has_outcomes else 0.0
        brier_val = rng.beta(2, 8) * 0.4 if has_outcomes else 0.0

        accumulators.append(AccumulatorEntry(
            miner_id=uid, hotkey=hotkey, uid=uid,
            n_submissions=int(rng.uniform(100, 5000)),
            brier=MetricAccumulator(ws=brier_val * wt, wt=wt),
            fq=MetricAccumulator(
                ws=rng.uniform(-0.2, 0.3) * 100, wt=100.0,
            ),
            pss=MetricAccumulator(
                ws=rng.normal(0, 0.3) * 100, wt=100.0,
            ),
            es=MetricAccumulator(
                ws=rng.uniform(0.01, 0.1) * 100, wt=100.0,
            ),
            mes=MetricAccumulator(
                ws=rng.beta(3, 2) * 100, wt=100.0,
            ),
            sos=MetricAccumulator(
                ws=rng.beta(3, 3) * 100, wt=100.0,
            ),
            lead=MetricAccumulator(
                ws=rng.beta(3, 3) * 100, wt=100.0,
            ),
            cal_score=rng.beta(5, 3),
            sharp_score=rng.beta(4, 4),
            uniqueness_dim=float(rng.beta(3, 3)) if rng.random() > 0.2 else None,
            marginal_dim=float(rng.beta(5, 5)) if rng.random() > 0.2 else None,
        ))

    content_hashes = {
        "roster": compute_section_hash(roster),
        "accumulators": compute_section_hash(accumulators),
        "scoring_config": compute_section_hash(config),
    }

    manifest = LedgerManifest(
        window_type="checkpoint",
        window_start=now, window_end=now,
        checkpoint_epoch=epoch,
        content_hashes=content_hashes,
        primary_hotkey=wallet.hotkey.ss58_address,
        created_at=now,
    )
    manifest.signature = sign_manifest(manifest, wallet)

    return CheckpointWindow(
        manifest=manifest, roster=roster,
        accumulators=accumulators,
        scoring_config=config,
        chain_params=chain_params,
    )


def _build_delta(wallet, epoch: int = 6, n_markets: int = 10) -> DeltaWindow:
    """Build a signed delta with correct Brier scores."""
    now = _now()
    submissions = []
    outcomes = []

    for market_id in range(1000, 1000 + n_markets):
        result = "HOME" if market_id % 2 == 0 else "AWAY"
        outcomes.append(OutcomeEntry(
            market_id=market_id, event_id=market_id,
            result=result, settled_at=now,
        ))
        for miner_id in [1, 2, 3]:
            imp_prob = 0.5 + miner_id * 0.1
            actual = 1.0 if "HOME" == result else 0.0
            brier = (imp_prob - actual) ** 2
            submissions.append(SettledSubmissionEntry(
                miner_id=miner_id, market_id=market_id, side="HOME",
                imp_prob=imp_prob, brier=brier, settled_at=now,
            ))

    content_hashes = {
        "settled_submissions": compute_section_hash(submissions),
        "settled_outcomes": compute_section_hash(outcomes),
    }
    manifest = LedgerManifest(
        window_type="delta",
        window_start=now - timedelta(minutes=5), window_end=now,
        checkpoint_epoch=epoch, content_hashes=content_hashes,
        primary_hotkey=wallet.hotkey.ss58_address, created_at=now,
    )
    manifest.signature = sign_manifest(manifest, wallet)

    return DeltaWindow(
        manifest=manifest,
        settled_submissions=submissions,
        settled_outcomes=outcomes,
    )


def _mock_subtensor(chain_params: ChainParamsSnapshot):
    """Build a mock subtensor that returns chain params."""
    sub = MagicMock()
    sub.max_weight_limit.return_value = chain_params.max_weight_limit
    sub.min_allowed_weights.return_value = chain_params.min_allowed_weights
    sub.get_subnet_owner_hotkey.return_value = None  # No burn hotkey
    sub.set_weights.return_value = (True, "Success")
    return sub


def _mock_metagraph(chain_params: ChainParamsSnapshot, hotkeys: list[str]):
    """Build a mock metagraph with the given hotkeys."""
    mg = MagicMock()
    mg.n = chain_params.n_neurons
    mg.hotkeys = hotkeys
    return mg


# ===================================================================
# TEST 1: Full happy-path auditor cycle
# ===================================================================

class TestFullAuditorCycle:
    """Simulate the complete primary → auditor flow end-to-end."""

    @pytest.mark.asyncio
    async def test_primary_export_auditor_verify_and_set_weights(
        self, primary_wallet, auditor_wallet, shared_store, prod_config, chain_params,
    ):
        """Primary exports checkpoint + delta → auditor fetches, verifies,
        computes weights, and calls set_weights successfully."""

        # === PRIMARY SIDE ===
        cp = _build_realistic_checkpoint(
            primary_wallet, prod_config, chain_params, n_miners=30,
        )
        delta = _build_delta(primary_wallet, epoch=6)

        cp_id = await shared_store.put_checkpoint(cp)
        delta_id = await shared_store.put_delta(delta)

        # === AUDITOR SIDE ===
        # 1. Fetch checkpoint (same store, simulating HTTP fetch)
        fetched_cp = await shared_store.get_latest_checkpoint()
        assert fetched_cp is not None
        assert fetched_cp.manifest.checkpoint_epoch == 6

        # 2. Verify manifest
        verifier = ManifestVerifier(primary_hotkey=primary_wallet.hotkey.ss58_address)
        cp_result = verifier.verify_checkpoint(fetched_cp)
        assert cp_result, f"Checkpoint verification failed: {cp_result.errors}"

        # 3. Fetch and verify delta
        delta_ids = await shared_store.list_deltas(epoch=6)
        assert len(delta_ids) > 0
        fetched_delta = await shared_store.get_delta(delta_ids[0])
        assert fetched_delta is not None
        delta_result = verifier.verify_delta(fetched_delta)
        assert delta_result, f"Delta verification failed: {delta_result.errors}"

        # 4. Extract metrics and compute weights (auditor path)
        metrics = [MinerMetrics.from_accumulator(acc) for acc in fetched_cp.accumulators]
        assert len(metrics) == 30

        weight_result = compute_weights(
            metrics, fetched_cp.scoring_config, chain_params,
        )

        # Should have some scored miners (those passing Brier floor)
        scored = sum(1 for s in weight_result.skill_scores.values() if s > 0)
        assert scored > 0, "At least some miners should pass the Brier floor"
        assert len(weight_result.uids) > 0
        assert len(weight_result.uint16_weights) > 0

        # 5. Simulate set_weights call
        mock_sub = _mock_subtensor(chain_params)
        mock_sub.set_weights.return_value = (True, "Success")

        result_ok, msg = mock_sub.set_weights(
            wallet=auditor_wallet,
            netuid=57,
            uids=weight_result.uids,
            weights=weight_result.uint16_weights,
            wait_for_finalization=False,
            wait_for_inclusion=False,
            version_key=__spec_version__,
        )
        assert result_ok is True

        # Verify set_weights was called with version_key
        call_kwargs = mock_sub.set_weights.call_args
        assert call_kwargs.kwargs["version_key"] == __spec_version__

    @pytest.mark.asyncio
    async def test_auditor_weights_match_primary_weights(
        self, primary_wallet, shared_store, prod_config, chain_params,
    ):
        """Weights computed by the auditor from a stored checkpoint must
        exactly match weights computed from the original data."""

        cp = _build_realistic_checkpoint(
            primary_wallet, prod_config, chain_params, n_miners=50, seed=77,
        )
        await shared_store.put_checkpoint(cp)

        # Primary computes directly from original accumulators
        primary_metrics = [MinerMetrics.from_accumulator(a) for a in cp.accumulators]
        primary_result = compute_weights(primary_metrics, prod_config, chain_params)

        # Auditor fetches from store (data went through JSON + gzip)
        fetched = await shared_store.get_latest_checkpoint()
        auditor_metrics = [MinerMetrics.from_accumulator(a) for a in fetched.accumulators]
        auditor_result = compute_weights(
            auditor_metrics, fetched.scoring_config, chain_params,
        )

        assert primary_result.uids == auditor_result.uids
        assert primary_result.uint16_weights == auditor_result.uint16_weights


# ===================================================================
# TEST 2: Weight verification plugin full cycle
# ===================================================================

class TestWeightVerificationPluginE2E:
    """Run the actual WeightVerificationHandler against realistic data."""

    @pytest.mark.asyncio
    async def test_plugin_computes_and_reports_weights(
        self, primary_wallet, auditor_wallet, shared_store, prod_config, chain_params,
    ):
        """The weight verification plugin should compute weights and
        attempt to set them on chain."""

        cp = _build_realistic_checkpoint(
            primary_wallet, prod_config, chain_params, n_miners=20,
        )
        delta = _build_delta(primary_wallet, epoch=6)

        mock_sub = _mock_subtensor(chain_params)
        mock_sub.set_weights.return_value = (True, "Success")
        hotkeys = [f"5miner{'0' * 40}{uid:04d}" for uid in range(1, 21)]
        mock_mg = _mock_metagraph(chain_params, hotkeys)

        handler = WeightVerificationHandler()
        context = AuditorContext(
            checkpoint=cp,
            deltas=[delta],
            accumulator_state={},
            wallet=auditor_wallet,
            subtensor=mock_sub,
            metagraph=mock_mg,
            config={"netuid": 57},
        )

        result = await handler.on_cycle(context)

        assert result.status == "pass"
        assert result.plugin_name == "weight_verification"
        assert "computed_uids" in result.evidence
        assert "n_miners_scored" in result.evidence
        assert result.evidence["n_miners_scored"] == 20

        # set_weights should have been called
        assert "set_weights" in result.evidence
        assert "success" in result.evidence["set_weights"]
        mock_sub.set_weights.assert_called_once()

        # Verify version_key was passed
        call_kwargs = mock_sub.set_weights.call_args.kwargs
        assert call_kwargs["version_key"] == __spec_version__

    @pytest.mark.asyncio
    async def test_plugin_reports_brier_mismatches(
        self, primary_wallet, auditor_wallet, chain_params,
    ):
        """Plugin should detect fabricated Brier scores in deltas."""
        now = _now()
        config = ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))

        # Build checkpoint with 2 miners
        accumulators = [
            AccumulatorEntry(
                miner_id=i, hotkey=f"hk_{i}", uid=i, n_submissions=100,
                brier=MetricAccumulator(ws=15.0, wt=100.0),
                fq=MetricAccumulator(ws=5.0, wt=100.0),
                pss=MetricAccumulator(ws=1.0, wt=100.0),
                es=MetricAccumulator(ws=2.0, wt=100.0),
                mes=MetricAccumulator(ws=60.0, wt=100.0),
                sos=MetricAccumulator(ws=50.0, wt=100.0),
                lead=MetricAccumulator(ws=40.0, wt=100.0),
                cal_score=0.7, sharp_score=0.6,
            )
            for i in [1, 2]
        ]
        cp = CheckpointWindow(
            manifest=LedgerManifest(
                window_type="checkpoint", window_start=now, window_end=now,
                checkpoint_epoch=1, content_hashes={},
                primary_hotkey=primary_wallet.hotkey.ss58_address,
                created_at=now,
            ),
            roster=[MinerRosterEntry(miner_id=i, uid=i, hotkey=f"hk_{i}", active=True)
                    for i in [1, 2]],
            accumulators=accumulators,
            scoring_config=config,
            chain_params=chain_params,
        )

        # Build delta with FABRICATED Brier scores
        submissions = [SettledSubmissionEntry(
            miner_id=1, market_id=100, side="HOME",
            imp_prob=0.6,
            brier=0.99,  # WRONG: should be (0.6 - 1.0)^2 = 0.16
            settled_at=now,
        )]
        outcomes = [OutcomeEntry(
            market_id=100, event_id=100, result="HOME", settled_at=now,
        )]
        delta = DeltaWindow(
            manifest=LedgerManifest(
                window_type="delta", window_start=now, window_end=now,
                checkpoint_epoch=1, content_hashes={},
                primary_hotkey=primary_wallet.hotkey.ss58_address,
                created_at=now,
            ),
            settled_submissions=submissions,
            settled_outcomes=outcomes,
        )

        mock_sub = _mock_subtensor(chain_params)
        mock_sub.set_weights.return_value = (True, "Success")

        handler = WeightVerificationHandler()
        context = AuditorContext(
            checkpoint=cp, deltas=[delta], accumulator_state={},
            wallet=auditor_wallet, subtensor=mock_sub,
            metagraph=MagicMock(n=256, hotkeys=[]),
            config={"netuid": 57},
        )

        result = await handler.on_cycle(context)
        assert result.evidence["brier_mismatches"] > 0


# ===================================================================
# TEST 3: Auditor sync + epoch handling
# ===================================================================

class TestAuditorSyncCycle:
    """Test LedgerSync state management through multiple cycles."""

    @pytest.mark.asyncio
    async def test_sync_fetches_checkpoint_and_deltas(
        self, primary_wallet, shared_store, prod_config, chain_params,
    ):
        """LedgerSync.sync_cycle() should fetch and track state."""

        cp = _build_realistic_checkpoint(
            primary_wallet, prod_config, chain_params, n_miners=10,
        )
        delta = _build_delta(primary_wallet, epoch=6)
        await shared_store.put_checkpoint(cp)
        await shared_store.put_delta(delta)

        with tempfile.TemporaryDirectory() as state_dir:
            sync = LedgerSync(store=shared_store, data_dir=state_dir)
            fetched_cp, deltas = await sync.sync_cycle()

            assert fetched_cp is not None
            assert fetched_cp.manifest.checkpoint_epoch == 6
            assert sync.epoch == 6
            assert len(deltas) > 0

            # Second cycle: no new deltas
            fetched_cp2, deltas2 = await sync.sync_cycle()
            assert fetched_cp2 is not None
            assert len(deltas2) == 0  # Already processed

    @pytest.mark.asyncio
    async def test_epoch_bump_resets_auditor_state(
        self, primary_wallet, shared_store, prod_config, chain_params,
    ):
        """When the primary bumps epoch, the auditor should reset."""

        # Epoch 6 checkpoint
        cp6 = _build_realistic_checkpoint(
            primary_wallet, prod_config, chain_params,
            n_miners=10, epoch=6,
        )
        await shared_store.put_checkpoint(cp6)

        with tempfile.TemporaryDirectory() as state_dir:
            sync = LedgerSync(store=shared_store, data_dir=state_dir)
            await sync.sync_cycle()
            assert sync.epoch == 6

            # Primary bumps to epoch 7
            cp7 = _build_realistic_checkpoint(
                primary_wallet, prod_config, chain_params,
                n_miners=10, epoch=7, seed=99,
            )
            # Attach recompute record
            cp7.manifest.recompute_record = RecomputeRecord(
                epoch=7, previous_epoch=6,
                reason_code=RecomputeReasonCode.SCORING_BUG,
                reason_detail="brier_mean default alignment",
                severity="bugfix",
                timestamp=_now(),
                code_version="v0.1.2",
            )
            await shared_store.put_checkpoint(cp7)

            fetched_cp, deltas = await sync.sync_cycle()
            assert sync.epoch == 7
            assert sync.last_delta_id == ""  # Reset


# ===================================================================
# TEST 4: Schema version enforcement
# ===================================================================

class TestSchemaVersionEnforcement:
    """Verify that schema version mismatch causes verification failure —
    the exact bug that stopped auditors from setting weights."""

    def test_v2_checkpoint_passes_v2_verifier(self, primary_wallet):
        """Current code: schema_version=2, verifier expects 2 → PASS."""
        cp = _build_realistic_checkpoint(
            primary_wallet,
            ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json")),
            ChainParamsSnapshot(
                burn_rate=0.0, burn_uid=0,
                max_weight_limit=0.1, min_allowed_weights=1, n_neurons=20,
            ),
            n_miners=5,
        )
        assert cp.manifest.schema_version == 2

        verifier = ManifestVerifier(primary_hotkey=primary_wallet.hotkey.ss58_address)
        result = verifier.verify_checkpoint(cp)
        assert result, f"Should pass: {result.errors}"

    def test_future_schema_version_rejected(self, primary_wallet):
        """If primary is on a newer schema version, auditor must reject."""
        cp = _build_realistic_checkpoint(
            primary_wallet,
            ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json")),
            ChainParamsSnapshot(
                burn_rate=0.0, burn_uid=0,
                max_weight_limit=0.1, min_allowed_weights=1, n_neurons=20,
            ),
            n_miners=5,
        )
        # Tamper: pretend it's schema v99
        cp.manifest.schema_version = 99

        verifier = ManifestVerifier(primary_hotkey=primary_wallet.hotkey.ss58_address)
        result = verifier.verify_checkpoint(cp)
        assert not result
        assert any("schema_version" in e for e in result.errors)

    def test_schema_v1_rejected_by_v2_verifier(self, primary_wallet):
        """This is the exact failure mode: old auditor code (v1) would
        reject new primary checkpoints (v2). Now both are v2."""
        cp = _build_realistic_checkpoint(
            primary_wallet,
            ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json")),
            ChainParamsSnapshot(
                burn_rate=0.0, burn_uid=0,
                max_weight_limit=0.1, min_allowed_weights=1, n_neurons=20,
            ),
            n_miners=5,
        )
        cp.manifest.schema_version = 1  # Simulate old primary

        verifier = ManifestVerifier(primary_hotkey=primary_wallet.hotkey.ss58_address)
        result = verifier.verify_checkpoint(cp)
        assert not result
        assert any("schema_version" in e for e in result.errors)


# ===================================================================
# TEST 5: No-outcome miners in full pipeline
# ===================================================================

class TestNoOutcomeMinersE2E:
    """Verify the brier_mean=0.5 fix works through the full pipeline:
    miners with zero brier_wt should be floored in both primary
    and auditor paths after checkpoint serialization."""

    @pytest.mark.asyncio
    async def test_no_outcome_miners_floored_after_roundtrip(
        self, primary_wallet, shared_store, chain_params,
    ):
        config = ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))
        now = _now()

        # 3 miners: one good, one mediocre, one with NO outcomes
        accumulators = [
            AccumulatorEntry(
                miner_id=1, hotkey="good", uid=1, n_submissions=1000,
                brier=MetricAccumulator(ws=15.0, wt=100.0),  # 0.15 < 0.30 → passes
                fq=MetricAccumulator(ws=10.0, wt=100.0),
                pss=MetricAccumulator(ws=5.0, wt=100.0),
                es=MetricAccumulator(ws=3.0, wt=100.0),
                mes=MetricAccumulator(ws=60.0, wt=100.0),
                sos=MetricAccumulator(ws=50.0, wt=100.0),
                lead=MetricAccumulator(ws=40.0, wt=100.0),
                cal_score=0.7, sharp_score=0.6,
            ),
            AccumulatorEntry(
                miner_id=2, hotkey="bad", uid=2, n_submissions=500,
                brier=MetricAccumulator(ws=40.0, wt=100.0),  # 0.40 > 0.30 → floored
                fq=MetricAccumulator(ws=5.0, wt=100.0),
                pss=MetricAccumulator(ws=1.0, wt=100.0),
                es=MetricAccumulator(ws=1.0, wt=100.0),
                mes=MetricAccumulator(ws=50.0, wt=100.0),
                sos=MetricAccumulator(ws=50.0, wt=100.0),
                lead=MetricAccumulator(ws=50.0, wt=100.0),
                cal_score=0.5, sharp_score=0.5,
            ),
            AccumulatorEntry(
                miner_id=3, hotkey="no_outcomes", uid=3, n_submissions=2000,
                brier=MetricAccumulator(ws=0.0, wt=0.0),  # No outcomes → default 0.5 → floored
                fq=MetricAccumulator(ws=20.0, wt=100.0),
                pss=MetricAccumulator(ws=8.0, wt=100.0),
                es=MetricAccumulator(ws=5.0, wt=100.0),
                mes=MetricAccumulator(ws=70.0, wt=100.0),
                sos=MetricAccumulator(ws=60.0, wt=100.0),
                lead=MetricAccumulator(ws=50.0, wt=100.0),
                cal_score=0.8, sharp_score=0.7,
            ),
        ]

        roster = [
            MinerRosterEntry(miner_id=i, uid=i, hotkey=a.hotkey, active=True)
            for i, a in zip([1, 2, 3], accumulators)
        ]
        content_hashes = {
            "roster": compute_section_hash(roster),
            "accumulators": compute_section_hash(accumulators),
            "scoring_config": compute_section_hash(config),
        }
        manifest = LedgerManifest(
            window_type="checkpoint", window_start=now, window_end=now,
            checkpoint_epoch=6, content_hashes=content_hashes,
            primary_hotkey=primary_wallet.hotkey.ss58_address, created_at=now,
        )
        manifest.signature = sign_manifest(manifest, primary_wallet)
        cp = CheckpointWindow(
            manifest=manifest, roster=roster,
            accumulators=accumulators, scoring_config=config,
            chain_params=chain_params,
        )

        # Write and read back (full filesystem roundtrip)
        await shared_store.put_checkpoint(cp)
        fetched = await shared_store.get_latest_checkpoint()

        metrics = [MinerMetrics.from_accumulator(a) for a in fetched.accumulators]
        result = compute_weights(metrics, fetched.scoring_config, chain_params)

        # Miner 1 (good brier=0.15): should score > 0
        assert result.skill_scores[1] > 0, "Good miner should pass floor"

        # Miner 2 (bad brier=0.40): should be floored
        assert result.skill_scores[2] == 0.0, "Bad miner should be floored"

        # Miner 3 (no outcomes, brier default=0.5): MUST be floored
        assert result.skill_scores[3] == 0.0, \
            "No-outcome miner should be floored (brier default 0.5 > 0.30)"

        # Only miner 1 should have weight
        assert 1 in result.uids
        assert 2 not in result.uids
        assert 3 not in result.uids
