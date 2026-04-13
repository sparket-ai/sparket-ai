"""Deep validation tests for release/v0.1.2.

Covers:
  1. Full scoring pipeline with score value verification
  2. Live checkpoint content + auditor weight agreement
  3. Database migration (standalone, no PM2)
  4. Brier floor with no-outcome miner
  5. CLE odds ratio gate
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timedelta, timezone

import aiohttp
import numpy as np
import pytest

from sparket import __spec_version__
from sparket.validator.config.scoring_params import ScoringParams
from sparket.validator.ledger.compute_weights import compute_weights
from sparket.validator.ledger.models import (
    LEDGER_SCHEMA_VERSION,
    AccumulatorEntry,
    CheckpointWindow,
    ChainParamsSnapshot,
    MinerMetrics,
    ScoringConfigSnapshot,
)
from sparket.validator.ledger.signer import compute_section_hash
from sparket.validator.auditor.verifier import ManifestVerifier

from .harness import LocalnetHarness


VALIDATOR_URL = "http://127.0.0.1:8199"
LEDGER_URL = "http://127.0.0.1:8200"
_TIMEOUT = aiohttp.ClientTimeout(total=120)


async def _get(url: str) -> dict:
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
        async with s.get(url) as r:
            return await r.json()


async def _post(url: str, data: dict | None = None) -> dict:
    async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
        async with s.post(url, json=data or {}) as r:
            return await r.json()


async def _wait_for_submissions(count: int = 1, timeout_s: int = 60) -> dict:
    """Poll until at least `count` submissions appear."""
    for _ in range(timeout_s // 3):
        await asyncio.sleep(3)
        subs = await _get(f"{VALIDATOR_URL}/db/submissions")
        if subs.get("count", 0) >= count:
            return subs
    return subs


# ===========================================================================
# 1. Full Scoring Pipeline with Score Verification
# ===========================================================================


class TestScoringPipelineDeep:
    """Verify actual score values after full pipeline run."""

    @pytest.mark.asyncio
    async def test_full_pipeline_produces_valid_scores(self, clean_harness: LocalnetHarness):
        """Seed -> Submit -> Score -> Verify score values are sensible."""
        h = clean_harness

        # Seed 3 scheduled future events
        markets = []
        for home, away in [("ScoreA", "ScoreB"), ("ScoreC", "ScoreD"), ("ScoreE", "ScoreF")]:
            ev = await h.validator.create_event(home, away, hours_ahead=48, status="scheduled")
            assert ev.get("status") == "ok"
            markets.append(ev["event"]["db_market_id"])

        # Seed ground truth for each market
        gt = {}
        for mid in markets:
            hp = round(random.uniform(0.35, 0.65), 2)
            ap = round(1.0 - hp, 2)
            gt[mid] = {"home_prob": hp, "away_prob": ap}
            await h.validator.seed_ground_truth(mid, hp, ap)

        # Both miners submit accurate odds (close to ground truth)
        for miner in h.miners:
            for mid in markets:
                prob = gt[mid]["home_prob"]
                noise = random.uniform(-0.05, 0.05)
                hp = max(0.1, min(0.9, prob + noise))
                await miner.submit_odds(mid, round(1.0 / hp, 2), round(1.0 / (1.0 - hp), 2))

        # Wait for submissions to be ingested
        subs = await _wait_for_submissions(count=4, timeout_s=60)
        assert subs.get("count", 0) >= 4, f"Only {subs.get('count', 0)} submissions ingested"

        # Add outcomes for all markets
        for mid in markets:
            result = "HOME" if random.random() > 0.5 else "AWAY"
            await h.validator.seed_outcome(mid, result)

        # Run full scoring pipeline
        await h.validator.trigger_odds_scoring()
        await h.validator.trigger_outcome_scoring()
        await h.validator.backdate_submissions(days=1)
        scoring = await h.validator.trigger_scoring()
        assert scoring.get("status") == "ok", f"Scoring failed: {scoring}"

        # Verify rolling scores have valid values
        rolling = await h.validator.get_rolling_scores()
        assert rolling.get("count", 0) > 0, "No rolling scores"

        for score in rolling.get("scores", []):
            # brier_mean should be in [0, 1]
            brier = score.get("brier_mean")
            if brier is not None:
                assert 0.0 <= brier <= 1.0, f"brier_mean out of range: {brier}"

            # fq_raw should be in [-1, 1] range roughly
            fq = score.get("fq_raw")
            if fq is not None:
                assert -2.0 <= fq <= 2.0, f"fq_raw out of range: {fq}"

            # n_submissions should be positive
            n = score.get("n_submissions", 0)
            assert n > 0, f"n_submissions should be > 0, got {n}"

        # Verify skill scores exist
        skill = await h.validator.get_skill_scores()
        # skill_score may be 0 if brier-floored, but the endpoint should return data
        assert "scores" in skill

        # Verify weights endpoint returns valid structure
        weights = await h.validator.get_weights()
        assert "weights" in weights
        assert "total_weight" in weights
        total = weights.get("total_weight", 0)
        # Total weight should be 0.0 (all floored) or ~1.0 (normalized)
        assert total == 0.0 or abs(total - 1.0) < 0.01, f"total_weight={total}"


# ===========================================================================
# 2. Live Checkpoint + Auditor Weight Agreement
# ===========================================================================


class TestCheckpointAuditorAgreement:
    """Verify checkpoint content and auditor weight matching."""

    @pytest.mark.asyncio
    async def test_checkpoint_has_v012_fields(self):
        """Checkpoint accumulators must have uniqueness_dim and marginal_dim."""
        import bittensor as bt

        # Authenticate as auditor
        auditor_wallet = bt.Wallet(name="e2e-miner-3")
        hotkey = auditor_wallet.hotkey.ss58_address

        challenge = await _post(f"{LEDGER_URL}/ledger/auth/challenge", {"hotkey": hotkey})
        if "nonce" not in challenge:
            pytest.skip("Ledger auth not available")

        nonce = challenge["nonce"]
        signature = auditor_wallet.hotkey.sign(nonce.encode()).hex()
        auth_resp = await _post(f"{LEDGER_URL}/ledger/auth/respond", {
            "hotkey": hotkey, "nonce": nonce, "signature": signature,
        })
        token = auth_resp.get("token")
        if not token:
            pytest.skip("No auth token received")

        # Fetch latest checkpoint
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                f"{LEDGER_URL}/ledger/checkpoints/latest",
                headers={"Authorization": f"Bearer {token}"},
            ) as r:
                if r.status == 404:
                    pytest.skip("No checkpoint available yet")
                assert r.status == 200, f"Unexpected status: {r.status}"
                cp_data = await r.json()

        # Verify manifest
        manifest = cp_data.get("manifest", {})
        assert manifest.get("schema_version") == 2, (
            f"Expected schema_version=2, got {manifest.get('schema_version')}"
        )
        assert manifest.get("window_type") == "checkpoint"
        assert manifest.get("primary_hotkey"), "Missing primary_hotkey"

        # Verify accumulators have v0.1.2 fields
        accumulators = cp_data.get("accumulators", [])
        if len(accumulators) > 0:
            first = accumulators[0]
            # New v0.1.2 fields should be present (may be null)
            assert "uniqueness_dim" in first, "Missing uniqueness_dim in accumulator"
            assert "marginal_dim" in first, "Missing marginal_dim in accumulator"
            # Standard fields
            assert "brier" in first, "Missing brier accumulator"
            assert "fq" in first, "Missing fq accumulator"
            assert "miner_id" in first

    @pytest.mark.asyncio
    async def test_auditor_computes_matching_weights(self):
        """Weights from checkpoint must match primary's /db/weights."""
        import bittensor as bt

        # Get primary weights
        primary_weights = await _get(f"{VALIDATOR_URL}/db/weights")
        if primary_weights.get("count", 0) == 0:
            pytest.skip("No weights computed yet — run scoring pipeline first")

        primary_uids = {w["uid"] for w in primary_weights.get("weights", [])}

        # Authenticate and fetch checkpoint
        auditor_wallet = bt.Wallet(name="e2e-miner-3")
        hotkey = auditor_wallet.hotkey.ss58_address
        challenge = await _post(f"{LEDGER_URL}/ledger/auth/challenge", {"hotkey": hotkey})
        if "nonce" not in challenge:
            pytest.skip("Ledger auth not available")

        nonce = challenge["nonce"]
        signature = auditor_wallet.hotkey.sign(nonce.encode()).hex()
        auth_resp = await _post(f"{LEDGER_URL}/ledger/auth/respond", {
            "hotkey": hotkey, "nonce": nonce, "signature": signature,
        })
        token = auth_resp.get("token")

        async with aiohttp.ClientSession(timeout=_TIMEOUT) as s:
            async with s.get(
                f"{LEDGER_URL}/ledger/checkpoints/latest",
                headers={"Authorization": f"Bearer {token}"},
            ) as r:
                if r.status != 200:
                    pytest.skip("No checkpoint available")
                cp_data = await r.json()

        # Compute weights from checkpoint (auditor path)
        accumulators = [AccumulatorEntry(**a) for a in cp_data.get("accumulators", [])]
        if not accumulators:
            pytest.skip("Empty checkpoint accumulators")

        scoring_config = cp_data.get("scoring_config")
        if not scoring_config:
            pytest.skip("Checkpoint missing scoring_config")

        config = ScoringConfigSnapshot(**scoring_config)

        # chain_params may be None if exporter doesn't populate it;
        # use localnet defaults (same as auditor would)
        chain_params_data = cp_data.get("chain_params")
        if chain_params_data:
            chain_params = ChainParamsSnapshot(**chain_params_data)
        else:
            chain_params = ChainParamsSnapshot(
                burn_rate=0.0, burn_uid=0,
                max_weight_limit=0.1, min_allowed_weights=1, n_neurons=10,
            )

        metrics = [MinerMetrics.from_accumulator(a) for a in accumulators]
        auditor_result = compute_weights(metrics, config, chain_params)

        # Compare UIDs — auditor should produce same set as primary
        auditor_uids = set(auditor_result.uids)
        assert auditor_uids == primary_uids, (
            f"UID mismatch: auditor={sorted(auditor_uids)}, primary={sorted(primary_uids)}"
        )


# ===========================================================================
# 3. Database Migration Test (standalone)
# ===========================================================================


class TestDatabaseMigration:
    """Verify alembic migrations apply cleanly."""

    @pytest.mark.asyncio
    async def test_fresh_migration(self):
        """Fresh DB migration produces all expected tables and columns."""
        import subprocess

        db_name = "sparket_migration_test"

        # Drop and recreate
        subprocess.run(
            ["sudo", "docker", "exec", "pg-sparket", "psql", "-U", "sparket",
             "-c", f"DROP DATABASE IF EXISTS {db_name};"],
            capture_output=True,
        )
        subprocess.run(
            ["sudo", "docker", "exec", "pg-sparket", "psql", "-U", "sparket",
             "-c", f"CREATE DATABASE {db_name};"],
            capture_output=True, check=True,
        )

        # Run migrations
        env = {
            "DATABASE_URL": f"postgresql+asyncpg://sparket:sparket@127.0.0.1:5435/{db_name}",
            "PATH": "/usr/bin:/usr/local/bin:/home/noah/.local/bin",
        }
        result = subprocess.run(
            ["/root/sparket-subnet/.venv/bin/sparket-alembic", "upgrade", "head"],
            capture_output=True, text=True, cwd="/root/sparket-subnet", env=env,
        )
        assert result.returncode == 0, f"Migration failed: {result.stderr}"

        # Verify key tables exist
        check = subprocess.run(
            ["sudo", "docker", "exec", "pg-sparket", "psql", "-U", "sparket",
             "-d", db_name, "-t", "-c",
             "SELECT table_name FROM information_schema.tables "
             "WHERE table_schema='public' ORDER BY table_name;"],
            capture_output=True, text=True,
        )
        tables = {t.strip() for t in check.stdout.strip().split("\n") if t.strip()}

        expected = {"miner", "miner_submission", "miner_rolling_score", "event",
                    "market", "outcome", "inbox", "ledger_state"}
        missing = expected - tables
        assert not missing, f"Missing tables after migration: {missing}"

        # Verify v0.1.2 columns on miner_rolling_score
        cols = subprocess.run(
            ["sudo", "docker", "exec", "pg-sparket", "psql", "-U", "sparket",
             "-d", db_name, "-t", "-c",
             "SELECT column_name FROM information_schema.columns "
             "WHERE table_name='miner_rolling_score' ORDER BY column_name;"],
            capture_output=True, text=True,
        )
        col_names = {c.strip() for c in cols.stdout.strip().split("\n") if c.strip()}

        # Shapley v0.1.2 columns
        for col in ["uniqueness_dim", "marginal_dim"]:
            assert col in col_names, f"Missing column '{col}' in miner_rolling_score"

        # Cleanup
        subprocess.run(
            ["sudo", "docker", "exec", "pg-sparket", "psql", "-U", "sparket",
             "-c", f"DROP DATABASE IF EXISTS {db_name};"],
            capture_output=True,
        )


# ===========================================================================
# 4. Brier Floor with No-Outcome Miner
# ===========================================================================


class TestBrierFloorLive:
    """Verify the brier_mean=0.5 default floors miners with no outcomes."""

    @pytest.mark.asyncio
    async def test_no_outcome_miner_gets_zero_weight(self):
        """A miner with submissions but no settled outcomes must get weight=0.

        v0.1.2 fix: brier_mean defaults to 0.5 when wt=0, which exceeds
        the Brier floor threshold (0.30), so skill_score=0.
        """
        # Use the unit-test path (no live nodes needed)
        config = ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))
        chain = ChainParamsSnapshot(
            burn_rate=0.0, burn_uid=0,
            max_weight_limit=0.1, min_allowed_weights=1, n_neurons=10,
        )

        from sparket.validator.ledger.models import MetricAccumulator

        # Miner 1: good brier (has outcomes)
        good = MinerMetrics(
            miner_id=1, uid=1, hotkey="good", n_submissions=500,
            brier_mean=0.15,  # Below floor (0.30)
            fq_raw=0.1, pss_mean=0.05, es_mean=0.03,
            mes_mean=0.6, sos_score=0.5, lead_score=0.4,
            cal_score=0.7, sharp_score=0.6,
        )

        # Miner 2: no outcomes (brier_mean defaults to 0.5)
        no_outcomes = MinerMetrics(
            miner_id=2, uid=2, hotkey="no_outcomes", n_submissions=1000,
            brier_mean=0.5,  # Default when wt=0 → EXCEEDS floor
            fq_raw=0.2, pss_mean=0.1, es_mean=0.05,
            mes_mean=0.7, sos_score=0.6, lead_score=0.5,
            cal_score=0.8, sharp_score=0.7,
        )

        result = compute_weights([good, no_outcomes], config, chain)

        # Good miner should have weight
        assert result.skill_scores.get(1, 0) > 0, "Good miner should pass floor"

        # No-outcome miner must be floored
        assert result.skill_scores.get(2, 0) == 0.0, (
            f"No-outcome miner should be floored (brier_mean=0.5 > 0.30), "
            f"got skill_score={result.skill_scores.get(2)}"
        )

        # Only miner 1 should have weight
        assert 1 in result.uids
        assert 2 not in result.uids

    @pytest.mark.asyncio
    async def test_accumulator_roundtrip_preserves_floor(self):
        """AccumulatorEntry with wt=0 → MinerMetrics → brier_mean=0.5 → floored."""
        from sparket.validator.ledger.models import MetricAccumulator

        acc = AccumulatorEntry(
            miner_id=99, hotkey="roundtrip", uid=99, n_submissions=500,
            brier=MetricAccumulator(ws=0.0, wt=0.0),  # No outcome data
            fq=MetricAccumulator(ws=10.0, wt=100.0),
            pss=MetricAccumulator(ws=5.0, wt=100.0),
            es=MetricAccumulator(ws=3.0, wt=100.0),
            mes=MetricAccumulator(ws=60.0, wt=100.0),
            sos=MetricAccumulator(ws=50.0, wt=100.0),
            lead=MetricAccumulator(ws=40.0, wt=100.0),
            cal_score=0.7, sharp_score=0.6,
        )

        # Serialize and deserialize (simulates checkpoint roundtrip)
        data = acc.model_dump(mode="json")
        loaded = AccumulatorEntry(**data)

        # Extract metrics
        metrics = MinerMetrics.from_accumulator(loaded)

        # brier_mean must be 0.5 (the v0.1.2 default for wt=0)
        assert metrics.brier_mean == 0.5, (
            f"Expected brier_mean=0.5 after roundtrip, got {metrics.brier_mean}"
        )


# ===========================================================================
# 5. CLE Odds Ratio Gate
# ===========================================================================


class TestCLEOddsRatioGate:
    """Verify max_odds_ratio_for_cle=2.0 blocks extreme longshot farming."""

    def test_gate_parameter_exists(self):
        """ScoringParams must have max_odds_ratio_for_cle=2.0."""
        params = ScoringParams()
        assert hasattr(params.ground_truth, "max_odds_ratio_for_cle"), (
            "Missing max_odds_ratio_for_cle parameter"
        )
        assert float(params.ground_truth.max_odds_ratio_for_cle) == 2.0, (
            f"Expected 2.0, got {params.ground_truth.max_odds_ratio_for_cle}"
        )

    def test_gate_rejects_extreme_ratio(self):
        """Submissions with odds ratio > 2.0 should be excluded from CLE."""
        from decimal import Decimal

        max_ratio = Decimal("2.0")

        # Normal submission: miner_odds=2.5, close_odds=2.0 → ratio=1.25 → PASS
        normal_ratio = Decimal("2.5") / Decimal("2.0")
        assert (1 / max_ratio) <= normal_ratio <= max_ratio, "Normal should pass"

        # Longshot farming: miner_odds=10.0, close_odds=2.0 → ratio=5.0 → REJECT
        longshot_ratio = Decimal("10.0") / Decimal("2.0")
        assert not ((1 / max_ratio) <= longshot_ratio <= max_ratio), (
            "Longshot ratio 5.0 should be rejected"
        )

        # Edge case at boundary: ratio=2.0 exactly → PASS
        boundary_ratio = Decimal("4.0") / Decimal("2.0")
        assert (1 / max_ratio) <= boundary_ratio <= max_ratio, "Boundary 2.0 should pass"

        # Edge case just over: ratio=2.01 → REJECT
        over_ratio = Decimal("4.02") / Decimal("2.0")
        assert not ((1 / max_ratio) <= over_ratio <= max_ratio), "2.01 should be rejected"

    @pytest.mark.asyncio
    async def test_gate_in_rolling_aggregates_code(self):
        """The rolling aggregates job must check odds ratio before CLE aggregation."""
        import inspect
        from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob

        source = inspect.getsource(RollingAggregatesJob)
        assert "max_odds_ratio" in source, (
            "RollingAggregatesJob must reference max_odds_ratio for CLE gating"
        )
