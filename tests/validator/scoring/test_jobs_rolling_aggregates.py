"""Integration tests for rolling aggregates job."""

import logging
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import numpy as np
import pytest

from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob



@pytest.fixture
def mock_db():
    """Create mock database manager."""
    db = MagicMock()
    db.write = AsyncMock()
    db.read = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_logger():
    """Create mock logger."""
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def job(mock_db, mock_logger):
    """Create job instance."""
    return RollingAggregatesJob(mock_db, mock_logger)


def make_submission(
    submission_id: int,
    submitted_at: datetime = None,
    cle: float = None,
    clv_prob: float = None,
    pss_brier: float = None,
    pss_log: float = None,
    minutes_to_close: int = 1440,  # Default 1 day
    brier: float = None,
    miner_odds: float = 2.0,
    close_odds: float = 2.0,
):
    """Create a mock submission row."""
    if submitted_at is None:
        submitted_at = datetime.now(timezone.utc) - timedelta(days=1)
    return {
        "submission_id": submission_id,
        "submitted_at": submitted_at,
        "cle": cle,
        "clv_prob": clv_prob,
        "pss_brier": pss_brier,
        "pss_log": pss_log,
        "minutes_to_close": minutes_to_close,
        "brier": brier,
        "miner_odds": miner_odds,
        "close_odds": close_odds,
    }


class TestRollingAggregatesJobInit:
    """Tests for job initialization."""

    def test_job_id(self, job):
        """Should have correct JOB_ID."""
        assert job.JOB_ID == "rolling_aggregates_v2"

    def test_has_params(self, job):
        """Should load scoring params."""
        assert job.params is not None


class TestRollingAggregatesJobExecute:
    """Tests for execute method."""

    async def test_no_miners(self, mock_db, mock_logger):
        """Should handle no miners gracefully."""
        mock_db.read = AsyncMock(return_value=[])

        job = RollingAggregatesJob(mock_db, mock_logger)
        await job.execute()

        # Should not write anything
        mock_db.write.assert_not_called()

    async def test_processes_miners(self, mock_db, mock_logger):
        """Should process each miner."""
        now = datetime.now(timezone.utc)

        # First call returns miners, second returns submissions
        mock_db.read = AsyncMock(
            side_effect=[
                # Miners
                [
                    {"miner_id": 1, "miner_hotkey": "hotkey1"},
                    {"miner_id": 2, "miner_hotkey": "hotkey2"},
                ],
                # Submissions for miner 1
                [
                    make_submission(1, now - timedelta(days=1), cle=0.05, clv_prob=0.02, pss_brier=0.2, brier=0.3),
                    make_submission(2, now - timedelta(days=2), cle=0.03, clv_prob=0.01, pss_brier=0.15, brier=0.35),
                ],
                # Submissions for miner 2
                [
                    make_submission(3, now - timedelta(hours=12), cle=0.08, clv_prob=0.03, pss_brier=0.25, brier=0.25),
                ],
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        await job.execute()

        # Should have written 2 results
        assert mock_db.write.call_count == 2

    async def test_handles_no_submissions(self, mock_db, mock_logger):
        """Should handle miners with no submissions."""
        mock_db.read = AsyncMock(
            side_effect=[
                [{"miner_id": 1, "miner_hotkey": "hotkey1"}],
                [],  # No submissions
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        await job.execute()

        # Should not write (no metrics)
        mock_db.write.assert_not_called()

    async def test_checkpoints_progress(self, mock_db, mock_logger):
        """Should checkpoint at intervals."""
        now = datetime.now(timezone.utc)

        # Create many miners to trigger checkpoint
        miners = [{"miner_id": i, "miner_hotkey": f"hotkey{i}"} for i in range(60)]
        submissions = [make_submission(i, now - timedelta(hours=i), cle=0.05) for i in range(3)]

        mock_db.read = AsyncMock(
            side_effect=[miners] + [submissions] * 60
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        job.CHECKPOINT_INTERVAL = 10
        await job.execute()

        # Should have processed all miners
        assert job.items_processed == 60


class TestComputeMinerMetrics:
    """Tests for _compute_miner_metrics method."""

    async def test_computes_es_metrics(self, mock_db, mock_logger):
        """Should compute ES metrics from CLE."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1), cle=0.05),
                make_submission(2, now - timedelta(days=2), cle=0.03),
                make_submission(3, now - timedelta(days=3), cle=0.07),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1,
            miner_hotkey="test",
            window_start=now - timedelta(days=30),
            window_end=now,
        )

        assert metrics is not None
        assert metrics["n_submissions"] == 3
        assert metrics["es_mean"] > 0.0
        assert metrics["es_std"] >= 0.0

    async def test_computes_mes_metrics(self, mock_db, mock_logger):
        """Should compute MES from CLV prob."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1), clv_prob=0.02),
                make_submission(2, now - timedelta(days=2), clv_prob=-0.01),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1,
            miner_hotkey="test",
            window_start=now - timedelta(days=30),
            window_end=now,
        )

        assert metrics is not None
        assert metrics["mes_mean"] > 0.0

    async def test_computes_fq_metrics(self, mock_db, mock_logger):
        """Should compute FQ from Brier score."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1), pss_brier=0.2, brier=0.3),
                make_submission(2, now - timedelta(days=2), pss_brier=0.15, brier=0.35),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1,
            miner_hotkey="test",
            window_start=now - timedelta(days=30),
            window_end=now,
        )

        assert metrics is not None
        assert metrics["pss_mean"] > 0.0  # PSS with time bonus
        assert metrics["brier_mean"] > 0.0  # Should have brier
        # FQ = 1 - 2*brier, so 1 - 2*0.325 ≈ 0.35
        assert 0.0 < metrics["fq_raw"] < 1.0

    async def test_handles_null_values(self, mock_db, mock_logger):
        """Should handle null metric values."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1)),  # All nulls
                make_submission(2, now - timedelta(days=2), cle=0.05),  # Only CLE
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1,
            miner_hotkey="test",
            window_start=now - timedelta(days=30),
            window_end=now,
        )

        assert metrics is not None
        # Should have ES metrics from the one valid CLE
        assert metrics["n_submissions"] == 2

    async def test_empty_submissions(self, mock_db, mock_logger):
        """Should return None for no submissions."""
        mock_db.read = AsyncMock(return_value=[])

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1,
            miner_hotkey="test",
            window_start=datetime.now(timezone.utc) - timedelta(days=30),
            window_end=datetime.now(timezone.utc),
        )

        assert metrics is None


class TestOddsRatioGating:
    """Tests for CLE odds-ratio gating in _compute_miner_metrics."""

    async def test_extreme_odds_excluded_from_cle(self, mock_db, mock_logger):
        """Submissions with miner_odds/close_odds > 2.0 should be excluded from ES."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                # Reasonable: odds ratio 2.1/2.0 = 1.05 → included
                make_submission(1, now - timedelta(days=1), cle=0.05, miner_odds=2.1, close_odds=2.0),
                # Extreme: odds ratio 5.3/1.9 = 2.79 → excluded
                make_submission(2, now - timedelta(days=1), cle=1.0, miner_odds=5.3, close_odds=1.9),
                # Extreme: odds ratio 4.1/2.0 = 2.05 → excluded
                make_submission(3, now - timedelta(days=1), cle=0.9, miner_odds=4.1, close_odds=2.0),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1, miner_hotkey="test",
            window_start=now - timedelta(days=30), window_end=now,
        )

        assert metrics is not None
        # Only the first submission's CLE (0.05) should contribute
        assert metrics["es_mean"] == pytest.approx(0.05, abs=0.01)

    async def test_reasonable_odds_included(self, mock_db, mock_logger):
        """Submissions with odds ratio within threshold should all contribute."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1), cle=0.05, miner_odds=2.1, close_odds=2.0),
                make_submission(2, now - timedelta(days=2), cle=0.03, miner_odds=1.95, close_odds=2.0),
                make_submission(3, now - timedelta(days=3), cle=-0.02, miner_odds=1.85, close_odds=2.0),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1, miner_hotkey="test",
            window_start=now - timedelta(days=30), window_end=now,
        )

        assert metrics is not None
        # All three should contribute — es_mean should reflect their weighted average
        assert metrics["es_mean"] > -0.1
        assert metrics["es_mean"] < 0.1

    async def test_missing_odds_excludes_cle(self, mock_db, mock_logger):
        """Submissions without odds data should not contribute to CLE."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(
            return_value=[
                make_submission(1, now - timedelta(days=1), cle=0.05, miner_odds=None, close_odds=None),
            ]
        )

        job = RollingAggregatesJob(mock_db, mock_logger)
        metrics = await job._compute_miner_metrics(
            miner_id=1, miner_hotkey="test",
            window_start=now - timedelta(days=30), window_end=now,
        )

        assert metrics is not None
        assert metrics["es_mean"] == 0.0  # No eligible CLE values


class TestApplyShrinkage:
    """Tests for _apply_shrinkage method."""

    def test_shrinks_toward_mean(self, mock_db, mock_logger):
        """Should shrink values toward population mean."""
        job = RollingAggregatesJob(mock_db, mock_logger)

        # Miner with few samples - should shrink more
        low_sample = {
            "miner_id": 1,
            "miner_hotkey": "test1",
            "n_submissions": 5,
            "n_eff": 3.0,
            "es_mean": 0.1,
            "es_std": 0.02,
            "es_adj": 5.0,
            "mes_mean": 0.95,
            "pss_mean": 0.3,
            "fq_raw": 0.3,
        }

        # Miner with many samples - should shrink less
        high_sample = {
            "miner_id": 2,
            "miner_hotkey": "test2",
            "n_submissions": 500,
            "n_eff": 300.0,
            "es_mean": 0.08,
            "es_std": 0.01,
            "es_adj": 8.0,
            "mes_mean": 0.92,
            "pss_mean": 0.25,
            "fq_raw": 0.25,
        }

        miner_metrics = {
            "1:test1": low_sample.copy(),
            "2:test2": high_sample.copy(),
        }

        result = job._apply_shrinkage(miner_metrics)

        # Low sample miner should be shrunk more toward population mean
        # High sample miner should be closer to original
        assert result["1:test1"]["es_mean"] != 0.1
        # High sample should have less shrinkage
        es_diff_low = abs(result["1:test1"]["es_mean"] - 0.1)
        es_diff_high = abs(result["2:test2"]["es_mean"] - 0.08)
        # Relative shrinkage should be higher for low sample
        assert es_diff_low / 0.1 > es_diff_high / 0.08


class TestPersistResults:
    """Tests for _persist_results method."""

    async def test_upserts_scores(self, mock_db, mock_logger):
        """Should upsert rolling scores."""
        job = RollingAggregatesJob(mock_db, mock_logger)

        metrics = {
            "1:test": {
                "miner_id": 1,
                "miner_hotkey": "test",
                "n_submissions": 10,
                "n_eff": 8.0,
                "es_mean": 0.05,
                "es_std": 0.02,
                "es_adj": 2.5,
                "mes_mean": 0.95,
                "pss_mean": 0.2,
                "fq_raw": 0.2,
                "brier_mean": 0.3,
            }
        }

        await job._persist_results(
            metrics,
            datetime.now(timezone.utc),
            30,
        )

        mock_db.write.assert_called_once()
        # Check params have correct types
        call_args = mock_db.write.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params")
        assert params["miner_id"] == 1
        assert isinstance(params["es_mean"], float)

