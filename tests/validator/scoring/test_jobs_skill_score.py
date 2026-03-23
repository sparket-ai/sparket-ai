"""Integration tests for skill score job."""

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
import numpy as np

from sparket.validator.scoring.jobs.skill_score import (
    SkillScoreJob,
    make_miner_key,
)



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
    return SkillScoreJob(mock_db, mock_logger)


def make_rolling_score(
    miner_id: int,
    miner_hotkey: str,
    fq_raw: float = 0.2,
    brier_mean: float = 0.4,
    pss_mean: float = 0.15,
    cal_score: float = 0.7,
    sharp_score: float = 0.6,
    es_adj: float = 1.5,
    mes_mean: float = 0.85,
    sos_score: float = 0.65,
    lead_score: float = 0.55,
):
    """Create a mock rolling score row."""
    return {
        "miner_id": miner_id,
        "miner_hotkey": miner_hotkey,
        "fq_raw": fq_raw,
        "brier_mean": brier_mean,
        "pss_mean": pss_mean,
        "cal_score": cal_score,
        "sharp_score": sharp_score,
        "es_adj": es_adj,
        "mes_mean": mes_mean,
        "sos_score": sos_score,
        "lead_score": lead_score,
    }


class TestMakeMinerKey:
    """Tests for make_miner_key function."""

    def test_creates_key(self):
        key = make_miner_key(1, "abc123")
        assert key == "1:abc123"

    def test_different_miners_different_keys(self):
        key1 = make_miner_key(1, "abc")
        key2 = make_miner_key(2, "abc")
        assert key1 != key2


class TestSkillScoreJobInit:
    """Tests for job initialization."""

    def test_job_id(self, job):
        """Should have correct JOB_ID."""
        assert job.JOB_ID == "skill_score_v2"

    def test_has_params(self, job):
        """Should load scoring params."""
        assert job.params is not None


class TestSkillScoreJobExecute:
    """Tests for execute method."""

    async def test_no_miners(self, mock_db, mock_logger):
        """Should handle no miners gracefully."""
        mock_db.read = AsyncMock(return_value=[])

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        mock_db.write.assert_not_called()

    async def test_computes_scores(self, mock_db, mock_logger):
        """Should compute skill scores for all miners."""
        mock_db.read = AsyncMock(
            return_value=[
                make_rolling_score(1, "hotkey1"),
                make_rolling_score(2, "hotkey2", fq_raw=0.3, es_adj=2.0),
                make_rolling_score(3, "hotkey3", fq_raw=0.1, es_adj=0.5),
            ]
        )

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        # Should write 3 results
        assert mock_db.write.call_count == 3

    async def test_skill_score_in_range(self, mock_db, mock_logger):
        """Skill scores should be in [0, 1] range."""
        mock_db.read = AsyncMock(
            return_value=[
                make_rolling_score(1, "h1", fq_raw=0.5, es_adj=1.0),
                make_rolling_score(2, "h2", fq_raw=0.1, es_adj=0.2),
            ]
        )

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        for call in mock_db.write.call_args_list:
            params = call.kwargs.get("params") or call[1].get("params")
            skill = params["skill_score"]
            assert 0 <= skill <= 1, f"Skill score {skill} out of range"

    async def test_handles_null_values(self, mock_db, mock_logger):
        """Should handle null metric values."""
        mock_db.read = AsyncMock(
            return_value=[
                {
                    "miner_id": 1,
                    "miner_hotkey": "h1",
                    "fq_raw": None,
                    "brier_mean": None,
                    "pss_mean": None,
                    "cal_score": None,
                    "sharp_score": None,
                    "es_adj": None,
                    "mes_mean": None,
                    "sos_score": None,
                    "lead_score": None,
                },
            ]
        )

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        # Should still write (with default values)
        mock_db.write.assert_called()

    async def test_normalizes_fq_and_edge(self, mock_db, mock_logger):
        """FQ and edge scores should be normalized across miners."""
        mock_db.read = AsyncMock(
            return_value=[
                make_rolling_score(1, "h1", fq_raw=0.1, es_adj=0.5),
                make_rolling_score(2, "h2", fq_raw=0.5, es_adj=2.5),
                make_rolling_score(3, "h3", fq_raw=0.3, es_adj=1.5),
            ]
        )

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        # Check that different FQ values result in different fq_scores
        params_list = [
            call.kwargs.get("params") or call[1].get("params")
            for call in mock_db.write.call_args_list
        ]

        fq_scores = [p["fq_score"] for p in params_list]
        assert len(set(fq_scores)) > 1, "FQ scores should vary"

    async def test_uses_percentile_when_miners_small(self, mock_db, mock_logger, monkeypatch):
        """Should fall back to percentile normalization for small miner counts."""
        from decimal import Decimal
        
        mock_db.read = AsyncMock(
            return_value=[
                make_rolling_score(1, "h1", pss_mean=0.1, es_adj=0.5),
                make_rolling_score(2, "h2", pss_mean=0.9, es_adj=2.5),
            ]
        )

        from sparket.validator.scoring.jobs import skill_score as skill_mod

        def fake_percentile(values):
            return np.array([0.2, 0.8], dtype=np.float64)

        def fail_zscore(values, alpha=1.0):
            raise AssertionError("zscore should not be used for small miner counts")

        monkeypatch.setattr(skill_mod, "normalize_percentile", fake_percentile)
        monkeypatch.setattr(skill_mod, "normalize_zscore_logistic", fail_zscore)

        job = SkillScoreJob(mock_db, mock_logger)
        # Use model_copy to create modified immutable pydantic models
        job.params = job.params.model_copy(update={
            "normalization": job.params.normalization.model_copy(update={"min_count_for_zscore": 5}),
            "skill_score_weights": job.params.skill_score_weights.model_copy(update={
                "w_outcome_accuracy": Decimal("0"),  # w_forecast alias
                "w_outcome_relative": Decimal("1"),  # w_skill alias
                "w_odds_edge": Decimal("0"),         # w_econ alias
                "w_info_adv": Decimal("0"),          # w_info alias
            }),
        })

        await job.execute()

        params_list = [
            call.kwargs.get("params") or call[1].get("params")
            for call in mock_db.write.call_args_list
        ]
        skill_scores = [p["skill_score"] for p in params_list]
        # Under Cobb-Douglas, scores are multiplicative products of clamped dimensions.
        # The key assertion is that percentile normalization was used (zscore would raise)
        # and that scores are ordered correctly (miner 2 > miner 1).
        assert len(skill_scores) == 2
        assert skill_scores[1] >= skill_scores[0], "Higher PSS miner should score >= lower"


class TestToFloatSafe:
    """Tests for _to_float_safe method."""

    def test_valid_float(self, job):
        assert job._to_float_safe(0.5, 0.0) == 0.5

    def test_valid_int(self, job):
        assert job._to_float_safe(1, 0.0) == 1.0

    def test_valid_string(self, job):
        assert job._to_float_safe("0.75", 0.0) == 0.75

    def test_none_returns_default(self, job):
        assert job._to_float_safe(None, 0.5) == 0.5

    def test_invalid_returns_default(self, job):
        assert job._to_float_safe("not a number", 0.3) == 0.3

