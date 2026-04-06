"""Tests for bias update job."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import logging
import pytest

from sparket.validator.scoring.jobs.bias_update import BiasUpdateJob


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.write = AsyncMock()
    db.read = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_logger():
    return MagicMock(spec=logging.Logger)


class TestBiasUpdateJob:
    """Tests for BiasUpdateJob."""

    def test_job_id(self, mock_db, mock_logger):
        job = BiasUpdateJob(mock_db, mock_logger)
        assert job.JOB_ID == "bias_update_v1"

    async def test_no_outcomes_skips(self, mock_db, mock_logger):
        """Should skip gracefully when no new outcomes."""
        mock_db.read = AsyncMock(side_effect=[
            [],  # _get_last_update_time → no prior updates
            [],  # _load_current_bias
            [],  # _SELECT_NEW_SETTLED_OUTCOMES
        ])

        job = BiasUpdateJob(mock_db, mock_logger)
        await job.execute()

        # Should not write any bias updates
        mock_db.write.assert_not_called()

    async def test_collects_observations_and_updates_bias(self, mock_db, mock_logger):
        """Should collect book quotes for settled outcomes and update bias."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(side_effect=[
            # _get_last_update_time: no prior updates
            [],
            # _load_current_bias: one existing entry
            [
                {
                    "sportsbook_id": 1,
                    "sport_id": 2,
                    "market_kind": "TOTAL",
                    "bias_factor": Decimal("1.0"),
                    "variance": Decimal("0.01"),
                    "mean_squared_error": Decimal("0"),
                    "sample_count": 0,
                    "version": 1,
                }
            ],
            # _SELECT_NEW_SETTLED_OUTCOMES: one settled outcome
            [
                {
                    "outcome_id": 1,
                    "market_id": 100,
                    "result": "OVER",
                    "settled_at": now,
                    "market_kind": "TOTAL",
                    "sport_id": 2,
                    "start_time_utc": now - timedelta(hours=3),
                }
            ],
            # _SELECT_CLOSING_BOOK_QUOTES: book quotes for this market
            [
                {
                    "sportsbook_id": 1,
                    "side": "OVER",
                    "imp_prob": Decimal("0.52"),
                },
                {
                    "sportsbook_id": 1,
                    "side": "UNDER",
                    "imp_prob": Decimal("0.48"),
                },
            ],
        ])

        job = BiasUpdateJob(mock_db, mock_logger)
        await job.execute()

        # Should have written bias updates
        assert mock_db.write.call_count > 0

        # Check that bias was updated with correct observations
        write_calls = mock_db.write.call_args_list
        bias_writes = [c for c in write_calls if "bias_factor" in (c.kwargs.get("params") or c[1].get("params", {}))]
        assert len(bias_writes) > 0

        # The update should have sample_count > 0
        params = bias_writes[0].kwargs.get("params") or bias_writes[0][1]["params"]
        assert params["sample_count"] > 0

    async def test_skips_unmapped_books(self, mock_db, mock_logger):
        """Should skip quotes from unmapped books (sportsbook_id=0)."""
        now = datetime.now(timezone.utc)

        mock_db.read = AsyncMock(side_effect=[
            [],  # last update time
            [],  # current bias
            # one outcome
            [{"outcome_id": 1, "market_id": 100, "result": "HOME",
              "settled_at": now, "market_kind": "MONEYLINE", "sport_id": 2,
              "start_time_utc": now - timedelta(hours=3)}],
            # quotes — all unmapped (sportsbook_id=0)
            [
                {"sportsbook_id": 0, "side": "HOME", "imp_prob": Decimal("0.6")},
                {"sportsbook_id": 0, "side": "AWAY", "imp_prob": Decimal("0.4")},
            ],
        ])

        job = BiasUpdateJob(mock_db, mock_logger)
        await job.execute()

        # No bias writes — all quotes were unmapped
        mock_db.write.assert_not_called()

    async def test_outcome_hit_correctly_assigned(self, mock_db, mock_logger):
        """Outcome hit should be 1 for winning side, 0 for losing side."""
        job = BiasUpdateJob(mock_db, mock_logger)

        # Mock a TOTAL market where OVER won
        outcome = {
            "outcome_id": 1,
            "market_id": 100,
            "result": "OVER",
            "settled_at": datetime.now(timezone.utc),
            "market_kind": "TOTAL",
            "sport_id": 2,
            "start_time_utc": datetime.now(timezone.utc) - timedelta(hours=3),
        }

        mock_db.read = AsyncMock(return_value=[
            {"sportsbook_id": 1, "side": "OVER", "imp_prob": Decimal("0.55")},
            {"sportsbook_id": 1, "side": "UNDER", "imp_prob": Decimal("0.45")},
        ])

        observations = await job._collect_observations(outcome)

        assert len(observations) == 2

        over_obs = next(o for o in observations if o.outcome_hit == 1)
        under_obs = next(o for o in observations if o.outcome_hit == 0)

        assert over_obs.book_prob == Decimal("0.55")
        assert under_obs.book_prob == Decimal("0.45")
