"""Tests for scoring pipeline fixes.

Covers:
1. Sportsbook auto-registration in SDIO ingestor
2. Snapshot pipeline JOIN correctness
3. Outcome ingestion for already-finished events
4. skill_dim column persist in SkillScoreJob
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Test Group 1: Sportsbook Registration
# ---------------------------------------------------------------------------


class TestSportsbookRegistration:
    """Tests for _ensure_sportsbooks() in sportsdata_ingestor."""

    @pytest.fixture
    def mock_db(self):
        db = MagicMock()
        db.write = AsyncMock(return_value=[])
        db.read = AsyncMock(return_value=[])
        return db

    @pytest.fixture
    def ingestor(self, mock_db):
        from sparket.validator.services.sportsdata_ingestor import SportsDataIngestor

        client = MagicMock()
        ing = SportsDataIngestor.__new__(SportsDataIngestor)
        ing.database = mock_db
        ing.provider_id = 1
        ing.client = client
        return ing

    def _make_odds(self, sportsbook: str):
        odds = MagicMock()
        odds.sportsbook = sportsbook
        return odds

    @pytest.mark.asyncio
    async def test_ensure_sportsbooks_upserts_new_books(self, ingestor, mock_db):
        """New sportsbook codes get upserted with correct provider_id."""
        # DB returns no existing codes
        mock_db.read = AsyncMock(return_value=[])
        # Write returns sportsbook_id for each upsert
        mock_db.write = AsyncMock(side_effect=[
            [(1,)],  # sportsbook upsert returns id=1
            0,       # bias seed
            [(2,)],  # sportsbook upsert returns id=2
            0,       # bias seed
        ])

        odds = [self._make_odds("FanDuel"), self._make_odds("DraftKings")]
        await ingestor._ensure_sportsbooks(odds)

        # Should have 4 writes: 2 upserts + 2 bias seeds
        assert mock_db.write.call_count == 4

        # First write should be sportsbook upsert for DraftKings (sorted)
        first_call = mock_db.write.call_args_list[0]
        assert first_call.kwargs["params"]["code"] == "DraftKings"
        assert first_call.kwargs["params"]["provider_id"] == 1

    @pytest.mark.asyncio
    async def test_ensure_sportsbooks_skips_existing(self, ingestor, mock_db):
        """Already-registered sportsbook codes are not re-inserted."""
        # DB returns FanDuel as already existing
        mock_db.read = AsyncMock(return_value=[{"code": "FanDuel"}])
        mock_db.write = AsyncMock(side_effect=[
            [(1,)],  # only DraftKings gets upserted
            0,       # bias seed
        ])

        odds = [self._make_odds("FanDuel"), self._make_odds("DraftKings")]
        await ingestor._ensure_sportsbooks(odds)

        # Only 2 writes: 1 upsert + 1 bias seed (FanDuel skipped)
        assert mock_db.write.call_count == 2

    @pytest.mark.asyncio
    async def test_ensure_sportsbooks_no_ops_when_all_exist(self, ingestor, mock_db):
        """No DB writes when all codes already registered."""
        mock_db.read = AsyncMock(return_value=[
            {"code": "FanDuel"},
            {"code": "DraftKings"},
        ])

        odds = [self._make_odds("FanDuel"), self._make_odds("DraftKings")]
        await ingestor._ensure_sportsbooks(odds)

        mock_db.write.assert_not_called()

    @pytest.mark.asyncio
    async def test_ensure_sportsbooks_handles_empty_odds(self, ingestor, mock_db):
        """No-op when odds list has no sportsbook codes."""
        odds_no_book = MagicMock()
        odds_no_book.sportsbook = None
        await ingestor._ensure_sportsbooks([odds_no_book])

        mock_db.read.assert_not_called()
        mock_db.write.assert_not_called()


# ---------------------------------------------------------------------------
# Test Group 2: Snapshot Pipeline JOIN
# ---------------------------------------------------------------------------


class TestSnapshotJoin:
    """Tests for _SELECT_MARKET_QUOTES JOIN correctness."""

    def test_query_matches_by_sportsbook_code(self):
        """The SQL query must join on s.code = pq.raw->>'sportsbook'."""
        from sparket.validator.scoring.ground_truth.snapshot_pipeline import (
            _SELECT_MARKET_QUOTES,
        )

        query_text = str(_SELECT_MARKET_QUOTES.text)
        assert "s.code = pq.raw->>'sportsbook'" in query_text, (
            "Snapshot pipeline JOIN must match sportsbook by code, not just provider_id"
        )

    def test_query_does_not_join_only_on_provider(self):
        """Ensure the old broken pattern (JOIN only on provider_id) is gone."""
        from sparket.validator.scoring.ground_truth.snapshot_pipeline import (
            _SELECT_MARKET_QUOTES,
        )

        query_text = str(_SELECT_MARKET_QUOTES.text)
        # Should NOT have the old single-condition join
        lines = [l.strip() for l in query_text.split("\n")]
        for line in lines:
            if "LEFT JOIN sportsbook" in line and "s.code" not in line:
                # The JOIN line itself won't have s.code, it's on the next line
                continue
        # Just verify the code condition exists somewhere after the JOIN
        join_idx = query_text.find("LEFT JOIN sportsbook")
        code_idx = query_text.find("s.code = pq.raw")
        assert code_idx > join_idx, "s.code condition must appear after the LEFT JOIN"


# ---------------------------------------------------------------------------
# Test Group 3: Outcome Ingestion
# ---------------------------------------------------------------------------


class TestOutcomeIngestion:
    """Tests for outcome ingestion fix in _refresh_schedule."""

    @pytest.fixture
    def mock_db(self):
        db = MagicMock()
        db.write = AsyncMock(return_value=0)
        db.read = AsyncMock(return_value=[])
        return db

    @pytest.fixture
    def ingestor(self, mock_db):
        from sparket.validator.services.sportsdata_ingestor import SportsDataIngestor

        ing = SportsDataIngestor.__new__(SportsDataIngestor)
        ing.database = mock_db
        ing.provider_id = 1
        return ing

    def _make_final_game(self, home_score=2, away_score=1):
        from sparket.providers.sportsdataio.enums import GameStatus

        game = MagicMock()
        game.status = GameStatus.FINAL
        game.home_score = home_score
        game.away_score = away_score
        game.game_id = 999
        return game

    @pytest.mark.asyncio
    async def test_record_outcomes_called_for_already_finished(self, ingestor, mock_db):
        """_record_outcomes runs even when _sync_event_status returns False."""
        # _sync_event_status returns 0 affected rows (already finished)
        mock_db.write = AsyncMock(return_value=0)
        # _record_outcomes calls read twice: markets query, then outcome exists check
        mock_db.read = AsyncMock(side_effect=[
            # _SELECT_MARKETS_FOR_EVENT returns one market
            [{"market_id": 100, "kind": "MONEYLINE", "line": None, "points_team_id": None}],
            # _CHECK_OUTCOME_EXISTS returns empty (no existing outcome)
            [],
        ])

        game = self._make_final_game()

        # Mock _resolve_market_result to return a valid result
        with patch.object(ingestor, "_resolve_market_result", return_value="HOME"):
            recorded = await ingestor._record_outcomes(
                event_id=1, game=game, home_team_id=10, away_team_id=20,
            )

        assert recorded == 1

    @pytest.mark.asyncio
    async def test_record_outcomes_skips_when_no_scores(self, ingestor, mock_db):
        """No outcomes recorded when game scores are None."""
        game = self._make_final_game(home_score=None, away_score=None)

        recorded = await ingestor._record_outcomes(
            event_id=1, game=game, home_team_id=10, away_team_id=20,
        )

        assert recorded == 0
        mock_db.read.assert_not_called()

    @pytest.mark.asyncio
    async def test_record_outcomes_idempotent(self, ingestor, mock_db):
        """Second call skips markets that already have outcomes."""
        # First read: markets exist. Second read: outcome already exists
        mock_db.read = AsyncMock(side_effect=[
            [{"market_id": 100, "kind": "MONEYLINE", "line": None, "points_team_id": None}],
            [(1,)],  # _CHECK_OUTCOME_EXISTS returns a row
        ])

        game = self._make_final_game()

        recorded = await ingestor._record_outcomes(
            event_id=1, game=game, home_team_id=10, away_team_id=20,
        )

        assert recorded == 0  # Skipped because outcome already exists


# ---------------------------------------------------------------------------
# Test Group 4: skill_dim Persist
# ---------------------------------------------------------------------------


class TestSkillDimPersist:
    """Tests for skill_dim column in SkillScoreJob."""

    def test_update_sql_includes_skill_dim(self):
        """_UPDATE_SKILL_SCORE must SET skill_dim."""
        from sparket.validator.scoring.jobs.skill_score import _UPDATE_SKILL_SCORE

        query_text = str(_UPDATE_SKILL_SCORE.text)
        assert "skill_dim = :skill_dim" in query_text, (
            "_UPDATE_SKILL_SCORE must include skill_dim = :skill_dim"
        )

    @pytest.mark.asyncio
    async def test_skill_score_job_persists_skill_dim(self):
        """SkillScoreJob.execute() writes skill_dim in params."""
        from sparket.validator.scoring.jobs.skill_score import SkillScoreJob

        mock_db = MagicMock()
        mock_logger = MagicMock(spec=logging.Logger)

        # 3 miners with rolling scores
        rolling_rows = [
            {
                "miner_id": i,
                "miner_hotkey": f"hotkey_{i}",
                "fq_raw": 0.2 + i * 0.05,
                "brier_mean": 0.4 - i * 0.02,
                "pss_mean": 0.1 + i * 0.05,
                "cal_score": 0.7,
                "sharp_score": 0.6,
                "es_adj": 1.5 + i * 0.1,
                "mes_mean": 0.85,
                "sos_score": 0.65,
                "lead_score": 0.55,
            }
            for i in range(3)
        ]

        mock_db.read = AsyncMock(return_value=rolling_rows)
        mock_db.write = AsyncMock()

        job = SkillScoreJob(mock_db, mock_logger)
        await job.execute()

        # Should write 3 times (one per miner)
        assert mock_db.write.call_count == 3

        # Each write should include skill_dim
        for call in mock_db.write.call_args_list:
            params = call.kwargs.get("params", call.args[1] if len(call.args) > 1 else {})
            assert "skill_dim" in params, "skill_dim must be in persist params"
            assert isinstance(params["skill_dim"], float), "skill_dim must be a float"
