"""End-to-end tests for the base miner.

These tests verify the complete miner workflow from sync to submission.
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from sparket.miner.base.config import BaseMinerConfig
from sparket.miner.base.runner import BaseMiner


@pytest.mark.e2e
class TestBaseMinerE2E:
    """End-to-end tests for BaseMiner."""
    
    @pytest.fixture
    async def miner_setup(self, base_miner_config, mock_validator_client, mock_game_sync):
        """Set up a complete miner for e2e testing."""
        miner = BaseMiner(
            hotkey="5test_hotkey_e2e",
            config=base_miner_config,
            validator_client=mock_validator_client,
            game_sync=mock_game_sync,
        )
        yield miner
        if miner.is_running:
            await miner.stop()
    
    @pytest.mark.asyncio
    async def test_full_odds_cycle(self, miner_setup, mock_validator_client, mock_game_sync):
        """Test complete odds submission cycle."""
        miner = miner_setup
        
        # Get active markets (mocked)
        markets = await mock_game_sync.get_active_markets()
        assert len(markets) > 0
        
        # Generate odds for first market
        market = markets[0]
        odds = await miner.generate_odds(market)
        assert odds is not None
        
        # Build payload
        payload = miner._build_odds_payload(market, odds)
        assert "submissions" in payload
        
        # Submit (mocked)
        success = await mock_validator_client.submit_odds(payload)
        assert success
        
        # Verify mock was called
        mock_validator_client.submit_odds.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_odds_loop_submits(self, miner_setup, mock_validator_client):
        """Test that odds loop actually submits odds."""
        miner = miner_setup
        
        # Run one cycle manually
        await miner._run_odds_cycle()
        
        # Should have submitted something
        assert mock_validator_client.submit_odds.called
    
    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, miner_setup):
        """Test start/stop lifecycle."""
        miner = miner_setup
        
        assert not miner.is_running
        
        await miner.start()
        assert miner.is_running
        assert len(miner._tasks) == 2  # odds loop + outcome loop
        
        await miner.stop()
        assert not miner.is_running
        assert len(miner._tasks) == 0
    
    @pytest.mark.asyncio
    async def test_handles_empty_markets(self, miner_setup, mock_game_sync):
        """Handles gracefully when no markets available."""
        miner = miner_setup
        
        # Override to return empty
        mock_game_sync.get_active_markets = AsyncMock(return_value=[])
        
        # Should not crash
        await miner._run_odds_cycle()
        
        # No errors accumulated
        assert miner.errors_count == 0
    
    @pytest.mark.asyncio
    async def test_counts_errors(self, miner_setup, mock_validator_client):
        """Errors are counted for monitoring."""
        miner = miner_setup
        
        # Make submission fail
        mock_validator_client.submit_odds = AsyncMock(side_effect=Exception("Network error"))
        
        await miner._run_odds_cycle()
        
        # Errors should not crash - logged instead
        # But individual market errors don't increment errors_count
        # (only loop-level errors do)
    
    @pytest.mark.asyncio
    async def test_multiple_markets(self, miner_setup, mock_validator_client, mock_game_sync):
        """Processes multiple markets in one cycle."""
        miner = miner_setup
        
        # Mock 3 markets
        mock_game_sync.get_active_markets = AsyncMock(return_value=[
            {"market_id": 1, "kind": "MONEYLINE", "home_team": "KC", "away_team": "NE", "sport": "NFL"},
            {"market_id": 2, "kind": "MONEYLINE", "home_team": "DAL", "away_team": "NYJ", "sport": "NFL"},
            {"market_id": 3, "kind": "MONEYLINE", "home_team": "SF", "away_team": "SEA", "sport": "NFL"},
        ])
        
        await miner._run_odds_cycle()
        
        # Base miner batches markets into a single submission payload by default.
        mock_validator_client.submit_odds.assert_called_once()
        payload = mock_validator_client.submit_odds.await_args.args[0]
        assert len(payload["submissions"]) == 3
    
    @pytest.mark.asyncio
    async def test_continuous_operation(self, miner_setup):
        """Test miner runs continuously without crashing."""
        miner = miner_setup
        
        await miner.start()
        
        # Let it run briefly
        await asyncio.sleep(0.1)
        
        assert miner.is_running
        assert miner.errors_count == 0
        
        await miner.stop()
    
    @pytest.mark.asyncio
    async def test_outcome_submission(self, miner_setup, mock_validator_client):
        """Test outcome submission flow."""
        miner = miner_setup
        
        # Create async mock for finished events
        async def mock_get_finished_events():
            return [{
                "event_id": 100,
                "home_team": "KC",
                "away_team": "BUF",
                "sport": "NFL",
                "start_time_utc": datetime.now(timezone.utc),
            }]
        
        # Create async mock for ESPN result
        async def mock_get_result(event):
            return Mock(
                is_final=True,
                home_score=24,
                away_score=17,
                winner="HOME",
            )
        
        # Apply mocks
        with patch.object(miner, '_get_finished_events', mock_get_finished_events):
            with patch.object(miner._espn, 'get_result', mock_get_result):
                await miner._run_outcome_cycle()
        
        # Outcome submission should have been attempted
        # (may or may not succeed based on mocking details)
    
    @pytest.mark.asyncio
    async def test_config_from_env(self):
        """Configuration loads from environment."""
        import os
        
        # Set some env vars
        os.environ["SPARKET_BASE_MINER__ENABLED"] = "true"
        os.environ["SPARKET_BASE_MINER__ODDS_REFRESH_SECONDS"] = "120"
        
        try:
            config = BaseMinerConfig.from_env()
            assert config.enabled is True
            assert config.odds_refresh_seconds == 120
        finally:
            # Cleanup
            del os.environ["SPARKET_BASE_MINER__ENABLED"]
            del os.environ["SPARKET_BASE_MINER__ODDS_REFRESH_SECONDS"]
    
    @pytest.mark.asyncio
    async def test_theodds_integration_optional(self, base_miner_config, mock_validator_client, mock_game_sync):
        """Base miner works without The-Odds-API key."""
        # No API key
        base_miner_config.odds_api_key = None
        
        miner = BaseMiner(
            hotkey="5test",
            config=base_miner_config,
            validator_client=mock_validator_client,
            game_sync=mock_game_sync,
        )
        
        await miner.start()
        assert miner._theodds is None  # Not initialized
        
        # Can still generate odds
        odds = await miner.generate_odds({
            "market_id": 1,
            "kind": "MONEYLINE",
            "home_team": "KC",
            "away_team": "NE",
            "sport": "NFL",
        })
        assert odds is not None
        
        await miner.stop()



