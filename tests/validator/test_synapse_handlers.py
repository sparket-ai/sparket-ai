"""End-to-end tests for validator synapse handlers.

Tests the full flow:
- ODDS_PUSH → IngestOddsHandler → miner_submission table
- OUTCOME_PUSH → IngestOutcomeHandler → inbox table
- GAME_DATA_REQUEST → GameDataHandler → response with games/markets
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import bittensor as bt
import pytest

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.handlers.ingest.ingest_odds import IngestOddsHandler
from sparket.validator.handlers.ingest.ingest_outcome import IngestOutcomeHandler
from sparket.validator.handlers.data.game_data import GameDataHandler


def make_synapse_with_hotkey(synapse_type: SparketSynapseType, payload: dict, hotkey: str) -> SparketSynapse:
    """Create a synapse with a proper dendrite/hotkey for testing."""
    synapse = SparketSynapse(type=synapse_type, payload=payload)
    # Use model_construct to bypass validation for test setup
    terminal_info = bt.TerminalInfo.model_construct(hotkey=hotkey)
    object.__setattr__(synapse, "dendrite", terminal_info)
    return synapse


class MockDatabase:
    """In-memory mock database for testing."""
    
    def __init__(self):
        self.writes: List[Dict[str, Any]] = []
        self.games: List[Dict[str, Any]] = []
        self.markets: List[Dict[str, Any]] = []
        self.miners: Dict[str, int] = {}  # hotkey -> miner_id
        self.valid_market_ids: List[int] = []  # Markets within submission window
        self.valid_events: Dict[int, Dict] = {}  # event_id -> event info
    
    async def read(self, query, params=None, mappings=False):
        query_str = str(query)
        
        # Miner lookup by hotkey (security feature)
        if "FROM miner WHERE hotkey" in query_str:
            hotkey = params.get("hotkey") if params else None
            if hotkey in self.miners:
                return [{"miner_id": self.miners[hotkey]}]
            return []
        
        # Valid markets query (within time window)
        if "FROM market m" in query_str and "JOIN event e" in query_str:
            return [{"market_id": mid} for mid in self.valid_market_ids]
        
        # Event validation query
        if "FROM event e" in query_str and "WHERE e.event_id" in query_str:
            event_id = params.get("event_id") if params else None
            if event_id in self.valid_events:
                return [self.valid_events[event_id]]
            return []
        
        # Check for duplicate outcome submission
        if "FROM inbox WHERE dedupe_key" in query_str:
            return []  # No duplicates
        
        # Game data handler queries
        if "FROM event e" in query_str and "JOIN league" in query_str:
            return self.games
        if "FROM market m" in query_str and "event_id = ANY" in query_str:
            return self.markets
        
        return []
    
    async def write(self, query, params=None, return_rows=False, mappings=False):
        self.writes.append({"query": str(query), "params": params})
        return [] if return_rows else 1
    
    async def write_many(self, query, params_list):
        """Batch write - records each params dict as a separate write."""
        for params in params_list:
            self.writes.append({"query": str(query), "params": params})
        return len(params_list)


class TestIngestOddsHandler:
    """Tests for ODDS_PUSH handling."""
    
    @pytest.fixture
    def db(self):
        db = MockDatabase()
        # Register a miner (security: miner_id derived from hotkey lookup)
        db.miners["miner_hotkey_123"] = 42
        db.miners["miner_123"] = 1
        # Mark markets as valid (within submission window)
        db.valid_market_ids = [100, 101]
        return db
    
    @pytest.fixture
    def handler(self, db):
        return IngestOddsHandler(db)
    
    async def test_handles_valid_odds_push(self, handler, db):
        """Valid ODDS_PUSH should persist to miner_submission and emit event."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.ODDS_PUSH,
            {
                "submissions": [
                    {
                        "market_id": 100,
                        "kind": "moneyline",
                        "prices": [
                            {"side": "home", "odds_eu": 1.91, "imp_prob": 0.5236},
                            {"side": "away", "odds_eu": 2.05, "imp_prob": 0.4878},
                        ],
                    }
                ],
            },
            "miner_hotkey_123",
        )
        
        event = await handler.handle_synapse(synapse)
        
        # Should emit MinerOddsPushed event
        assert event is not None
        assert event.event_data["miner_hotkey"] == "miner_hotkey_123"
        
        # Should have written 2 rows (home + away)
        assert len(db.writes) == 2
        
        # Check first write params - miner_id derived from hotkey lookup (42)
        params = db.writes[0]["params"]
        assert params["miner_id"] == 42  # From hotkey lookup, not payload
        assert params["market_id"] == 100
        assert params["miner_hotkey"] == "miner_hotkey_123"
        assert params["side"] in ("HOME", "AWAY")
        assert params["odds_eu"] in (1.91, 2.05)
    
    async def test_rejects_unregistered_miner(self, handler, db):
        """Miner not registered in DB should be rejected."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.ODDS_PUSH,
            {"submissions": [{"market_id": 100, "prices": [{"side": "home", "odds_eu": 1.91}]}]},
            "unknown_miner",
        )
        
        event = await handler.handle_synapse(synapse)
        
        assert event is not None
        assert event.event_data["payload"]["error"] == "miner_not_registered"
        assert len(db.writes) == 0
    
    async def test_rejects_out_of_bounds_odds(self, handler, db):
        """Odds outside bounds (1.01-1000) should be rejected."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.ODDS_PUSH,
            {
                "submissions": [
                    {
                        "market_id": 100,
                        "prices": [
                            {"side": "home", "odds_eu": 1.005},  # Too low
                            {"side": "away", "odds_eu": 5000},   # Too high
                        ],
                    }
                ],
            },
            "miner_hotkey_123",
        )
        
        event = await handler.handle_synapse(synapse)
        
        # Both prices rejected
        assert len(db.writes) == 0
    
    async def test_ignores_non_odds_synapse(self, handler, db):
        """Non-ODDS_PUSH synapses should return None."""
        synapse = SparketSynapse(
            type=SparketSynapseType.OUTCOME_PUSH,
            payload={"event_id": "123"},
        )
        
        event = await handler.handle_synapse(synapse)
        
        assert event is None
        assert len(db.writes) == 0
    
    async def test_handles_multiple_markets(self, handler, db):
        """Multiple market submissions should all be persisted."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.ODDS_PUSH,
            {
                "submissions": [
                    {
                        "market_id": 100,
                        "kind": "moneyline",
                        "prices": [
                            {"side": "home", "odds_eu": 1.91, "imp_prob": 0.52},
                            {"side": "away", "odds_eu": 2.05, "imp_prob": 0.48},
                        ],
                    },
                    {
                        "market_id": 101,
                        "kind": "spread",
                        "prices": [
                            {"side": "home", "odds_eu": 1.95, "imp_prob": 0.51},
                            {"side": "away", "odds_eu": 1.95, "imp_prob": 0.51},
                        ],
                    },
                ],
            },
            "miner_123",
        )
        
        event = await handler.handle_synapse(synapse)
        
        # Should have 4 writes (2 markets × 2 sides)
        assert len(db.writes) == 4
        
        # Check market IDs
        market_ids = {w["params"]["market_id"] for w in db.writes}
        assert market_ids == {100, 101}


class TestIngestOutcomeHandler:
    """Tests for OUTCOME_PUSH handling."""
    
    @pytest.fixture
    def db(self):
        db = MockDatabase()
        now = datetime.now(timezone.utc)
        # Add a valid event (started 1 hour ago, within 12h window)
        db.valid_events[456] = {
            "event_id": 456,
            "status": "in_play",
            "start_time_utc": now - timedelta(hours=1),
        }
        return db
    
    @pytest.fixture
    def handler(self, db):
        return IngestOutcomeHandler(db)
    
    async def test_handles_valid_outcome_push(self, handler, db):
        """Valid OUTCOME_PUSH should persist to inbox and emit event."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.OUTCOME_PUSH,
            {
                "event_id": 456,
                "result": "HOME",
                "score_home": 102,
                "score_away": 98,
            },
            "miner_hotkey_xyz",
        )
        
        event = await handler.handle_synapse(synapse)
        
        # Should emit MinerOutcomePushed event
        assert event is not None
        assert event.event_data["miner_hotkey"] == "miner_hotkey_xyz"
        assert event.event_data["payload"]["accepted"] is True
        
        # Should have written to inbox
        assert len(db.writes) == 1
        params = db.writes[0]["params"]
        assert params["topic"] == "outcome.submit"
        assert "dedupe_key" in params
    
    async def test_rejects_nonexistent_event(self, handler, db):
        """Outcome for non-existent event should be rejected."""
        synapse = make_synapse_with_hotkey(
            SparketSynapseType.OUTCOME_PUSH,
            {"event_id": 999, "result": "HOME"},
            "miner_hotkey_xyz",
        )
        
        event = await handler.handle_synapse(synapse)
        
        assert event is not None
        assert event.event_data["payload"]["accepted"] is False
        assert event.event_data["payload"]["reason"] == "event_not_found_or_ineligible"
        assert len(db.writes) == 0
    
    async def test_ignores_non_outcome_synapse(self, handler, db):
        """Non-OUTCOME_PUSH synapses should return None."""
        synapse = SparketSynapse(
            type=SparketSynapseType.ODDS_PUSH,
            payload={"miner_id": 1, "submissions": []},
        )
        
        event = await handler.handle_synapse(synapse)
        
        assert event is None
        assert len(db.writes) == 0


class TestGameDataHandler:
    """Tests for GAME_DATA_REQUEST handling."""
    
    @pytest.fixture
    def db(self):
        db = MockDatabase()
        now = datetime.now(timezone.utc)
        # Add test games with the new schema (minimal info for miners)
        db.games = [
            {
                "event_id": 1,
                "venue": "Stadium A",
                "start_time_utc": now + timedelta(days=1),
                "league_code": "nba",
                "sport_code": "basketball",
                "home_team": "Lakers",
                "away_team": "Celtics",
            },
            {
                "event_id": 2,
                "venue": "Stadium B",
                "start_time_utc": now + timedelta(days=2),
                "league_code": "nba",
                "sport_code": "basketball",
                "home_team": "Warriors",
                "away_team": "Heat",
            },
        ]
        db.markets = [
            {"market_id": 1001, "event_id": 1, "kind": "MONEYLINE", "line": None},
            {"market_id": 1002, "event_id": 1, "kind": "SPREAD", "line": -3.5},
            {"market_id": 1003, "event_id": 2, "kind": "MONEYLINE", "line": None},
        ]
        return db
    
    @pytest.fixture
    def handler(self, db):
        return GameDataHandler(db)
    
    async def test_returns_games_and_markets(self, handler, db):
        """GAME_DATA_REQUEST should return games with embedded markets."""
        synapse = SparketSynapse(
            type=SparketSynapseType.GAME_DATA_REQUEST,
            payload={},
        )
        
        response = await handler.handle_synapse(synapse)
        
        assert "error" not in response
        assert len(response["games"]) == 2
        assert "retrieved_at" in response
        assert "window" in response
    
    async def test_serializes_games_correctly(self, handler, db):
        """Games should be serialized with minimal info for miners."""
        synapse = SparketSynapse(
            type=SparketSynapseType.GAME_DATA_REQUEST,
            payload={},
        )
        
        response = await handler.handle_synapse(synapse)
        
        game = response["games"][0]
        assert game["event_id"] == 1
        assert game["home_team"] == "Lakers"
        assert game["away_team"] == "Celtics"
        assert game["venue"] == "Stadium A"
        assert game["league"] == "nba"
        assert game["sport"] == "basketball"
        assert "start_time_utc" in game
        assert "accepts_odds" in game
        assert "markets" in game
    
    async def test_embeds_markets_in_games(self, handler, db):
        """Markets should be embedded in their respective games."""
        synapse = SparketSynapse(
            type=SparketSynapseType.GAME_DATA_REQUEST,
            payload={},
        )
        
        response = await handler.handle_synapse(synapse)
        
        # First game should have 2 markets
        game1 = next(g for g in response["games"] if g["event_id"] == 1)
        assert len(game1["markets"]) == 2
        
        # Second game should have 1 market
        game2 = next(g for g in response["games"] if g["event_id"] == 2)
        assert len(game2["markets"]) == 1
    
    async def test_rejects_invalid_synapse_type(self, handler, db):
        """Non-GAME_DATA_REQUEST synapses should return error."""
        synapse = SparketSynapse(
            type=SparketSynapseType.ODDS_PUSH,
            payload={},
        )
        
        response = await handler.handle_synapse(synapse)
        
        assert response == {"error": "invalid_synapse_type"}
    
    async def test_handles_empty_database(self, handler):
        """Should handle empty database gracefully."""
        empty_db = MockDatabase()
        handler = GameDataHandler(empty_db)
        
        synapse = SparketSynapse(
            type=SparketSynapseType.GAME_DATA_REQUEST,
            payload={},
        )
        
        response = await handler.handle_synapse(synapse)
        
        assert response["games"] == []
        assert "retrieved_at" in response
        assert "window" in response
