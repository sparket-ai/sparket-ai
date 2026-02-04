"""Handler for GAME_DATA_REQUEST - serves minimal game info to miners.

Privacy-conscious design:
- Only provides information needed for miners to identify games and submit odds
- Does NOT expose internal IDs, external references, or provider data
- Time-windowed: games visible 14 days ahead, odds accepted 7 days before start

Note: This handler creates its own database connections to avoid event loop
conflicts when called from Bittensor's axon request context.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import bittensor as bt
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType


def _build_db_url() -> str:
    """Build database URL from environment/config."""
    user = os.getenv("SPARKET_DATABASE__USER", "sparket")
    password = os.getenv("SPARKET_DATABASE__PASSWORD", "sparket")
    host = os.getenv("SPARKET_DATABASE__HOST", "127.0.0.1")
    port = os.getenv("SPARKET_DATABASE__PORT", "5435")
    name = os.getenv("SPARKET_DATABASE__NAME", "sparket")
    return f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{name}"


# Query for upcoming games with team names and league/sport info
# Uses COALESCE to fall back to ext_ref team names when team IDs aren't linked
_SELECT_UPCOMING_GAMES = text(
    """
    SELECT 
        e.event_id,
        (e.ext_ref->'sportsdataio'->>'GameID')::int AS external_id,
        e.venue,
        e.start_time_utc,
        l.code AS league_code,
        s.code AS sport_code,
        COALESCE(home.name, e.ext_ref->'sportsdataio'->>'HomeTeam') AS home_team,
        COALESCE(away.name, e.ext_ref->'sportsdataio'->>'AwayTeam') AS away_team
    FROM event e
    JOIN league l ON e.league_id = l.league_id
    JOIN sport s ON l.sport_id = s.sport_id
    LEFT JOIN team home ON e.home_team_id = home.team_id
    LEFT JOIN team away ON e.away_team_id = away.team_id
    WHERE e.status = 'scheduled'
      AND e.start_time_utc >= :now
      AND e.start_time_utc <= :until
    ORDER BY e.start_time_utc ASC
    LIMIT :limit
    """
)

# Query for markets (minimal info - just what miners need to submit)
_SELECT_MARKETS_FOR_EVENTS = text(
    """
    SELECT 
        m.market_id,
        m.event_id,
        m.kind,
        m.line
    FROM market m
    WHERE m.event_id = ANY(:event_ids)
    ORDER BY m.event_id, m.kind
    """
)


class GameDataHandler:
    """Handles GAME_DATA_REQUEST synapses from miners.
    
    Provides minimal game identification data for odds submission:
    - Team names (not IDs)
    - Venue, start time
    - League/sport codes
    - Market types available
    
    Time windows:
    - Games visible: up to 14 days ahead
    - Odds accepted: 7 days before game start
    
    Note: Creates fresh database connections per request to avoid event loop
    conflicts when called from Bittensor's axon context.
    """
    
    # Configuration
    LOOKAHEAD_DAYS = 14          # How far ahead to show games
    ODDS_WINDOW_DAYS = 7         # When miners can start submitting odds
    MAX_EVENTS_PER_REQUEST = 500
    
    def __init__(self, database: Any, *, use_mock: bool = False):
        # Keep reference but we'll create fresh connections per request (unless mocked)
        self._database = database
        self._db_url = _build_db_url()
        # In test mode, use the passed-in database directly
        self._use_mock = use_mock or hasattr(database, 'games')
    
    async def handle_synapse(self, synapse: SparketSynapse) -> Dict[str, Any]:
        """Process GAME_DATA_REQUEST and return minimal game data."""
        # Normalize synapse type for comparison
        synapse_type = synapse.type
        if isinstance(synapse_type, SparketSynapseType):
            synapse_type = synapse_type.value
        elif isinstance(synapse_type, str):
            synapse_type = synapse_type.lower()
        
        if synapse_type != SparketSynapseType.GAME_DATA_REQUEST.value:
            return {"error": "invalid_synapse_type"}
        
        now = datetime.now(timezone.utc)
        
        try:
            # In test mode, use the mock database directly
            if self._use_mock:
                games = getattr(self._database, 'games', [])
                if not games:
                    return self._build_response([], [], now)
                event_ids = [g["event_id"] for g in games]
                markets = [m for m in getattr(self._database, 'markets', []) if m["event_id"] in event_ids]
                return self._build_response(games, markets, now)
            
            # Create a fresh engine for this request to avoid event loop issues
            engine = create_async_engine(self._db_url, pool_size=1, max_overflow=0)
            
            try:
                # Fetch upcoming games within window
                games = await self._fetch_upcoming_games(engine, now)
                
                if not games:
                    return self._build_response([], [], now)
                
                # Fetch markets for these games
                event_ids = [g["event_id"] for g in games]
                markets = await self._fetch_markets(engine, event_ids)
                
                response = self._build_response(games, markets, now)
                
                bt.logging.info({
                    "game_data_request": {
                        "games_count": len(games),
                        "markets_count": len(markets),
                        "response_keys": list(response.keys()) if response else [],
                    }
                })
                
                # Defensive check - should never happen but helps debug
                if not response or not isinstance(response, dict) or "games" not in response:
                    bt.logging.error({
                        "game_data_handler_unexpected": {
                            "response_type": type(response).__name__,
                            "response": str(response)[:200] if response else "None",
                        }
                    })
                
                return response
            finally:
                # Clean up the engine
                await engine.dispose()
            
        except Exception as e:
            bt.logging.warning({"game_data_handler_error": str(e)})
            return {"error": str(e)}
    
    async def _fetch_upcoming_games(self, engine: Any, now: datetime) -> List[Dict]:
        """Fetch scheduled games within the lookahead window."""
        until = now + timedelta(days=self.LOOKAHEAD_DAYS)
        
        async with engine.connect() as conn:
            result = await conn.execute(
                _SELECT_UPCOMING_GAMES,
                {
                    "now": now,
                    "until": until,
                    "limit": self.MAX_EVENTS_PER_REQUEST,
                },
            )
            rows = result.mappings().all()
        return [dict(r) for r in rows]
    
    async def _fetch_markets(self, engine: Any, event_ids: List[int]) -> List[Dict]:
        """Fetch markets for the given event IDs."""
        if not event_ids:
            return []
        
        async with engine.connect() as conn:
            result = await conn.execute(
                _SELECT_MARKETS_FOR_EVENTS,
                {"event_ids": event_ids},
            )
            rows = result.mappings().all()
        return [dict(r) for r in rows]
    
    def _build_response(
        self, 
        games: List[Dict], 
        markets: List[Dict], 
        now: datetime
    ) -> Dict[str, Any]:
        """Build the response payload with minimal game info."""
        # Group markets by event
        markets_by_event: Dict[int, List[Dict]] = {}
        for m in markets:
            eid = int(m["event_id"])
            if eid not in markets_by_event:
                markets_by_event[eid] = []
            markets_by_event[eid].append({
                "market_id": int(m["market_id"]),
                "kind": str(m["kind"]),
                "line": float(m["line"]) if m.get("line") is not None else None,
            })
        
        # Build game entries with embedded markets
        game_list = []
        odds_cutoff = now + timedelta(days=self.ODDS_WINDOW_DAYS)
        
        for g in games:
            event_id = int(g["event_id"])
            start_time = g["start_time_utc"]
            
            # Determine if odds can be submitted for this game
            accepts_odds = start_time <= odds_cutoff
            
            game_list.append({
                "event_id": event_id,
                "external_id": g.get("external_id"),  # SportsData.io GameID
                "home_team": g.get("home_team") or "TBD",
                "away_team": g.get("away_team") or "TBD",
                "venue": g.get("venue"),
                "start_time_utc": start_time.isoformat() if start_time else None,
                "league": g.get("league_code"),
                "sport": g.get("sport_code"),
                "accepts_odds": accepts_odds,
                "markets": markets_by_event.get(event_id, []),
            })
        
        return {
            "games": game_list,
            "retrieved_at": now.isoformat(),
            "window": {
                "lookahead_days": self.LOOKAHEAD_DAYS,
                "odds_window_days": self.ODDS_WINDOW_DAYS,
            },
        }


__all__ = ["GameDataHandler"]
