"""Game data sync service for miners.

Handles fetching events/markets from validators and storing locally.
Supports delta sync to minimize data transfer for established miners.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import bittensor as bt
from sqlalchemy import text

from sparket.miner.client import ValidatorClient


_UPSERT_EVENT = text(
    """
    INSERT INTO event (event_id, external_id, home_team, away_team, venue, start_time_utc, status, league, sport, synced_at)
    VALUES (:event_id, :external_id, :home_team, :away_team, :venue, :start_time_utc, :status, :league, :sport, :synced_at)
    ON CONFLICT (event_id) DO UPDATE SET
        external_id = EXCLUDED.external_id,
        home_team = EXCLUDED.home_team,
        away_team = EXCLUDED.away_team,
        venue = EXCLUDED.venue,
        start_time_utc = EXCLUDED.start_time_utc,
        status = EXCLUDED.status,
        league = EXCLUDED.league,
        sport = EXCLUDED.sport,
        synced_at = EXCLUDED.synced_at
    """
)

_UPSERT_MARKET = text(
    """
    INSERT INTO market (market_id, event_id, kind, line, points_team_id, synced_at)
    VALUES (:market_id, :event_id, :kind, :line, :points_team_id, :synced_at)
    ON CONFLICT (market_id) DO UPDATE SET
        event_id = EXCLUDED.event_id,
        kind = EXCLUDED.kind,
        line = EXCLUDED.line,
        points_team_id = EXCLUDED.points_team_id,
        synced_at = EXCLUDED.synced_at
    """
)

_SELECT_LAST_SYNC = text(
    """
    SELECT MAX(synced_at) as last_sync FROM event
    """
)

_SELECT_ACTIVE_MARKETS = text(
    """
    SELECT m.market_id, m.event_id, m.kind, m.line, e.start_time_utc
    FROM market m
    JOIN event e ON e.event_id = m.event_id
    WHERE e.status = 'scheduled'
      AND e.start_time_utc > :now
    ORDER BY e.start_time_utc ASC
    """
)


class GameDataSync:
    """Manages syncing game data from validators.
    
    Features:
    - Delta sync: Only fetches new events after initial full sync
    - Periodic background sync
    - Exposes active markets for pricing
    """
    
    def __init__(
        self,
        *,
        database: Any,
        client: ValidatorClient,
        sync_interval_seconds: int = 300,
    ) -> None:
        self.database = database
        self.client = client
        self.sync_interval = sync_interval_seconds
        self._last_sync_ts: Optional[datetime] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def start(self) -> None:
        """Start background sync loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._sync_loop())
        bt.logging.info({"game_data_sync": "started", "interval": self.sync_interval})
    
    async def stop(self) -> None:
        """Stop background sync loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        bt.logging.info({"game_data_sync": "stopped"})
    
    async def _sync_loop(self) -> None:
        """Background loop that periodically syncs game data."""
        while self._running:
            try:
                await self.sync_once()
            except Exception as e:
                bt.logging.warning({"game_data_sync_error": str(e)})
            await asyncio.sleep(self.sync_interval)
    
    async def sync_once(self) -> bool:
        """Perform a single sync operation.
        
        Returns True if sync succeeded, False otherwise.
        """
        # Determine if this is a delta or full sync
        since_ts = await self._get_last_sync_ts()
        
        bt.logging.info({
            "game_data_sync": {
                "mode": "delta" if since_ts else "full",
                "since_ts": since_ts.isoformat() if since_ts else None,
            }
        })
        
        # Fetch from validator
        data = await self.client.fetch_game_data(since_ts=since_ts)
        if data is None:
            return False
        
        # Handler returns "games" with embedded markets
        games = data.get("games", [])
        sync_ts = self._parse_sync_ts(data.get("retrieved_at"))
        
        if not games:
            bt.logging.info({"game_data_sync": "no_new_data"})
            return True
        
        # Extract flat lists for persistence
        events = []
        markets = []
        for g in games:
            events.append({
                "event_id": g.get("event_id"),
                "external_id": g.get("external_id"),  # SportsData.io GameID
                "home_team": g.get("home_team"),
                "away_team": g.get("away_team"),
                "venue": g.get("venue"),
                "start_time_utc": g.get("start_time_utc"),
                "league": g.get("league"),
                "sport": g.get("sport"),
                "accepts_odds": g.get("accepts_odds"),
            })
            for m in g.get("markets", []):
                markets.append({
                    "market_id": m.get("market_id"),
                    "event_id": g.get("event_id"),
                    "kind": m.get("kind"),
                    "line": m.get("line"),
                })
        
        # Persist to local DB
        await self._persist_events(events, sync_ts)
        await self._persist_markets(markets, sync_ts)
        
        # Update last sync timestamp
        self._last_sync_ts = sync_ts
        
        bt.logging.info({
            "game_data_sync": {
                "status": "success",
                "games_synced": len(games),
                "events_synced": len(events),
                "markets_synced": len(markets),
                "sync_ts": sync_ts.isoformat() if sync_ts else None,
            }
        })
        
        return True
    
    async def get_active_markets(self) -> List[Dict[str, Any]]:
        """Get all active markets available for pricing."""
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            rows = await self.database.read(
                _SELECT_ACTIVE_MARKETS,
                params={"now": now},
                mappings=True,
            )
            return [
                {
                    "market_id": int(r["market_id"]),
                    "event_id": int(r["event_id"]),
                    "kind": str(r["kind"]),
                    "line": float(r["line"]) if r.get("line") else None,
                    "start_time_utc": r["start_time_utc"],
                }
                for r in rows
            ]
        except Exception as e:
            bt.logging.warning({"get_active_markets_error": str(e)})
            return []
    
    async def _get_last_sync_ts(self) -> Optional[datetime]:
        """Get the last sync timestamp from local storage."""
        if self._last_sync_ts:
            return self._last_sync_ts
        try:
            rows = await self.database.read(_SELECT_LAST_SYNC)
            if rows and rows[0].get("last_sync"):
                ts = rows[0]["last_sync"]
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                self._last_sync_ts = ts
                return ts
        except Exception:
            pass
        return None
    
    async def _persist_events(self, events: List[Dict], sync_ts: datetime) -> None:
        """Persist events to local database."""
        sync_ts_naive = sync_ts.replace(tzinfo=None) if sync_ts else datetime.now(timezone.utc).replace(tzinfo=None)
        for event in events:
            try:
                start_time = event.get("start_time_utc")
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace("Z", "+00:00")).replace(tzinfo=None)
                
                await self.database.write(
                    _UPSERT_EVENT,
                    params={
                        "event_id": int(event["event_id"]),
                        "external_id": int(event["external_id"]) if event.get("external_id") else None,
                        "home_team": event.get("home_team"),
                        "away_team": event.get("away_team"),
                        "venue": event.get("venue"),
                        "start_time_utc": start_time,
                        "status": event.get("status", "scheduled"),
                        "league": event.get("league"),
                        "sport": event.get("sport"),
                        "synced_at": sync_ts_naive,
                    },
                )
            except Exception as e:
                bt.logging.warning({"persist_event_error": str(e), "event_id": event.get("event_id")})
    
    async def _persist_markets(self, markets: List[Dict], sync_ts: datetime) -> None:
        """Persist markets to local database."""
        sync_ts_naive = sync_ts.replace(tzinfo=None) if sync_ts else datetime.now(timezone.utc).replace(tzinfo=None)
        for market in markets:
            try:
                await self.database.write(
                    _UPSERT_MARKET,
                    params={
                        "market_id": int(market["market_id"]),
                        "event_id": int(market["event_id"]),
                        "kind": str(market["kind"]),
                        "line": float(market["line"]) if market.get("line") is not None else None,
                        "points_team_id": int(market["points_team_id"]) if market.get("points_team_id") else None,
                        "synced_at": sync_ts_naive,
                    },
                )
            except Exception as e:
                bt.logging.warning({"persist_market_error": str(e), "market_id": market.get("market_id")})
    
    def _parse_sync_ts(self, value: Any) -> datetime:
        """Parse sync timestamp from response."""
        if value is None:
            return datetime.now(timezone.utc)
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
        return datetime.now(timezone.utc)


__all__ = ["GameDataSync"]


