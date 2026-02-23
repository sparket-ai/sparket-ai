from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text

from sparket.shared.rows import EventRow, MarketRow

MarketKey = Tuple[str, Optional[Decimal], Optional[int]]


def _ensure_utc(dt: datetime | None) -> datetime | None:
    """Ensure datetime is timezone-aware (UTC)."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # Assume naive datetimes are UTC
        return dt.replace(tzinfo=timezone.utc)
    return dt


_SELECT_EVENT_BY_SDI0 = text(
    """
    SELECT event_id, start_time_utc
    FROM event
    WHERE (ext_ref->'sportsdataio'->>'GameID') = :game_id
    LIMIT 1
    """
)

_INSERT_EVENT = text(
    """
    INSERT INTO event (
        league_id, home_team_id, away_team_id, venue, start_time_utc, status, ext_ref, created_at
    ) VALUES (
        :league_id, :home_team_id, :away_team_id, :venue, :start_time_utc, :status, :ext_ref, :created_at
    ) RETURNING event_id
    """
)

_SELECT_MARKET = text(
    """
    SELECT market_id
    FROM market
    WHERE event_id = :event_id
      AND kind = :kind
      AND COALESCE(line, CAST(0 AS numeric)) = COALESCE(CAST(:line AS numeric), CAST(0 AS numeric))
      AND COALESCE(points_team_id, CAST(0 AS bigint)) = COALESCE(CAST(:points_team_id AS bigint), CAST(0 AS bigint))
    LIMIT 1
    """
)

_INSERT_MARKET = text(
    """
    INSERT INTO market (
        event_id, kind, line, points_team_id, created_at
    ) VALUES (
        :event_id, :kind, :line, :points_team_id, :created_at
    )
    """
)


async def ensure_event_for_sdio(database: Any, event_row: EventRow) -> tuple[int, datetime]:
    ext = (event_row.get("ext_ref") or {}).get("sportsdataio") or {}
    game_id = str(ext.get("GameID", ""))
    if not game_id:
        raise ValueError("missing SDIO GameID in ext_ref")

    found = await database.read(_SELECT_EVENT_BY_SDI0, params={"game_id": game_id}, mappings=True)
    if found:
        row = found[0]
        return int(row["event_id"]), row["start_time_utc"]

    params = {
        "league_id": event_row.get("league_id"),
        "home_team_id": event_row.get("home_team_id"),
        "away_team_id": event_row.get("away_team_id"),
        "venue": event_row.get("venue"),
        "start_time_utc": _ensure_utc(event_row.get("start_time_utc")),
        "status": event_row.get("status"),
        "ext_ref": json.dumps(event_row.get("ext_ref") or {}),
        "created_at": _ensure_utc(event_row.get("created_at")) or datetime.now(timezone.utc),
    }
    rows = await database.write(_INSERT_EVENT, params=params, return_rows=True, mappings=True)
    ev_id = int(rows[0]["event_id"]) if rows else None
    if ev_id is None:
        found = await database.read(_SELECT_EVENT_BY_SDI0, params={"game_id": game_id}, mappings=True)
        if not found:
            raise RuntimeError("failed to upsert event")
        row = found[0]
        return int(row["event_id"]), row["start_time_utc"]
    return ev_id, params["start_time_utc"]


async def ensure_market(database: Any, market_row: MarketRow, *, event_id: int) -> int:
    kind = market_row.get("kind")
    if hasattr(kind, "name"):
        kind = kind.name
    elif isinstance(kind, str):
        kind = kind.upper()
    raw_line = market_row.get("line")
    line = Decimal(str(raw_line)) if raw_line is not None else None
    params = {
        "event_id": event_id,
        "kind": kind,
        "line": line,
        "points_team_id": market_row.get("points_team_id"),
        "created_at": _ensure_utc(market_row.get("created_at")) or datetime.now(timezone.utc),
    }
    found = await database.read(_SELECT_MARKET, params=params, mappings=True)
    if found:
        return int(found[0]["market_id"])
    try:
        await database.write(_INSERT_MARKET, params=params)
    except Exception:
        pass
    found = await database.read(_SELECT_MARKET, params=params, mappings=True)
    if not found:
        raise RuntimeError("failed to upsert market")
    return int(found[0]["market_id"])


def _market_key(row: MarketRow) -> MarketKey:
    """Normalise a MarketRow into a hashable lookup key."""
    kind = row.get("kind")
    if hasattr(kind, "name"):
        kind = kind.name
    elif isinstance(kind, str):
        kind = kind.upper()
    raw_line = row.get("line")
    line = Decimal(str(raw_line)) if raw_line is not None else None
    pts = row.get("points_team_id")
    return (str(kind), line, int(pts) if pts is not None else None)


_SELECT_ALL_MARKETS_FOR_EVENT = text(
    """
    SELECT market_id, kind, 
           COALESCE(line, CAST(0 AS numeric)) AS line_cmp,
           line,
           COALESCE(points_team_id, CAST(0 AS bigint)) AS pts_cmp,
           points_team_id
    FROM market
    WHERE event_id = :event_id
    """
)


async def ensure_markets_batch(
    database: Any,
    market_rows: List[MarketRow],
    *,
    event_id: int,
) -> Dict[MarketKey, int]:
    """Resolve market IDs for multiple rows with minimal DB round-trips.

    1. One SELECT to fetch all existing markets for the event.
    2. Determine which rows are new.
    3. Individual INSERTs for new markets (with conflict handling).
    4. Returns mapping of (kind, line, points_team_id) -> market_id.
    """
    if not market_rows:
        return {}

    existing = await database.read(
        _SELECT_ALL_MARKETS_FOR_EVENT,
        params={"event_id": event_id},
        mappings=True,
    )

    known: Dict[MarketKey, int] = {}
    for row in existing:
        k = row["kind"]
        if hasattr(k, "name"):
            k = k.name
        elif isinstance(k, str):
            k = k.upper()
        raw_line = row["line"]
        line = Decimal(str(raw_line)) if raw_line is not None else None
        pts = row["points_team_id"]
        key = (str(k), line, int(pts) if pts is not None else None)
        known[key] = int(row["market_id"])

    result: Dict[MarketKey, int] = {}
    for mrow in market_rows:
        key = _market_key(mrow)
        if key in known:
            result[key] = known[key]
            continue
        if key in result:
            continue
        mid = await ensure_market(database, mrow, event_id=event_id)
        result[key] = mid
        known[key] = mid

    return result

