from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import bittensor as bt
from sqlalchemy import text

from sparket.providers.providers import get_provider_id
from sparket.providers.sportsdataio import (
    LeagueCode,
    LeagueConfig,
    SportsDataIOClient,
    SportsDataIOConfig,
    build_default_config,
    map_game_to_event,
    ensure_markets_for_event,
    map_moneyline_quotes,
    map_spread_quotes,
    map_total_quotes,
    resolve_spread_result,
    resolve_total_result,
)
from sparket.providers.sportsdataio.catalog import team_rows_from_catalog
from sparket.providers.sportsdataio.enums import GameStatus
from sparket.providers.sportsdataio.types import Game, GameOdds, GameOddsSet, Team
from sparket.validator.database.resolver import (
    ensure_event_for_sdio,
    ensure_market,
    ensure_markets_batch,
    _market_key,
    MarketKey,
)
from sparket.validator.handlers.ingest.provider_ingest import insert_provider_quotes, upsert_provider_closing_for_event
from sparket.shared.rows import ProviderQuoteRow


# SQL for event status sync and outcome recording
_UPDATE_EVENT_STATUS = text("""
    UPDATE event SET status = :status
    WHERE event_id = :event_id AND status != :status
""")

_PATCH_EVENT_TEAMS = text("""
    UPDATE event
    SET
        home_team_id = COALESCE(home_team_id, :home_team_id),
        away_team_id = COALESCE(away_team_id, :away_team_id)
    WHERE event_id = :event_id
      AND (home_team_id IS NULL OR away_team_id IS NULL)
""")

_SELECT_MARKETS_FOR_EVENT = text("""
    SELECT market_id, kind, line, points_team_id
    FROM market
    WHERE event_id = :event_id
""")

_UPSERT_OUTCOME = text("""
    INSERT INTO outcome (market_id, settled_at, result, score_home, score_away, details)
    VALUES (:market_id, :settled_at, :result, :score_home, :score_away, :details)
    ON CONFLICT (market_id) DO UPDATE SET
        settled_at = EXCLUDED.settled_at,
        result = EXCLUDED.result,
        score_home = EXCLUDED.score_home,
        score_away = EXCLUDED.score_away,
        details = EXCLUDED.details
""")

_CHECK_OUTCOME_EXISTS = text("""
    SELECT 1 FROM outcome WHERE market_id = :market_id LIMIT 1
""")

_UPSERT_SPORTSBOOK = text("""
    INSERT INTO sportsbook (provider_id, code, name, is_sharp, active, created_at)
    VALUES (:provider_id, :code, :name, false, true, NOW())
    ON CONFLICT (code) DO NOTHING
    RETURNING sportsbook_id
""")

_SELECT_SPORTSBOOK_CODES = text("""
    SELECT code FROM sportsbook WHERE provider_id = :provider_id
""")

_SEED_SPORTSBOOK_BIAS = text("""
    INSERT INTO sportsbook_bias (sportsbook_id, sport_id, market_kind, bias_factor, variance, sample_count, version, computed_at)
    SELECT :sportsbook_id, s.sport_id, mk.kind::market_kind, 1.0, 0.01, 0, 1, NOW()
    FROM sport s
    CROSS JOIN (VALUES ('MONEYLINE'), ('SPREAD'), ('TOTAL')) AS mk(kind)
    ON CONFLICT DO NOTHING
""")


@dataclass
class TrackedEvent:
    league_code: LeagueCode
    game_id: int
    event_id: int
    start_time: datetime
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    next_snapshot_at: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    last_history_ts: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    seen_odd_ids: Set[int] = field(default_factory=set)
    post_start_polls_remaining: int = 2
    closing_captured: bool = False
    closing_ts: Optional[datetime] = None

    def minutes_to_start(self, now: datetime) -> float:
        delta = self.start_time - now
        return delta.total_seconds() / 60.0


@dataclass
class LeagueState:
    config: LeagueConfig
    league_id: Optional[int] = None
    next_schedule_refresh: datetime = field(default_factory=lambda: datetime.min.replace(tzinfo=timezone.utc))
    team_index: Dict[str, int] = field(default_factory=dict)


@dataclass
class OddsCacheEntry:
    expires_at: datetime
    odds: Optional[GameOddsSet]


TrackedKey = Tuple[LeagueCode, int]
FINAL_GAME_STATUSES = {
    GameStatus.FINAL,
    GameStatus.FINAL_OT,
    GameStatus.FINAL_SO,
    GameStatus.FINAL_AET,
    GameStatus.FINAL_PEN,
    GameStatus.AWARDED,
    GameStatus.FORFEIT,
}


class SportsDataIngestor:
    """
    Coordinates SportsDataIO ingestion for the validator.

    Responsibilities:
      - Keep reference catalogs (teams) aligned with provider data
      - Track upcoming events and ensure markets exist
      - Capture odds snapshots / delta feeds with adaptive cadence
    """

    POST_START_CUTOFF_DAYS = 5
    MAX_CONCURRENT_SNAPSHOTS = 8

    def __init__(
        self,
        *,
        database: Any,
        client: Optional[SportsDataIOClient] = None,
        config: Optional[SportsDataIOConfig] = None,
    ) -> None:
        self.database = database
        self.config = config or build_default_config()
        self.client = client or SportsDataIOClient()
        self.provider_id = get_provider_id(self.config.provider_code)
        if not self.provider_id:
            raise RuntimeError("Provider ID for SDIO unavailable; seed reference.provider first.")
        self.leagues: Dict[LeagueCode, LeagueState] = {
            league.code: LeagueState(config=league) for league in self.config.leagues
        }
        self.tracked_events: Dict[TrackedKey, TrackedEvent] = {}
        self._odds_cache: Dict[Tuple[LeagueCode, int], OddsCacheEntry] = {}
        self._outcome_score_missing_logged: Set[int] = set()
        self._known_sportsbook_codes: Optional[Set[str]] = None
        self._market_id_cache: Dict[Tuple[int, MarketKey], int] = {}

    async def close(self) -> None:
        self._odds_cache.clear()
        await self.client.close()

    async def initialize_from_db(self) -> int:
        """
        Initialize tracked events from database on startup.
        This avoids re-fetching full schedules from the provider API.
        Returns count of events restored.
        """
        now = datetime.now(timezone.utc)
        restored = 0
        
        # Query upcoming events from database
        rows = await self.database.read(
            text("""
                SELECT 
                    e.event_id,
                    e.league_id,
                    e.home_team_id,
                    e.away_team_id,
                    e.start_time_utc,
                    e.ext_ref,
                    l.code as league_code,
                    (SELECT MAX(pq.ts) FROM provider_quote pq 
                     JOIN market m ON pq.market_id = m.market_id 
                     WHERE m.event_id = e.event_id) as latest_quote_ts
                FROM event e
                JOIN league l ON e.league_id = l.league_id
                WHERE e.status IN ('scheduled', 'in_play')
                  AND e.start_time_utc > :min_time
                  AND e.start_time_utc < :max_time
            """),
            params={
                "min_time": now - timedelta(hours=6),  # Include recently started games
                "max_time": now + timedelta(days=8),
            },
            mappings=True,
        )
        
        for row in rows:
            ext_ref = row["ext_ref"] or {}
            sdio_data = ext_ref.get("sportsdataio", {})
            game_id = sdio_data.get("GameID")
            if not game_id:
                continue
                
            # Map league code
            league_code_str = row["league_code"]
            try:
                league_code = LeagueCode(league_code_str.lower())
            except ValueError:
                continue
                
            if league_code not in self.leagues:
                continue
            
            key = self._tracked_key(league_code, game_id)
            if key in self.tracked_events:
                continue  # Already tracked
            
            # Create tracked event with watermark from DB
            latest_quote_ts = row["latest_quote_ts"]
            tracked = TrackedEvent(
                league_code=league_code,
                game_id=game_id,
                event_id=row["event_id"],
                start_time=row["start_time_utc"],
                home_team_id=row["home_team_id"],
                away_team_id=row["away_team_id"],
                last_history_ts=latest_quote_ts or datetime.min.replace(tzinfo=timezone.utc),
            )
            self.tracked_events[key] = tracked
            restored += 1
        
        bt.logging.info({
            "sdio_ingestor_init": {
                "events_restored_from_db": restored,
                "total_tracked": len(self.tracked_events),
            }
        })
        return restored

    @staticmethod
    def _ts(value: Optional[datetime]) -> Optional[str]:
        if value is None:
            return None
        return value.astimezone(timezone.utc).isoformat()

    def _tracked_key(self, league_code: LeagueCode, game_id: int) -> TrackedKey:
        return (league_code, int(game_id))

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------
    async def run_once(self, *, now: Optional[datetime] = None) -> None:
        from sparket.validator.observability.metrics import (
            PROVIDER_REQUESTS, PROVIDER_ERRORS, PROVIDER_LATENCY,
            EVENTS_ACTIVE, MARKETS_ACTIVE,
        )
        import time as _obs_time

        now = now or datetime.now(timezone.utc)
        self._evict_expired_cache(now)
        cycle_start = now
        total_snapshots = 0
        total_errors = 0
        leagues_processed = 0
        for league_code, state in self.leagues.items():
            metrics: Dict[str, Any] = {
                "league": league_code.value,
                "schedule_games": 0,
                "events_tracked": 0,
                "snapshot_attempts": 0,
                "snapshot_success": 0,
                "snapshot_missed": 0,
            }
            _t0 = _obs_time.monotonic()
            try:
                PROVIDER_REQUESTS.labels(endpoint=f"sdio/{league_code.value}").inc()
                await self._ensure_league_id(state)
                if state.league_id is None:
                    metrics["error"] = "missing_league_id"
                    continue
                if state.next_schedule_refresh <= now:
                    metrics["schedule_games"] = await self._refresh_schedule(state, now)
                await self._refresh_odds(state, now, metrics=metrics)
                leagues_processed += 1
                total_snapshots += metrics["snapshot_success"]
            except Exception as exc:
                metrics["error"] = str(exc)
                total_errors += 1
                PROVIDER_ERRORS.labels(endpoint=f"sdio/{league_code.value}").inc()
                bt.logging.warning({"sdio_ingestor_error": str(exc), "league": league_code.value})
            finally:
                PROVIDER_LATENCY.labels(endpoint=f"sdio/{league_code.value}").observe(
                    _obs_time.monotonic() - _t0
                )
                bt.logging.info({"sdio_ingestor_league": metrics})
        elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
        bt.logging.info({
            "sdio_cycle_complete": {
                "leagues": leagues_processed,
                "snapshots": total_snapshots,
                "errors": total_errors,
                "elapsed_seconds": round(elapsed, 1),
                "total_tracked": len(self.tracked_events),
            }
        })

        # Update active event/market gauges
        EVENTS_ACTIVE.set(len(self.tracked_events))

    # ------------------------------------------------------------------
    # Schedule + catalog
    # ------------------------------------------------------------------
    async def _ensure_league_id(self, state: LeagueState) -> None:
        if state.league_id is not None:
            return
        query = text("select league_id from league where lower(code) = lower(:code) limit 1")
        rows = await self.database.read(query, params={"code": state.config.league_code}, mappings=True)
        if not rows:
            bt.logging.warning({"sdio_league_missing": state.config.league_code})
            return
        state.league_id = int(rows[0]["league_id"])
        await self._refresh_team_catalog(state)

    async def _refresh_team_catalog(self, state: LeagueState) -> None:
        teams = await self.client.fetch_team_catalog(state.config)
        if not teams:
            return
        rows = team_rows_from_catalog(teams, league_id=state.league_id)
        for row in rows:
            await self._ensure_team_row(row)
        state.team_index = await self._load_team_index(state.league_id)

    async def _ensure_team_row(self, row: dict) -> None:
        league_id = row["league_id"]
        ext = (row.get("ext_ref") or {}).get("sportsdataio") or {}
        team_key = ext.get("Key")
        if not team_key:
            return
        select = text(
            """
            select team_id from team
            where league_id = :league_id
              and (ext_ref->'sportsdataio'->>'Key') = :team_key
            limit 1
            """
        )
        found = await self.database.read(select, params={"league_id": league_id, "team_key": team_key}, mappings=True)
        if found:
            return
        insert = text(
            """
            insert into team (league_id, name, abbrev, ext_ref)
            values (:league_id, :name, :abbrev, :ext_ref)
            """
        )
        await self.database.write(
            insert,
            params={
                "league_id": league_id,
                "name": row["name"],
                "abbrev": row.get("abbrev"),
                "ext_ref": json.dumps(row.get("ext_ref") or {}),
            },
        )

    async def _load_team_index(self, league_id: int) -> Dict[str, int]:
        query = text(
            """
            select
                team_id,
                name,
                ext_ref->'sportsdataio'->>'Key' as team_key,
                ext_ref->'sportsdataio'->>'TeamID' as team_sdio_id
            from team
            where league_id = :league_id
            """
        )
        rows = await self.database.read(query, params={"league_id": league_id}, mappings=True)
        index: Dict[str, int] = {}
        for row in rows:
            team_id = int(row["team_id"])
            aliases = (
                row.get("team_key"),
                row.get("name"),
                row.get("team_sdio_id"),
            )
            for alias in aliases:
                if alias is None:
                    continue
                key = str(alias).strip()
                if not key:
                    continue
                index.setdefault(key, team_id)
                index.setdefault(key.casefold(), team_id)
        return index

    @staticmethod
    def _resolve_team_id(team_index: Dict[str, int], team_ref: Optional[str]) -> Optional[int]:
        if team_ref is None:
            return None
        token = str(team_ref).strip()
        if not token:
            return None
        return team_index.get(token) or team_index.get(token.casefold())

    async def _patch_event_teams(
        self,
        event_id: int,
        home_team_id: Optional[int],
        away_team_id: Optional[int],
    ) -> None:
        if home_team_id is None and away_team_id is None:
            return
        await self.database.write(
            _PATCH_EVENT_TEAMS,
            params={
                "event_id": event_id,
                "home_team_id": home_team_id,
                "away_team_id": away_team_id,
            },
        )

    async def _refresh_schedule(self, state: LeagueState, now: datetime) -> int:
        # Off-season skip: avoid wasting API calls when no games are upcoming
        if (
            state.config.off_season_months
            and now.month in state.config.off_season_months
            and not await self._has_upcoming_events(state, now)
        ):
            bt.logging.info({
                "sdio_schedule_skip": {
                    "league": state.config.code.value,
                    "reason": "off_season",
                    "month": now.month,
                }
            })
            state.next_schedule_refresh = now + timedelta(hours=6)
            return 0

        if state.config.schedule_mode == "season":
            games = await self._fetch_schedule_season(state, now)
        else:
            games = await self._fetch_schedule_window(state, now)
        team_index = state.team_index
        if not team_index:
            team_index = await self._load_team_index(state.league_id)
            state.team_index = team_index
        start_cutoff = now - timedelta(days=1)
        end_cutoff = now + timedelta(days=state.config.track_days_ahead)
        stale_cutoff = now - timedelta(days=self.POST_START_CUTOFF_DAYS)
        total_upserts = 0
        tracked = 0
        status_updates = 0
        outcomes_recorded = 0
        skipped = 0
        for game in games:
            if not game.date_time:
                continue

            # Fast filter: skip games outside our processing window entirely.
            # Avoids thousands of pointless DB round-trips for full-season fetches.
            game_start = game.date_time
            if game_start.tzinfo is None:
                game_start = game_start.replace(tzinfo=timezone.utc)
            if game_start < stale_cutoff or game_start > end_cutoff:
                skipped += 1
                continue

            event_id, start_time, home_id, away_id = await self._upsert_event(state, game, team_index)
            total_upserts += 1

            try:
                updated = await self._sync_event_status(event_id, game, now)
                if updated:
                    status_updates += 1
                if self._is_game_final(game):
                    resolved_home, resolved_away = await self._resolve_final_scores(state, game, now)
                    recorded = await self._record_outcomes(
                        event_id,
                        game,
                        home_id,
                        away_id,
                        home_score_override=resolved_home,
                        away_score_override=resolved_away,
                    )
                    outcomes_recorded += recorded
            except Exception as e:
                bt.logging.warning({"sdio_status_sync_error": str(e), "event_id": event_id})

            should_track = start_cutoff <= start_time <= end_cutoff
            key = self._tracked_key(state.config.code, game.game_id)
            if should_track and not self._is_game_final(game):
                self._ensure_tracked_event(state, game, event_id, start_time, home_id, away_id)
                tracked += 1
            else:
                self.tracked_events.pop(key, None)

        bt.logging.info({
            "sdio_schedule_refresh": {
                "league": state.config.code.value,
                "api_games": len(games),
                "processed": total_upserts,
                "tracked": tracked,
                "skipped": skipped,
                "status_updates": status_updates,
                "outcomes_recorded": outcomes_recorded,
            }
        })
        state.next_schedule_refresh = now + timedelta(minutes=state.config.schedule_refresh_minutes)
        return total_upserts

    async def _fetch_schedule_window(self, state: LeagueState, now: datetime) -> List[Game]:
        start = now.date()
        end = date.fromordinal(start.toordinal() + state.config.track_days_ahead)
        return await self.client.fetch_schedule_window(state.config, start, end)

    async def _has_upcoming_events(self, state: LeagueState, now: datetime) -> bool:
        """Check DB for any upcoming scheduled events in this league."""
        rows = await self.database.read(
            text("""
                SELECT 1 FROM event
                WHERE league_id = :league_id
                  AND status = 'scheduled'
                  AND start_time_utc > :now
                LIMIT 1
            """),
            params={"league_id": state.league_id, "now": now},
        )
        return bool(rows)

    async def _fetch_schedule_season(self, state: LeagueState, now: datetime) -> List[Game]:
        seasons = self._compute_active_seasons(now, state.config)
        all_games: List[Game] = []
        for season_code, season_type in seasons:
            try:
                games = await self.client.fetch_schedule_season(
                    state.config,
                    season_code,
                    season_type=season_type or None,
                )
                bt.logging.debug({
                    "sdio_season_fetch": {
                        "league": state.config.code.value,
                        "season_code": season_code,
                        "games_count": len(games),
                    }
                })
                all_games.extend(games)
            except Exception as e:
                bt.logging.warning({
                    "sdio_season_fetch_error": {
                        "league": state.config.code.value,
                        "season_code": season_code,
                        "error": str(e),
                    }
                })
        return all_games

    @staticmethod
    def _primary_season_year(now: datetime, config: LeagueConfig) -> int:
        """Compute the primary season year from offset rules."""
        offset = int(config.season_year_offset or 0)
        if offset == 0:
            return now.year
        elif offset == -1:
            return now.year - 1 if now.month <= 2 else now.year
        else:
            return now.year + 1 if now.month >= 10 else now.year

    @staticmethod
    def _adjacent_season_year(primary: int, config: LeagueConfig) -> int:
        """Compute the adjacent season year for transition periods."""
        offset = int(config.season_year_offset or 0)
        if offset == -1:
            return primary + 1
        else:
            return primary + 1 if offset >= 1 else primary + 1

    @staticmethod
    def _compute_active_seasons(
        now: datetime, config: LeagueConfig,
    ) -> List[Tuple[str, Optional[str]]]:
        """Return a list of (season_code, season_type) pairs to fetch.

        Handles:
        - Multiple season types (e.g., PRE/REG/POST for NFL)
        - Transition months: fetches both current and adjacent season
        - Normal months: fetches current season only
        """
        fmt = config.season_format or "{year}"
        season_types = config.season_types
        if not season_types:
            season_types = [config.season_type or ""]

        primary_year = SportsDataIngestor._primary_season_year(now, config)

        years = [primary_year]
        if config.transition_months and now.month in config.transition_months:
            adj = SportsDataIngestor._adjacent_season_year(primary_year, config)
            if adj != primary_year:
                years.append(adj)

        pairs: List[Tuple[str, Optional[str]]] = []
        for year in years:
            for st in season_types:
                st_upper = (st or "").upper()
                code = fmt.format(
                    year=year, season_type=st_upper,
                    SEASONTYPE=st_upper, YEAR=year,
                )
                pairs.append((code, st_upper or None))
        return pairs

    async def _upsert_event(
        self,
        state: LeagueState,
        game: Game,
        team_index: Dict[str, int],
    ) -> Tuple[int, datetime, Optional[int], Optional[int]]:
        if not game.date_time:
            raise ValueError("game missing date_time")
        home_id = self._resolve_team_id(team_index, game.home_team)
        away_id = self._resolve_team_id(team_index, game.away_team)
        event_row = map_game_to_event(game, league_id=state.league_id, home_team_id=home_id, away_team_id=away_id)
        event_id, start_time = await ensure_event_for_sdio(self.database, event_row)
        await self._patch_event_teams(event_id, home_id, away_id)
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        return event_id, start_time, home_id, away_id

    # ------------------------------------------------------------------
    # Event lifecycle sync (status + outcomes)
    # ------------------------------------------------------------------
    def _is_game_final(self, game: Game) -> bool:
        """Check if game is in a final/completed state."""
        if game.status is None:
            return False
        return game.status in FINAL_GAME_STATUSES

    async def _resolve_final_scores(
        self,
        state: LeagueState,
        game: Game,
        now: datetime,
    ) -> Tuple[Optional[int], Optional[int]]:
        if game.home_score is not None and game.away_score is not None:
            return game.home_score, game.away_score
        try:
            odds_set = await self._get_line_history_for_event(state, game.game_id, now, tracked=None)
        except Exception:
            return game.home_score, game.away_score
        if odds_set is None:
            return game.home_score, game.away_score
        return odds_set.home_score, odds_set.away_score

    def _game_status_to_event_status(self, game: Game) -> str:
        """Map SDIO game status to our event status."""
        if game.status is None:
            return "scheduled"
        mapping = {
            GameStatus.SCHEDULED: "scheduled",
            GameStatus.IN_PROGRESS: "in_play",
            GameStatus.FINAL: "finished",
            GameStatus.FINAL_OT: "finished",
            GameStatus.FINAL_SO: "finished",
            GameStatus.FINAL_AET: "finished",
            GameStatus.FINAL_PEN: "finished",
            GameStatus.AWARDED: "finished",
            GameStatus.FORFEIT: "finished",
            GameStatus.POSTPONED: "postponed",
            GameStatus.CANCELED: "canceled",
        }
        return mapping.get(game.status, "scheduled")

    async def _sync_event_status(self, event_id: int, game: Game, now: datetime) -> bool:
        """Update event status from SDIO game status. Returns True if updated."""
        new_status = self._game_status_to_event_status(game)
        result = await self.database.write(
            _UPDATE_EVENT_STATUS,
            params={"event_id": event_id, "status": new_status},
        )
        if result > 0:
            bt.logging.debug({
                "sdio_event_status_updated": {
                    "event_id": event_id,
                    "game_id": game.game_id,
                    "new_status": new_status,
                }
            })
            return True
        return False

    async def _record_outcomes(
        self,
        event_id: int,
        game: Game,
        home_team_id: Optional[int],
        away_team_id: Optional[int],
        *,
        home_score_override: Optional[int] = None,
        away_score_override: Optional[int] = None,
    ) -> int:
        """Record outcomes for all markets of a finished game. Returns count recorded."""
        missing_logged = getattr(self, "_outcome_score_missing_logged", None)
        if missing_logged is None:
            missing_logged = set()
            self._outcome_score_missing_logged = missing_logged
        home_score = game.home_score if home_score_override is None else home_score_override
        away_score = game.away_score if away_score_override is None else away_score_override
        if home_score is None or away_score is None:
            if event_id not in missing_logged:
                bt.logging.debug({
                    "sdio_outcome_skip": {
                        "event_id": event_id,
                        "reason": "scores_not_available",
                    }
                })
                missing_logged.add(event_id)
            return 0

        missing_logged.discard(event_id)
        settled_at = datetime.now(timezone.utc)

        # Get all markets for this event
        markets = await self.database.read(
            _SELECT_MARKETS_FOR_EVENT,
            params={"event_id": event_id},
            mappings=True,
        )

        if not markets:
            return 0

        recorded = 0
        for market in markets:
            market_id = market["market_id"]
            kind = market["kind"]
            line = float(market["line"]) if market["line"] is not None else None
            points_team_id = market["points_team_id"]

            # Check if outcome already exists
            existing = await self.database.read(
                _CHECK_OUTCOME_EXISTS,
                params={"market_id": market_id},
            )
            if existing:
                continue

            # Resolve result based on market type
            result = self._resolve_market_result(
                kind=kind,
                line=line,
                points_team_id=points_team_id,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_score=home_score,
                away_score=away_score,
            )

            if result is None:
                bt.logging.debug({
                    "sdio_outcome_unresolved": {
                        "market_id": market_id,
                        "kind": kind,
                    }
                })
                continue

            try:
                await self.database.write(
                    _UPSERT_OUTCOME,
                    params={
                        "market_id": market_id,
                        "settled_at": settled_at,
                        "result": result.upper(),
                        "score_home": home_score,
                        "score_away": away_score,
                        "details": json.dumps({
                            "source": "sdio_auto",
                            "game_id": game.game_id,
                            "game_status": game.status.value if game.status else None,
                        }),
                    },
                )
                recorded += 1
                bt.logging.debug({
                    "sdio_outcome_recorded": {
                        "market_id": market_id,
                        "kind": kind,
                        "result": result,
                        "score": f"{home_score}-{away_score}",
                    }
                })
            except Exception as e:
                bt.logging.warning({
                    "sdio_outcome_error": {
                        "market_id": market_id,
                        "error": str(e),
                    }
                })

        if recorded > 0:
            bt.logging.info({
                "sdio_outcomes_recorded": {
                    "event_id": event_id,
                    "game_id": game.game_id,
                    "count": recorded,
                    "score": f"{home_score}-{away_score}",
                }
            })

        return recorded

    def _resolve_market_result(
        self,
        kind: str,
        line: Optional[float],
        points_team_id: Optional[int],
        home_team_id: Optional[int],
        away_team_id: Optional[int],
        home_score: int,
        away_score: int,
    ) -> Optional[str]:
        """Resolve market result from scores."""
        kind_upper = kind.upper() if kind else ""

        if kind_upper == "MONEYLINE":
            if home_score > away_score:
                return "home"
            if home_score < away_score:
                return "away"
            return "draw"
        elif kind_upper == "TOTAL" and line is not None:
            return resolve_total_result(line, home_score, away_score)
        elif kind_upper == "SPREAD" and line is not None:
            # Determine if points_team is home or away
            points_team_is_home = (points_team_id == home_team_id) if points_team_id else True
            return resolve_spread_result(line, points_team_is_home, home_score, away_score)

        return None

    def _ensure_tracked_event(
        self,
        state: LeagueState,
        game: Game,
        event_id: int,
        start_time: datetime,
        home_team_id: Optional[int],
        away_team_id: Optional[int],
    ) -> None:
        key = self._tracked_key(state.config.code, game.game_id)
        tracked = self.tracked_events.get(key)
        if tracked is None:
            tracked = TrackedEvent(
                league_code=state.config.code,
                game_id=game.game_id,
                event_id=event_id,
                start_time=start_time,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
            )
            self.tracked_events[key] = tracked
        else:
            tracked.event_id = event_id
            tracked.start_time = start_time
            tracked.home_team_id = home_team_id
            tracked.away_team_id = away_team_id

    # ------------------------------------------------------------------
    # Odds snapshots
    # ------------------------------------------------------------------
    async def _refresh_odds(self, state: LeagueState, now: datetime, *, metrics: Optional[Dict[str, Any]] = None) -> None:
        events = list(self._events_for_league(state.config.code))
        if metrics is not None:
            metrics["events_tracked"] = len(events)
        stale_cutoff = now - timedelta(days=self.POST_START_CUTOFF_DAYS)
        pending: List[TrackedEvent] = []
        for tracked in events:
            if tracked.start_time < stale_cutoff:
                self._finalize_tracked_event(state, tracked, reason="past_cutoff")
            elif tracked.start_time <= now and tracked.post_start_polls_remaining <= 0:
                self._finalize_tracked_event(state, tracked, reason="post_start_window_exhausted")
            elif tracked.next_snapshot_at <= now:
                pending.append(tracked)

        if not pending:
            return

        sem = asyncio.Semaphore(self.MAX_CONCURRENT_SNAPSHOTS)

        async def _snap(t: TrackedEvent) -> None:
            async with sem:
                await self._capture_snapshot(state, t, now, metrics=metrics)

        results = await asyncio.gather(
            *(_snap(t) for t in pending), return_exceptions=True,
        )
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                bt.logging.warning({
                    "sdio_snapshot_error": {
                        "league": state.config.code.value,
                        "game_id": int(pending[i].game_id),
                        "error": str(result),
                    }
                })

    def _events_for_league(self, league_code: LeagueCode) -> Iterable[TrackedEvent]:
        return [ev for ev in self.tracked_events.values() if ev.league_code == league_code]

    async def _capture_snapshot(self, state: LeagueState, tracked: TrackedEvent, now: datetime, *, metrics: Optional[Dict[str, Any]] = None) -> None:
        if metrics is not None:
            metrics["snapshot_attempts"] += 1
        bt.logging.debug(
            {
                "sdio_snapshot_start": {
                    "league": state.config.code.value,
                    "game_id": int(tracked.game_id),
                    "event_id": tracked.event_id,
                    "start_time": self._ts(tracked.start_time),
                    "next_snapshot_at": self._ts(tracked.next_snapshot_at),
                    "watermark": self._ts(tracked.last_history_ts),
                    "seen_ids": len(tracked.seen_odd_ids),
                }
            }
        )
        odds_set = await self._resolve_line_history(state, tracked, now)
        if not odds_set or not odds_set.pregame:
            bt.logging.debug(
                {
                    "sdio_snapshot_no_history": {
                        "league": state.config.code.value,
                        "game_id": int(tracked.game_id),
                        "reason": "no_history" if not odds_set else "empty_pregame",
                    }
                }
            )
            tracked.next_snapshot_at = now + timedelta(minutes=state.config.odds_refresh_minutes)
            if metrics is not None:
                metrics["snapshot_missed"] += 1
            return
        latest_ts = await self._persist_odds(state, tracked, odds_set)
        if latest_ts is None:
            bt.logging.debug(
                {
                    "sdio_snapshot_no_new_lines": {
                        "league": state.config.code.value,
                        "game_id": int(tracked.game_id),
                    }
                }
            )
            tracked.next_snapshot_at = now + timedelta(minutes=state.config.odds_refresh_minutes)
            if metrics is not None:
                metrics["snapshot_missed"] += 1
            return None
        if metrics is not None:
            metrics["snapshot_success"] += 1
        tracked.next_snapshot_at = now + self._next_snapshot_interval(state, tracked, now)
        if tracked.start_time <= now:
            if not tracked.closing_captured:
                tracked.closing_captured = True
                tracked.closing_ts = latest_ts
                await self._record_closing_snapshot(state, tracked, latest_ts)
            tracked.post_start_polls_remaining = max(0, tracked.post_start_polls_remaining - 1)
            if tracked.post_start_polls_remaining == 0:
                self._finalize_tracked_event(state, tracked, reason="post_start_window_exhausted")

    async def _resolve_line_history(self, state: LeagueState, tracked: TrackedEvent, now: datetime) -> Optional[GameOddsSet]:
        return await self._get_line_history_for_event(state, tracked.game_id, now, tracked=tracked)

    async def _get_line_history_for_event(
        self, state: LeagueState, game_id: int, now: datetime, *, tracked: Optional[TrackedEvent] = None
    ) -> Optional[GameOddsSet]:
        key = (state.config.code, int(game_id))
        ttl_minutes = max(1, state.config.odds_refresh_minutes)
        
        # Check in-memory cache first
        entry = self._odds_cache.get(key)
        if entry and entry.expires_at > now:
            bt.logging.debug(
                {
                    "sdio_history_cache_hit": {
                        "league": state.config.code.value,
                        "game_id": int(game_id),
                        "expires_at": self._ts(entry.expires_at),
                    }
                }
            )
            return entry.odds
        
        # Check DB watermark - if we have recent quotes, skip API call
        if tracked and tracked.last_history_ts > datetime.min.replace(tzinfo=timezone.utc):
            cache_cutoff = now - timedelta(minutes=ttl_minutes)
            if tracked.last_history_ts > cache_cutoff:
                bt.logging.debug(
                    {
                        "sdio_history_db_fresh": {
                            "league": state.config.code.value,
                            "game_id": int(game_id),
                            "watermark": self._ts(tracked.last_history_ts),
                            "cutoff": self._ts(cache_cutoff),
                        }
                    }
                )
                # Put empty entry in cache to prevent repeated checks
                self._odds_cache[key] = OddsCacheEntry(
                    expires_at=now + timedelta(minutes=ttl_minutes),
                    odds=None,
                )
                return None
        
        try:
            odds_set = await self.client.fetch_line_history(state.config, int(game_id))
        except Exception as exc:
            bt.logging.warning(
                {
                    "sdio_odds_fetch_error": str(exc),
                    "league": state.config.code.value,
                    "game_id": int(game_id),
                }
            )
            self._odds_cache.pop(key, None)
            return None
        bt.logging.debug(
            {
                "sdio_history_cache_miss": {
                    "league": state.config.code.value,
                    "game_id": int(game_id),
                    "pregame_count": len(odds_set.pregame) if odds_set else 0,
                }
            }
        )
        self._odds_cache[key] = OddsCacheEntry(
            expires_at=now + timedelta(minutes=ttl_minutes),
            odds=odds_set,
        )
        return odds_set

    def _evict_expired_cache(self, now: datetime) -> None:
        expired = [key for key, entry in self._odds_cache.items() if entry.expires_at <= now]
        for key in expired:
            self._odds_cache.pop(key, None)

    def _next_snapshot_interval(self, state: LeagueState, tracked: TrackedEvent, now: datetime) -> timedelta:
        minutes = tracked.minutes_to_start(now)
        if minutes > 24 * 60:
            return timedelta(minutes=state.config.odds_refresh_minutes)
        if minutes > 120:
            return timedelta(minutes=state.config.hot_odds_refresh_minutes)
        return timedelta(minutes=max(1, state.config.hot_odds_refresh_minutes // 2))

    def _filter_new_pregame(self, tracked: TrackedEvent, odds_set: GameOddsSet) -> List[GameOdds]:
        fresh: List[GameOdds] = []
        for odds in odds_set.pregame or []:
            odd_id = getattr(odds, "game_odd_id", None)
            updated = odds.updated or tracked.last_history_ts
            if odd_id is not None:
                if odd_id in tracked.seen_odd_ids:
                    continue
            elif updated <= tracked.last_history_ts:
                continue
            fresh.append(odds)
        return fresh

    def _mark_history_processed(self, tracked: TrackedEvent, processed: Iterable[GameOdds]) -> None:
        latest = tracked.last_history_ts
        for odds in processed:
            odd_id = getattr(odds, "game_odd_id", None)
            if odd_id is not None:
                tracked.seen_odd_ids.add(odd_id)
            if odds.updated and odds.updated > latest:
                latest = odds.updated
        tracked.last_history_ts = latest if latest.tzinfo else latest.replace(tzinfo=timezone.utc)

    async def _record_closing_snapshot(self, state: LeagueState, tracked: TrackedEvent, closing_ts: datetime) -> None:
        closing_ts = closing_ts if closing_ts.tzinfo else closing_ts.replace(tzinfo=timezone.utc)
        bt.logging.info(
            {
                "sdio_closing_capture": {
                    "league": state.config.code.value,
                    "game_id": int(tracked.game_id),
                    "event_id": tracked.event_id,
                    "closing_ts": self._ts(closing_ts),
                }
            }
        )
        try:
            await upsert_provider_closing_for_event(
                database=self.database,
                provider_id=self.provider_id,
                event_id=tracked.event_id,
                close_ts=closing_ts,
            )
        except Exception as exc:
            bt.logging.warning(
                {
                    "sdio_closing_upsert_error": str(exc),
                    "league": state.config.code.value,
                    "game_id": int(tracked.game_id),
                }
            )

    def _finalize_tracked_event(self, state: LeagueState, tracked: TrackedEvent, *, reason: str) -> None:
        key = self._tracked_key(tracked.league_code, tracked.game_id)
        self.tracked_events.pop(key, None)
        self._odds_cache.pop((tracked.league_code, tracked.game_id), None)
        # Evict market ID cache entries for this event
        stale_keys = [k for k in self._market_id_cache if k[0] == tracked.event_id]
        for k in stale_keys:
            self._market_id_cache.pop(k, None)
        bt.logging.info(
            {
                "sdio_tracked_event_finalized": {
                    "league": state.config.code.value,
                    "game_id": int(tracked.game_id),
                    "event_id": tracked.event_id,
                    "reason": reason,
                }
            }
        )

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    async def _ensure_sportsbooks(self, fresh_odds: List[GameOdds]) -> None:
        """Register any new sportsbook codes found in fresh odds.

        Uses an in-memory cache to avoid querying the DB on every call.
        Only hits the DB when genuinely new codes are discovered.
        """
        codes = {odds.sportsbook for odds in fresh_odds if odds.sportsbook}
        if not codes:
            return

        # Lazy-load existing codes on first call
        if self._known_sportsbook_codes is None:
            existing_rows = await self.database.read(
                _SELECT_SPORTSBOOK_CODES,
                params={"provider_id": self.provider_id},
                mappings=True,
            )
            self._known_sportsbook_codes = {r["code"] for r in existing_rows}

        new_codes = codes - self._known_sportsbook_codes
        if not new_codes:
            return

        for code in sorted(new_codes):
            rows = await self.database.write(
                _UPSERT_SPORTSBOOK,
                params={"provider_id": self.provider_id, "code": code, "name": code},
                return_rows=True,
            )
            self._known_sportsbook_codes.add(code)
            if rows:
                sportsbook_id = rows[0][0]
                await self.database.write(
                    _SEED_SPORTSBOOK_BIAS,
                    params={"sportsbook_id": sportsbook_id},
                )
                bt.logging.info({
                    "sportsbook_registered": {
                        "code": code,
                        "sportsbook_id": sportsbook_id,
                    }
                })

    async def _resolve_markets_cached(
        self, event_id: int, market_rows: List[dict],
    ) -> Dict[MarketKey, int]:
        """Resolve market IDs with a two-layer strategy: in-memory cache, then batch DB."""
        uncached: List[dict] = []
        result: Dict[MarketKey, int] = {}
        for mrow in market_rows:
            key = _market_key(mrow)
            cache_key = (event_id, key)
            if cache_key in self._market_id_cache:
                result[key] = self._market_id_cache[cache_key]
            else:
                uncached.append(mrow)

        if uncached:
            from_db = await ensure_markets_batch(
                self.database, uncached, event_id=event_id,
            )
            for key, mid in from_db.items():
                result[key] = mid
                self._market_id_cache[(event_id, key)] = mid

        return result

    async def _persist_odds(self, state: LeagueState, tracked: TrackedEvent, odds_set: GameOddsSet) -> Optional[datetime]:
        fresh_odds = self._filter_new_pregame(tracked, odds_set)
        if not fresh_odds:
            bt.logging.debug(
                {
                    "sdio_history_no_fresh_rows": {
                        "league": state.config.code.value,
                        "game_id": int(tracked.game_id),
                    }
                }
            )
            return None
        await self._ensure_sportsbooks(fresh_odds)

        # Collect all distinct market rows across fresh odds, then batch-resolve
        all_market_rows: List[dict] = []
        odds_market_rows: List[List[dict]] = []
        for odds in fresh_odds:
            rows = ensure_markets_for_event(
                tracked.event_id,
                spread_lines=self._extract_spread_lines(odds),
                total_points_list=self._extract_total_points(odds),
                home_team_id=tracked.home_team_id,
                away_team_id=tracked.away_team_id,
            )
            odds_market_rows.append(rows)
            all_market_rows.extend(rows)

        resolved = await self._resolve_markets_cached(tracked.event_id, all_market_rows)

        quotes: List[ProviderQuoteRow] = []
        provider_id = self.provider_id
        for odds, rows in zip(fresh_odds, odds_market_rows):
            for mrow in rows:
                key = _market_key(mrow)
                market_id = resolved.get(key)
                if market_id is None:
                    continue
                quotes.extend(self._map_quotes_for_market(odds, provider_id, market_id))
        if quotes:
            await insert_provider_quotes(database=self.database, quotes=quotes)
        bt.logging.info({
            "sdio_odds_persisted": {
                "league": state.config.code.value,
                "game_id": int(tracked.game_id),
                "event_id": tracked.event_id,
                "markets": len(resolved),
                "quotes": len(quotes),
                "snapshots": len(fresh_odds),
            }
        })
        self._mark_history_processed(tracked, fresh_odds)
        return tracked.last_history_ts

    def _extract_spread_lines(self, odds) -> List[float]:
        lines: List[float] = []
        try:
            value = odds.spread.point_spread
            if value is not None:
                lines.append(float(value))
        except Exception:
            pass
        return lines

    def _extract_total_points(self, odds) -> List[float]:
        totals: List[float] = []
        try:
            value = odds.total.total_points
            if value is not None:
                totals.append(float(value))
        except Exception:
            pass
        return totals

    def _map_quotes_for_market(self, odds, provider_id: int, market_id: int) -> List[ProviderQuoteRow]:
        quotes: List[ProviderQuoteRow] = []
        quotes.extend(map_moneyline_quotes(odds, provider_id, market_id, ts=odds.updated))
        quotes.extend(map_spread_quotes(odds, provider_id, market_id, ts=odds.updated))
        quotes.extend(map_total_quotes(odds, provider_id, market_id, ts=odds.updated))
        return list(quotes)


__all__ = ["SportsDataIngestor"]

