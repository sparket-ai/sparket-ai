from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Sequence

import pytest

from sparket.providers.sportsdataio.config import LeagueCode, LeagueConfig, SportsDataIOConfig
from sparket.providers.sportsdataio.types import Game, GameOdds, GameOddsSet
from sparket.validator.services import SportsDataIngestor, TrackedEvent


class FakeClient:
    def __init__(self, games: Any, odds: Any):
        self._games_by_league: Dict[str, List[Game]] = {}
        self._games_default: List[Game] = []
        if isinstance(games, dict):
            for key, value in games.items():
                league_key = self._normalize_league_key(key)
                entries = value if isinstance(value, (list, tuple)) else [value]
                self._games_by_league[league_key] = list(entries)
        elif isinstance(games, (list, tuple)):
            self._games_default = list(games)
        elif games is not None:
            self._games_default = [games]

        self._history: Dict[tuple[str, int] | int, GameOddsSet] = {}
        if isinstance(odds, dict):
            for key, value in odds.items():
                if isinstance(key, tuple) and len(key) == 2:
                    league_key = self._normalize_league_key(key[0])
                    self._history[(league_key, int(key[1]))] = value
                else:
                    self._history[int(key)] = value
        elif isinstance(odds, (list, tuple)):
            for item in odds:
                self._history[int(item.game_id)] = item
        elif odds is not None:
            self._history[int(odds.game_id)] = odds
        self.schedule_requests = 0
        self.odds_requests = 0

    async def close(self) -> None:  # pragma: no cover - not used
        return None

    async def fetch_team_catalog(self, league_config):  # pragma: no cover - skipped
        return []

    async def fetch_schedule_window(self, league_config, start_date, end_date):
        self.schedule_requests += 1
        games = self._games_by_league.get(self._normalize_league_key(league_config), self._games_default)
        return list(games)

    async def fetch_schedule_season(self, league_config, season_code, season_type=None):
        self.schedule_requests += 1
        games = self._games_by_league.get(self._normalize_league_key(league_config), self._games_default)
        return list(games)

    async def fetch_line_history(self, league_config, game_id):
        self.odds_requests += 1
        league_key = self._normalize_league_key(league_config)
        return self._history.get((league_key, int(game_id))) or self._history.get(int(game_id))

    def _normalize_league_key(self, league_config) -> str:
        if isinstance(league_config, LeagueConfig):
            return league_config.code.value
        if isinstance(league_config, LeagueCode):
            return league_config.value
        if isinstance(league_config, str):
            try:
                return LeagueCode(league_config).value
            except Exception:
                return league_config.lower()
        return str(league_config).lower()


class StubDatabase:
    async def read(self, *_, **__):
        return []

    async def write(self, *_, **__):
        return 0


def base_league_config() -> LeagueConfig:
    return LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{DATE}",
        odds_url="https://example.com/odds/{GAMEID}",
        delta_url=None,
        teams_url="https://example.com/teams",
        schedule_refresh_minutes=60,
        odds_refresh_minutes=15,
        hot_odds_refresh_minutes=5,
        delta_minutes=10,
        hot_delta_minutes=2,
        track_days_ahead=7,
    )


def test_run_once_invokes_persist(monkeypatch):
    asyncio.run(_run_once_invokes_persist(monkeypatch))


async def _run_once_invokes_persist(monkeypatch):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    start_dt = now + timedelta(days=2)
    game = Game.model_validate(
        {
            "GameID": 1,
            "Season": 2024,
            "SeasonType": "Regular",
            "Week": 1,
            "Date": start_dt.isoformat(),
            "HomeTeam": "NE",
            "AwayTeam": "NYJ",
        }
    )
    odds = GameOdds.model_validate(
        {
            "GameID": 1,
            "Sportsbook": "TestBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -110,
            "MoneyLineAway": 120,
            "PointSpread": -3.5,
            "PointSpreadHome": -110,
            "PointSpreadAway": -110,
            "OverUnder": 45.5,
            "OverPayout": -110,
            "UnderPayout": -105,
        }
    )
    odds_set = GameOddsSet(game_id=1, pregame=[odds])

    league_cfg = LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{DATE}",
        odds_url="https://example.com/odds/{GAMEID}",
        delta_url=None,
        teams_url="https://example.com/teams",
        schedule_refresh_minutes=60,
        odds_refresh_minutes=15,
        hot_odds_refresh_minutes=5,
        delta_minutes=10,
        hot_delta_minutes=2,
        track_days_ahead=7,
    )
    config = SportsDataIOConfig(leagues=[league_cfg])
    client = FakeClient(game, odds_set)
    ingestor = SportsDataIngestor(database=StubDatabase(), client=client, config=config)

    # Pre-load league metadata to avoid DB dependency
    for state in ingestor.leagues.values():
        state.league_id = 10
        state.team_index = {"NE": 1, "NYJ": 2}

    async def fake_ensure_event_for_sdio(database, event_row):
        return int(event_row["ext_ref"]["sportsdataio"]["GameID"]), event_row["start_time_utc"]

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event_for_sdio,
    )

    persist_calls: list[int] = []

    async def fake_persist(self, state, tracked, odds_set):
        persist_calls.append(tracked.game_id)
        return tracked.start_time

    monkeypatch.setattr(
        SportsDataIngestor,
        "_persist_odds",
        fake_persist,
    )

    await ingestor.run_once(now=now)

    assert len(ingestor.tracked_events) == 1
    assert persist_calls == [1]
    assert client.schedule_requests == 1
    assert client.odds_requests == 1


def test_snapshot_interval_speeds_up():
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameID": 9,
            "Season": 2024,
            "SeasonType": "Regular",
            "Week": 1,
            "Date": now.isoformat(),
            "HomeTeam": "CHI",
            "AwayTeam": "GB",
        }
    )
    odds = GameOdds.model_validate(
        {
            "GameID": 9,
            "Sportsbook": "TestBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -110,
            "MoneyLineAway": 120,
            "PointSpread": -3.5,
            "PointSpreadHome": -110,
            "PointSpreadAway": -110,
            "OverUnder": 45.5,
            "OverPayout": -110,
            "UnderPayout": -105,
        }
    )
    odds_set = GameOddsSet(game_id=9, pregame=[odds])

    league_cfg = LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{DATE}",
        odds_url="https://example.com/odds/{GAMEID}",
        delta_url=None,
        teams_url="https://example.com/teams",
    )
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient(game, odds_set), config=config)
    state = SimpleNamespace(config=league_cfg)

    tracked_far = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=1,
        event_id=1,
        start_time=now + timedelta(days=3),
    )
    tracked_warm = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=2,
        event_id=2,
        start_time=now + timedelta(hours=3),
    )
    tracked_hot = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=3,
        event_id=3,
        start_time=now + timedelta(minutes=30),
    )

    far_interval = ingestor._next_snapshot_interval(state, tracked_far, now)
    warm_interval = ingestor._next_snapshot_interval(state, tracked_warm, now)
    hot_interval = ingestor._next_snapshot_interval(state, tracked_hot, now)

    assert far_interval > warm_interval > hot_interval


def test_schedule_tracks_only_window(monkeypatch):
    asyncio.run(_schedule_tracks_only_window(monkeypatch))


async def _schedule_tracks_only_window(monkeypatch):
    now = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
    near = Game.model_validate(
        {
            "GameID": 50,
            "Season": 2025,
            "SeasonType": "Regular",
            "Date": (now + timedelta(hours=6)).isoformat(),
            "HomeTeam": "KC",
            "AwayTeam": "BUF",
        }
    )
    far = Game.model_validate(
        {
            "GameID": 51,
            "Season": 2025,
            "SeasonType": "Regular",
            "Date": (now + timedelta(days=20)).isoformat(),
            "HomeTeam": "DAL",
            "AwayTeam": "NYG",
        }
    )
    odds_set = GameOddsSet(game_id=50, pregame=[])
    client = FakeClient([near, far], odds_set)
    config = SportsDataIOConfig(leagues=[base_league_config()])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=client, config=config)
    state = next(iter(ingestor.leagues.values()))
    state.league_id = 22
    state.team_index = {"KC": 1, "BUF": 2, "DAL": 3, "NYG": 4}

    async def fake_ensure_event(database, event_row):
        gid = event_row["ext_ref"]["sportsdataio"]["GameID"]
        return gid, event_row["start_time_utc"]

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event,
    )

    inserted = await ingestor._refresh_schedule(state, now)
    # far game (20 days out) is beyond track_days_ahead=7, so only near game is upserted
    assert inserted == 1
    assert (LeagueCode.NFL, 50) in ingestor.tracked_events
    assert (LeagueCode.NFL, 51) not in ingestor.tracked_events


def test_refresh_schedule_uses_line_history_scores_for_nfl(monkeypatch):
    asyncio.run(_refresh_schedule_uses_line_history_scores_for_nfl(monkeypatch))


async def _refresh_schedule_uses_line_history_scores_for_nfl(monkeypatch):
    now = datetime(2026, 2, 12, 12, 0, tzinfo=timezone.utc)
    final_game = Game.model_validate(
        {
            "GameID": 19098,
            "Season": 2025,
            "SeasonType": "Regular",
            "Date": (now - timedelta(days=2)).isoformat(),
            "Status": "Final",
            "HomeTeam": "LV",
            "AwayTeam": "LAC",
            # No HomeScore/AwayScore in NFL SchedulesBasic payload.
        }
    )
    odds_with_score = GameOddsSet.model_validate(
        {
            "GameID": 19098,
            "Status": "Final",
            "HomeTeamScore": 9,
            "AwayTeamScore": 20,
            "PregameOdds": [],
        }
    )
    client = FakeClient([final_game], {19098: odds_with_score})
    config = SportsDataIOConfig(leagues=[base_league_config()])
    class DbForStatus(StubDatabase):
        async def write(self, *_, **__):
            return 0
    ingestor = SportsDataIngestor(database=DbForStatus(), client=client, config=config)
    state = next(iter(ingestor.leagues.values()))
    state.league_id = 22
    state.team_index = {"LV": 1, "LAC": 2}

    async def fake_ensure_event(database, event_row):
        gid = event_row["ext_ref"]["sportsdataio"]["GameID"]
        return gid, event_row["start_time_utc"]

    captured: list[tuple[int | None, int | None]] = []

    async def fake_record(self, event_id, game, home_team_id, away_team_id, *, home_score_override=None, away_score_override=None):
        captured.append((home_score_override, away_score_override))
        return 0

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event,
    )
    monkeypatch.setattr(SportsDataIngestor, "_record_outcomes", fake_record)

    inserted = await ingestor._refresh_schedule(state, now)
    assert inserted == 1
    assert captured == [(9, 20)]


def test_tracks_games_across_leagues(monkeypatch):
    asyncio.run(_tracks_games_across_leagues(monkeypatch))


async def _tracks_games_across_leagues(monkeypatch):
    now = datetime(2025, 1, 5, 15, 0, tzinfo=timezone.utc)
    nba_game = Game.model_validate(
        {
            "GameID": 77,
            "Season": 2024,
            "SeasonType": "Regular",
            "Date": (now + timedelta(hours=4)).isoformat(),
            "HomeTeam": "LAL",
            "AwayTeam": "BOS",
        }
    )
    nhl_game = Game.model_validate(
        {
            "GameID": 77,
            "Season": 2024,
            "SeasonType": "Regular",
            "Date": (now + timedelta(hours=5)).isoformat(),
            "HomeTeam": "NYR",
            "AwayTeam": "MTL",
        }
    )
    nba_odds = GameOdds.model_validate(
        {
            "GameID": nba_game.game_id,
            "Sportsbook": "HoopsBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -120,
            "MoneyLineAway": 110,
        }
    )
    nhl_odds = GameOdds.model_validate(
        {
            "GameID": nhl_game.game_id,
            "Sportsbook": "IceBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -130,
            "MoneyLineAway": 115,
        }
    )
    nba_cfg = LeagueConfig(
        code=LeagueCode.NBA,
        league_code="nba",
        sport_code="basketball",
        schedule_url="https://example.com/nba/schedule/{DATE}",
        odds_url="https://example.com/nba/odds/{GAMEID}",
        teams_url="https://example.com/nba/teams",
        track_days_ahead=3,
    )
    nhl_cfg = LeagueConfig(
        code=LeagueCode.NHL,
        league_code="nhl",
        sport_code="hockey",
        schedule_url="https://example.com/nhl/schedule/{DATE}",
        odds_url="https://example.com/nhl/odds/{GAMEID}",
        teams_url="https://example.com/nhl/teams",
        track_days_ahead=3,
    )
    config = SportsDataIOConfig(leagues=[nba_cfg, nhl_cfg])
    client = FakeClient(
        games={LeagueCode.NBA: [nba_game], LeagueCode.NHL: [nhl_game]},
        odds={
            (LeagueCode.NBA, nba_game.game_id): GameOddsSet(game_id=nba_game.game_id, pregame=[nba_odds]),
            (LeagueCode.NHL, nhl_game.game_id): GameOddsSet(game_id=nhl_game.game_id, pregame=[nhl_odds]),
        },
    )
    ingestor = SportsDataIngestor(database=StubDatabase(), client=client, config=config)
    for code, state in ingestor.leagues.items():
        state.league_id = 100 if code == LeagueCode.NBA else 200
        if code == LeagueCode.NBA:
            state.team_index = {"LAL": 1, "BOS": 2}
        else:
            state.team_index = {"NYR": 3, "MTL": 4}

    async def fake_ensure_event(database, event_row):
        gid = event_row["ext_ref"]["sportsdataio"]["GameID"]
        return gid + event_row["league_id"], event_row["start_time_utc"]

    async def fake_persist(self, state, tracked, odds_set):
        return tracked.start_time

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event,
    )
    monkeypatch.setattr(SportsDataIngestor, "_persist_odds", fake_persist)

    await ingestor.run_once(now=now)
    assert len(ingestor.tracked_events) == 2
    tracked_leagues = {tracked.league_code for tracked in ingestor.tracked_events.values()}
    assert tracked_leagues == {LeagueCode.NBA, LeagueCode.NHL}


def test_persist_odds_inserts_quotes(monkeypatch):
    asyncio.run(_persist_odds_inserts_quotes(monkeypatch))


async def _persist_odds_inserts_quotes(monkeypatch):
    now = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameID": 7,
            "Season": 2024,
            "SeasonType": "Regular",
            "Week": 2,
            "Date": now.isoformat(),
            "HomeTeam": "KC",
            "AwayTeam": "BUF",
        }
    )
    odds = GameOdds.model_validate(
        {
            "GameID": 7,
            "Sportsbook": "TestBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -130,
            "MoneyLineAway": 110,
            "PointSpread": -3.0,
            "PointSpreadHome": -115,
            "PointSpreadAway": -105,
            "OverUnder": 47.5,
            "OverPayout": -110,
            "UnderPayout": -110,
        }
    )
    odds_set = GameOddsSet(game_id=7, pregame=[odds])

    league_cfg = LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{DATE}",
        odds_url="https://example.com/odds/{GAMEID}",
        delta_url=None,
        teams_url="https://example.com/teams",
    )
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient(game, odds_set), config=config)

    tracked = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=7,
        event_id=700,
        start_time=now + timedelta(hours=1),
    )

    _next_mid = 500

    async def fake_ensure_markets_batch(database, market_rows, *, event_id):
        nonlocal _next_mid
        from sparket.validator.database.resolver import _market_key
        result = {}
        for mrow in market_rows:
            key = _market_key(mrow)
            if key not in result:
                result[key] = _next_mid
                _next_mid += 1
        return result

    captured_quotes = []

    async def fake_insert_provider_quotes(*, database, quotes):
        batch = list(quotes)
        captured_quotes.append(batch)
        return len(batch)

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_markets_batch",
        fake_ensure_markets_batch,
    )
    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.insert_provider_quotes",
        fake_insert_provider_quotes,
    )

    state = next(iter(ingestor.leagues.values()))
    result = await ingestor._persist_odds(state, tracked, odds_set)

    assert isinstance(result, datetime)
    assert result == now
    assert captured_quotes
    assert sum(len(batch) for batch in captured_quotes) >= 4


def test_history_filters_duplicates(monkeypatch):
    asyncio.run(_history_filters_duplicates(monkeypatch))


async def _history_filters_duplicates(monkeypatch):
    now = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameID": 8,
            "Season": 2024,
            "SeasonType": "Regular",
            "Week": 2,
            "Date": (now + timedelta(hours=5)).isoformat(),
            "HomeTeam": "KC",
            "AwayTeam": "BUF",
        }
    )
    odds_a = GameOdds.model_validate(
        {
            "GameID": 8,
            "GameOddId": 1,
            "Sportsbook": "TestBook",
            "Updated": (now - timedelta(hours=2)).isoformat(),
            "MoneyLineHome": -120,
            "MoneyLineAway": 110,
        }
    )
    odds_b = GameOdds.model_validate(
        {
            "GameID": 8,
            "GameOddId": 2,
            "Sportsbook": "TestBook",
            "Updated": (now - timedelta(hours=1)).isoformat(),
            "MoneyLineHome": -125,
            "MoneyLineAway": 115,
        }
    )
    odds_set = GameOddsSet(game_id=8, pregame=[odds_a, odds_b])
    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient(game, odds_set), config=config)

    state = next(iter(ingestor.leagues.values()))
    tracked = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=8,
        event_id=800,
        start_time=now + timedelta(hours=4),
    )

    _next_mid = 500

    async def fake_ensure_markets_batch(database, market_rows, *, event_id):
        nonlocal _next_mid
        from sparket.validator.database.resolver import _market_key
        result = {}
        for mrow in market_rows:
            key = _market_key(mrow)
            if key not in result:
                result[key] = _next_mid
                _next_mid += 1
        return result

    async def fake_insert_provider_quotes(*, database, quotes):
        return len(list(quotes))

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_markets_batch",
        fake_ensure_markets_batch,
    )
    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.insert_provider_quotes",
        fake_insert_provider_quotes,
    )

    first = await ingestor._persist_odds(state, tracked, odds_set)
    assert isinstance(first, datetime)
    seen = set(tracked.seen_odd_ids)
    assert seen == {1, 2}
    watermark = tracked.last_history_ts

    second = await ingestor._persist_odds(state, tracked, odds_set)
    assert second is None
    assert tracked.seen_odd_ids == seen
    assert tracked.last_history_ts == watermark


def test_season_schedule_mode(monkeypatch):
    asyncio.run(_season_schedule_mode(monkeypatch))


async def _season_schedule_mode(monkeypatch):
    now = datetime(2025, 7, 1, 12, 0, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameID": 100,
            "Season": 2025,
            "SeasonType": "Regular",
            "Week": 1,
            "Date": (now + timedelta(days=2)).isoformat(),
            "HomeTeam": "DAL",
            "AwayTeam": "NYG",
        }
    )
    odds = GameOdds.model_validate(
        {
            "GameID": 100,
            "Sportsbook": "TestBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -110,
            "MoneyLineAway": 110,
        }
    )
    odds_set = GameOddsSet(game_id=100, pregame=[odds])
    league_cfg = LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/nfl/scores/json/SchedulesBasic/{SEASON}",
        odds_url="https://example.com/odds/{GAMEID}",
        teams_url="https://example.com/teams",
        schedule_mode="season",
        season_format="{year}{season_type}",
        season_type="REG",
    )
    config = SportsDataIOConfig(leagues=[league_cfg])
    client = FakeClient([game], odds_set)
    ingestor = SportsDataIngestor(database=StubDatabase(), client=client, config=config)

    state = next(iter(ingestor.leagues.values()))
    state.league_id = 30
    state.team_index = {"DAL": 1, "NYG": 2}

    async def fake_ensure_event_for_sdio(database, event_row):
        return int(event_row.get("ext_ref", {}).get("sportsdataio", {}).get("GameID", 0)), event_row["start_time_utc"]

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event_for_sdio,
    )

    async def fake_persist(self, state, tracked, odds_set):
        return tracked.start_time

    monkeypatch.setattr(SportsDataIngestor, "_persist_odds", fake_persist)

    await ingestor.run_once(now=now)
    assert client.schedule_requests == 1
    assert len(ingestor.tracked_events) == 1


def test_snapshot_cache_reuses_fetch(monkeypatch):
    asyncio.run(_snapshot_cache_reuses_fetch(monkeypatch))


async def _snapshot_cache_reuses_fetch(monkeypatch):
    now = datetime(2025, 1, 3, 12, 0, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameID": 11,
            "Season": 2024,
            "SeasonType": "Regular",
            "Week": 3,
            "Date": (now + timedelta(days=1)).isoformat(),
            "HomeTeam": "DAL",
            "AwayTeam": "SF",
        }
    )
    odds = GameOdds.model_validate(
        {
            "GameID": 11,
            "Sportsbook": "TestBook",
            "Updated": now.isoformat(),
            "MoneyLineHome": -115,
            "MoneyLineAway": 105,
        }
    )
    odds_set = GameOddsSet(game_id=11, pregame=[odds])

    league_cfg = LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{DATE}",
        odds_url="https://example.com/odds/{GAMEID}",
        delta_url=None,
        teams_url="https://example.com/teams",
    )
    config = SportsDataIOConfig(leagues=[league_cfg])
    client = FakeClient([game], odds_set)
    ingestor = SportsDataIngestor(database=StubDatabase(), client=client, config=config)

    state = next(iter(ingestor.leagues.values()))
    state.league_id = 20
    tracked = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=11,
        event_id=1100,
        start_time=now + timedelta(days=1, hours=2),
    )
    ingestor.tracked_events = {(LeagueCode.NFL, 11): tracked}

    async def fake_persist(self, state, tracked, odds_set):
        return tracked.start_time

    monkeypatch.setattr(SportsDataIngestor, "_persist_odds", fake_persist)

    await ingestor._refresh_odds(state, now)
    assert client.odds_requests == 1

    tracked.next_snapshot_at = datetime.min.replace(tzinfo=timezone.utc)
    await ingestor._refresh_odds(state, now + timedelta(minutes=1))
    assert client.odds_requests == 1


def test_post_start_finalizes_and_records_closing(monkeypatch):
    asyncio.run(_post_start_finalizes_and_records_closing(monkeypatch))


async def _post_start_finalizes_and_records_closing(monkeypatch):
    now = datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc)
    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], []), config=config)
    state = next(iter(ingestor.leagues.values()))
    state.league_id = 42
    tracked = TrackedEvent(
        league_code=LeagueCode.NFL,
        game_id=900,
        event_id=9000,
        start_time=now - timedelta(minutes=5),
    )
    ingestor.tracked_events = {(LeagueCode.NFL, tracked.game_id): tracked}

    closing_calls: list[datetime] = []

    async def fake_record(self, state, tracked, ts):
        closing_calls.append(ts)

    async def fake_resolve(self, state, tracked, now):
        odds = GameOdds.model_validate(
            {
                "GameID": tracked.game_id,
                "Sportsbook": "TestBook",
                "Updated": now.isoformat(),
            }
        )
        return GameOddsSet(game_id=tracked.game_id, pregame=[odds])

    async def fake_persist(self, state, tracked, odds_set):
        return now

    monkeypatch.setattr(SportsDataIngestor, "_record_closing_snapshot", fake_record)
    monkeypatch.setattr(SportsDataIngestor, "_resolve_line_history", fake_resolve)
    monkeypatch.setattr(SportsDataIngestor, "_persist_odds", fake_persist)

    metrics = {"snapshot_attempts": 0, "snapshot_success": 0, "snapshot_missed": 0}
    await ingestor._capture_snapshot(state, tracked, now, metrics=metrics)
    assert tracked.post_start_polls_remaining == 1
    assert closing_calls == [now]
    assert tracked.closing_captured is True

    await ingestor._capture_snapshot(state, tracked, now + timedelta(minutes=1), metrics=metrics)
    assert (LeagueCode.NFL, tracked.game_id) not in ingestor.tracked_events


def test_soccer_final_variants_are_treated_as_finished():
    config = SportsDataIOConfig(leagues=[base_league_config()])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], []), config=config)

    final_aet = Game.model_validate(
        {
            "GameID": 501,
            "Season": 2026,
            "SeasonType": "Regular",
            "Date": "2026-05-18T20:00:00Z",
            "Status": "FinalAET",
            "HomeTeam": "MAN",
            "AwayTeam": "LIV",
        }
    )
    final_pen = Game.model_validate(
        {
            "GameID": 502,
            "Season": 2026,
            "SeasonType": "Regular",
            "Date": "2026-05-18T20:00:00Z",
            "Status": "FinalPEN",
            "HomeTeam": "MAN",
            "AwayTeam": "LIV",
        }
    )

    assert ingestor._is_game_final(final_aet) is True
    assert ingestor._is_game_final(final_pen) is True
    assert ingestor._game_status_to_event_status(final_aet) == "finished"
    assert ingestor._game_status_to_event_status(final_pen) == "finished"


def test_resolve_team_id_supports_casefold_alias():
    index = {
        "BVB": 1,
        "bayer 04 leverkusen": 2,
        "532": 3,
    }
    assert SportsDataIngestor._resolve_team_id(index, "BVB") == 1
    assert SportsDataIngestor._resolve_team_id(index, "BAYER 04 LEVERKUSEN") == 2
    assert SportsDataIngestor._resolve_team_id(index, "532") == 3
    assert SportsDataIngestor._resolve_team_id(index, "unknown") is None


def test_resolve_market_result_moneyline_no_signature_errors():
    config = SportsDataIOConfig(leagues=[base_league_config()])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], []), config=config)

    assert (
        ingestor._resolve_market_result(
            kind="MONEYLINE",
            line=None,
            points_team_id=None,
            home_team_id=1,
            away_team_id=2,
            home_score=20,
            away_score=17,
        )
        == "home"
    )
    assert (
        ingestor._resolve_market_result(
            kind="MONEYLINE",
            line=None,
            points_team_id=None,
            home_team_id=1,
            away_team_id=2,
            home_score=17,
            away_score=20,
        )
        == "away"
    )


def test_upsert_event_resolves_soccer_keys_and_patches(monkeypatch):
    asyncio.run(_upsert_event_resolves_soccer_keys_and_patches(monkeypatch))


async def _upsert_event_resolves_soccer_keys_and_patches(monkeypatch):
    now = datetime(2026, 2, 14, 14, 30, tzinfo=timezone.utc)
    game = Game.model_validate(
        {
            "GameId": 93686,
            "Season": 2026,
            "SeasonType": "Regular",
            "DateTime": now.isoformat(),
            "Status": "Scheduled",
            "HomeTeamName": "BV Borussia 09 Dortmund",
            "AwayTeamName": "1. FSV Mainz 05",
            "HomeTeamKey": "BVB",
            "AwayTeamKey": "MAI",
            "HomeTeamId": 532,
            "AwayTeamId": 528,
        }
    )
    config = SportsDataIOConfig(leagues=[base_league_config()])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], []), config=config)

    class CaptureDatabase(StubDatabase):
        def __init__(self):
            self.calls = []

        async def write(self, query, params=None, **kwargs):
            self.calls.append(params or {})
            return 0

    capture_db = CaptureDatabase()
    ingestor.database = capture_db

    state = next(iter(ingestor.leagues.values()))
    state.league_id = 22
    team_index = {"BVB": 101, "MAI": 102}

    async def fake_ensure_event_for_sdio(database, event_row):
        return 7001, event_row["start_time_utc"]

    monkeypatch.setattr(
        "sparket.validator.services.sportsdata_ingestor.ensure_event_for_sdio",
        fake_ensure_event_for_sdio,
    )

    event_id, _, home_id, away_id = await ingestor._upsert_event(state, game, team_index)

    assert event_id == 7001
    assert home_id == 101
    assert away_id == 102
    assert capture_db.calls
    assert capture_db.calls[0]["event_id"] == 7001
    assert capture_db.calls[0]["home_team_id"] == 101
    assert capture_db.calls[0]["away_team_id"] == 102


# ===========================================================================
# Improvement 1: Parallel odds fetching
# ===========================================================================

def test_parallel_odds_respects_concurrency():
    asyncio.run(_test_parallel_odds_respects_concurrency())


async def _test_parallel_odds_respects_concurrency():
    """With concurrency=2, max 2 snapshots should run simultaneously."""
    now = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    peak_concurrent = 0
    current_concurrent = 0

    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], None), config=config)
    ingestor.MAX_CONCURRENT_SNAPSHOTS = 2

    state = SimpleNamespace(config=league_cfg)

    for i in range(6):
        key = (LeagueCode.NFL, 100 + i)
        ingestor.tracked_events[key] = TrackedEvent(
            league_code=LeagueCode.NFL,
            game_id=100 + i,
            event_id=100 + i,
            start_time=now + timedelta(days=1),
        )

    original_capture = SportsDataIngestor._capture_snapshot

    async def counting_capture(self, st, tracked, n, *, metrics=None):
        nonlocal peak_concurrent, current_concurrent
        current_concurrent += 1
        if current_concurrent > peak_concurrent:
            peak_concurrent = current_concurrent
        await asyncio.sleep(0.01)
        current_concurrent -= 1

    ingestor._capture_snapshot = counting_capture.__get__(ingestor, SportsDataIngestor)
    await ingestor._refresh_odds(state, now)

    assert peak_concurrent <= 2


def test_parallel_odds_single_failure():
    asyncio.run(_test_parallel_odds_single_failure())


async def _test_parallel_odds_single_failure():
    """One failing snapshot shouldn't prevent others from completing."""
    now = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    completed = []

    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], None), config=config)
    state = SimpleNamespace(config=league_cfg)

    for i in range(4):
        key = (LeagueCode.NFL, 200 + i)
        ingestor.tracked_events[key] = TrackedEvent(
            league_code=LeagueCode.NFL,
            game_id=200 + i,
            event_id=200 + i,
            start_time=now + timedelta(days=1),
        )

    async def maybe_fail(self, st, tracked, n, *, metrics=None):
        if tracked.game_id == 201:
            raise RuntimeError("boom")
        completed.append(tracked.game_id)

    ingestor._capture_snapshot = maybe_fail.__get__(ingestor, SportsDataIngestor)
    await ingestor._refresh_odds(state, now)

    assert len(completed) == 3
    assert 201 not in completed


def test_parallel_odds_empty_league():
    asyncio.run(_test_parallel_odds_empty())


async def _test_parallel_odds_empty():
    """Zero tracked events should be a no-op."""
    now = datetime(2025, 6, 1, 12, 0, tzinfo=timezone.utc)
    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], None), config=config)
    state = SimpleNamespace(config=league_cfg)

    metrics = {"events_tracked": 0, "snapshot_attempts": 0, "snapshot_success": 0, "snapshot_missed": 0}
    await ingestor._refresh_odds(state, now, metrics=metrics)
    assert metrics["snapshot_attempts"] == 0


# ===========================================================================
# Improvement 2: Batched market resolution
# ===========================================================================

def test_ensure_markets_batch_creates_missing():
    asyncio.run(_test_ensure_markets_batch_creates())


async def _test_ensure_markets_batch_creates():
    from sparket.validator.database.resolver import ensure_markets_batch, MarketKey

    next_id = 1

    class MarketDB:
        async def read(self, query, params=None, mappings=False):
            return []

        async def write(self, query, params=None, return_rows=False, mappings=False):
            nonlocal next_id
            mid = next_id
            next_id += 1
            return 0

    class ReadAfterWrite:
        """Returns empty on first read per kind, then returns after write."""
        def __init__(self):
            self._written = {}
            self._next_id = 1

        async def read(self, query, params=None, mappings=False):
            sql = str(query)
            if "event_id" in (params or {}) and "kind" in (params or {}):
                key = (params["kind"], params.get("line"), params.get("points_team_id"))
                if key in self._written:
                    return [{"market_id": self._written[key]}]
            if params and "event_id" in params and "kind" not in params:
                return [{"market_id": v, "kind": k[0], "line": None, "line_cmp": 0, "pts_cmp": 0, "points_team_id": None}
                        for k, v in self._written.items()]
            return []

        async def write(self, query, params=None, return_rows=False, mappings=False):
            if params and "kind" in params:
                key = (params["kind"], params.get("line"), params.get("points_team_id"))
                mid = self._next_id
                self._next_id += 1
                self._written[key] = mid
            return 0

    db = ReadAfterWrite()
    rows = [
        {"kind": "MONEYLINE", "line": None, "points_team_id": None},
        {"kind": "SPREAD", "line": -3.5, "points_team_id": None},
        {"kind": "TOTAL", "line": 45.5, "points_team_id": None},
    ]
    result = await ensure_markets_batch(db, rows, event_id=1)
    assert len(result) == 3


def test_ensure_markets_batch_empty():
    asyncio.run(_test_batch_empty())


async def _test_batch_empty():
    from sparket.validator.database.resolver import ensure_markets_batch
    result = await ensure_markets_batch(StubDatabase(), [], event_id=1)
    assert result == {}


# ===========================================================================
# Improvement 3: In-memory caches
# ===========================================================================

def test_sportsbook_cache_avoids_repeated_queries():
    asyncio.run(_test_sportsbook_cache())


async def _test_sportsbook_cache():
    """After first load, _ensure_sportsbooks should not re-query DB for known codes."""
    db_reads = 0

    class CountingDB:
        async def read(self, *_, **__):
            nonlocal db_reads
            db_reads += 1
            return [{"code": "FanDuel"}, {"code": "DraftKings"}]

        async def write(self, *_, **__):
            return 0

    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=CountingDB(), client=FakeClient([], None), config=config)

    odds1 = SimpleNamespace(sportsbook="FanDuel")
    odds2 = SimpleNamespace(sportsbook="DraftKings")

    await ingestor._ensure_sportsbooks([odds1, odds2])
    first_reads = db_reads
    await ingestor._ensure_sportsbooks([odds1, odds2])

    assert db_reads == first_reads, "Second call should not query DB"


def test_sportsbook_cache_detects_new_codes():
    asyncio.run(_test_sportsbook_new())


async def _test_sportsbook_new():
    writes = []

    class TrackDB:
        async def read(self, *_, **__):
            return [{"code": "FanDuel"}]

        async def write(self, query, params=None, return_rows=False, mappings=False):
            writes.append(params)
            if return_rows:
                return [(999,)]
            return 0

    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=TrackDB(), client=FakeClient([], None), config=config)

    await ingestor._ensure_sportsbooks([SimpleNamespace(sportsbook="FanDuel")])
    assert len(writes) == 0

    await ingestor._ensure_sportsbooks([SimpleNamespace(sportsbook="BetMGM")])
    assert any(p.get("code") == "BetMGM" for p in writes if p)
    assert "BetMGM" in ingestor._known_sportsbook_codes


def test_market_cache_cleared_on_finalize():
    league_cfg = base_league_config()
    config = SportsDataIOConfig(leagues=[league_cfg])
    ingestor = SportsDataIngestor(database=StubDatabase(), client=FakeClient([], None), config=config)
    state = SimpleNamespace(config=league_cfg)

    tracked = TrackedEvent(
        league_code=LeagueCode.NFL, game_id=42, event_id=42,
        start_time=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    key = (LeagueCode.NFL, 42)
    ingestor.tracked_events[key] = tracked

    from sparket.validator.database.resolver import MarketKey
    from decimal import Decimal
    ingestor._market_id_cache[(42, ("MONEYLINE", None, None))] = 100
    ingestor._market_id_cache[(42, ("SPREAD", Decimal("-3.5"), None))] = 101
    ingestor._market_id_cache[(99, ("MONEYLINE", None, None))] = 200

    ingestor._finalize_tracked_event(state, tracked, reason="test")

    assert (42, ("MONEYLINE", None, None)) not in ingestor._market_id_cache
    assert (42, ("SPREAD", Decimal("-3.5"), None)) not in ingestor._market_id_cache
    assert (99, ("MONEYLINE", None, None)) in ingestor._market_id_cache


# ===========================================================================
# Improvement 4: Smart season resolution
# ===========================================================================

def _nba_config() -> LeagueConfig:
    return LeagueConfig(
        code=LeagueCode.NBA,
        league_code="nba",
        sport_code="basketball",
        schedule_url="https://example.com/games/{SEASON}",
        odds_url="https://example.com/odds/{GAMEID}",
        schedule_mode="season",
        season_format="{year}",
        season_year_offset=1,
        off_season_months=[7, 8, 9],
        transition_months=[6, 10],
    )


def _nfl_config() -> LeagueConfig:
    return LeagueConfig(
        code=LeagueCode.NFL,
        league_code="nfl",
        sport_code="football",
        schedule_url="https://example.com/schedule/{SEASON}",
        odds_url="https://example.com/odds/{GAMEID}",
        schedule_mode="season",
        season_format="{year}{season_type}",
        season_types=["PRE", "REG", "POST"],
        season_year_offset=-1,
        off_season_months=[4, 5, 6, 7],
        transition_months=[2, 3, 8, 9],
    )


def _mlb_config() -> LeagueConfig:
    return LeagueConfig(
        code=LeagueCode.MLB,
        league_code="mlb",
        sport_code="baseball",
        schedule_url="https://example.com/games/{SEASON}",
        odds_url="https://example.com/odds/{GAMEID}",
        schedule_mode="season",
        season_format="{year}",
        off_season_months=[11, 12, 1, 2],
        transition_months=[3, 10],
    )


def test_nba_season_year_october():
    """Oct 2026: NBA new season starts → primary year = 2027."""
    now = datetime(2026, 10, 15, tzinfo=timezone.utc)
    cfg = _nba_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    codes = [s[0] for s in seasons]
    assert "2027" in codes
    # Oct is a transition month, so both 2027 and 2028 should appear
    assert len(codes) == 2


def test_nba_season_year_june():
    """Jun 2026: NBA season ending → primary 2026 + transition."""
    now = datetime(2026, 6, 15, tzinfo=timezone.utc)
    cfg = _nba_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    codes = [s[0] for s in seasons]
    assert "2026" in codes
    assert len(codes) == 2  # June is transition


def test_nba_season_year_march():
    """Mar 2026: mid-season, no transition → just 2026."""
    now = datetime(2026, 3, 15, tzinfo=timezone.utc)
    cfg = _nba_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    codes = [s[0] for s in seasons]
    assert codes == ["2026"]


def test_nfl_includes_preseason():
    """NFL season_types should include PRE, REG, POST."""
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    cfg = _nfl_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    season_types = [s[1] for s in seasons]
    assert "PRE" in season_types
    assert "REG" in season_types
    assert "POST" in season_types


def test_nfl_transition_fetches_adjacent():
    """Sept is a transition month for NFL → should fetch both years."""
    now = datetime(2026, 9, 1, tzinfo=timezone.utc)
    cfg = _nfl_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    years = {s[0][:4] for s in seasons}
    assert len(years) == 2


def test_mlb_calendar_year():
    """MLB: June 2026, no offset → just 2026, no transitions."""
    now = datetime(2026, 6, 15, tzinfo=timezone.utc)
    cfg = _mlb_config()
    seasons = SportsDataIngestor._compute_active_seasons(now, cfg)
    assert len(seasons) == 1
    assert seasons[0][0] == "2026"


def test_off_season_skip():
    asyncio.run(_test_off_season_skip())


async def _test_off_season_skip():
    """During off-season with no upcoming games, schedule refresh should be skipped."""
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)

    class EmptyDB:
        async def read(self, *_, **__):
            return []
        async def write(self, *_, **__):
            return 0

    nfl_cfg = _nfl_config()
    config = SportsDataIOConfig(leagues=[nfl_cfg])
    client = FakeClient([], None)
    ingestor = SportsDataIngestor(database=EmptyDB(), client=client, config=config)

    state = ingestor.leagues[LeagueCode.NFL]
    state.league_id = 1
    state.team_index = {}

    result = await ingestor._refresh_schedule(state, now)
    assert result == 0
    assert client.schedule_requests == 0


def test_off_season_with_upcoming_games():
    asyncio.run(_test_off_season_with_games())


async def _test_off_season_with_games():
    """During off-season but DB has upcoming games → should still fetch."""
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)

    class HasGamesDB:
        async def read(self, query, params=None, mappings=False):
            sql_text = str(getattr(query, "text", query))
            if "status = 'scheduled'" in sql_text:
                return [(1,)]
            return []
        async def write(self, *_, **__):
            return 0

    nfl_cfg = _nfl_config()
    config = SportsDataIOConfig(leagues=[nfl_cfg])
    client = FakeClient([], None)
    ingestor = SportsDataIngestor(database=HasGamesDB(), client=client, config=config)

    state = ingestor.leagues[LeagueCode.NFL]
    state.league_id = 1
    state.team_index = {}

    result = await ingestor._refresh_schedule(state, now)
    assert client.schedule_requests > 0

