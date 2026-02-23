from __future__ import annotations

import asyncio
import os
from datetime import date, datetime
from typing import Any, Iterable, List, Optional, Sequence

import httpx
import bittensor as bt

from .config import LeagueCode, LeagueConfig, SportsDataIOConfig, build_default_config
from .types import Game, GameOddsSet, Team
from .leagues import SoccerCompetition


def format_sdio_date(target_date: date) -> str:
    """SportsDataIO date format: YYYY-MMM-DD (month abbreviation uppercase)."""
    return target_date.strftime("%Y-%b-%d").upper()


class SportsDataIOClient:
    """
    Async HTTP client for SportsDataIO endpoints.

    - Handles API key header `Ocp-Apim-Subscription-Key`
    - Provides helpers to fetch schedules, odds snapshots, and delta feeds
    - Retries transient HTTP errors with exponential backoff
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        config: Optional[SportsDataIOConfig] = None,
        timeout_seconds: float = 10.0,
        max_retries: int = 2,
    ) -> None:
        self.api_key = api_key or os.getenv("SDIO_API_KEY")
        self.config = config or build_default_config()
        self.max_retries = max_retries
        limits = httpx.Limits(max_keepalive_connections=20, max_connections=40)
        self._client = httpx.AsyncClient(timeout=timeout_seconds, limits=limits)

    async def close(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "SportsDataIOClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Public fetch helpers
    # ------------------------------------------------------------------
    async def fetch_schedule_by_date(
        self,
        league: LeagueConfig | LeagueCode | str,
        target_date: date,
    ) -> List[Game]:
        cfg = self._resolve_league(league)
        url = str(cfg.schedule_url).replace("{DATE}", format_sdio_date(target_date))
        payload = await self._get_json(url)
        return self._parse_games(payload)

    async def fetch_schedule_window(
        self,
        league: LeagueConfig | LeagueCode | str,
        start_date: date,
        end_date: date,
    ) -> List[Game]:
        games: List[Game] = []
        current = start_date
        while current <= end_date:
            games.extend(await self.fetch_schedule_by_date(league, current))
            current = date.fromordinal(current.toordinal() + 1)
        return games

    async def fetch_schedule_season(
        self,
        league: LeagueConfig | LeagueCode | str,
        season_code: str,
        *,
        season_type: Optional[str] = None,
    ) -> List[Game]:
        cfg = self._resolve_league(league)
        url = str(cfg.schedule_url)
        if "{SEASON}" in url:
            url = url.replace("{SEASON}", season_code)
        if season_type and "{SEASONTYPE}" in url:
            url = url.replace("{SEASONTYPE}", season_type)
        if "{YEAR}" in url:
            url = url.replace("{YEAR}", season_code[:4])
        bt.logging.debug({"sdio_schedule_request": {"league": cfg.code.value, "season_code": season_code, "url": url}})
        payload = await self._get_json(url)
        games = self._parse_games(payload)
        bt.logging.debug({"sdio_schedule_response": {"league": cfg.code.value, "raw_count": len(payload) if isinstance(payload, list) else 0, "parsed_count": len(games)}})
        return games

    async def fetch_line_history(
        self,
        league: LeagueConfig | LeagueCode | str,
        game_id: int,
    ) -> Optional[GameOddsSet]:
        cfg = self._resolve_league(league)
        url = str(cfg.odds_url)
        if "{GAMEID}" not in url:
            raise ValueError("LeagueConfig.odds_url must contain {GAMEID} placeholder for line history.")
        url = url.replace("{GAMEID}", str(game_id))
        bt.logging.debug(
            {
                "sdio_line_history_request": {
                    "league": cfg.code.value,
                    "game_id": int(game_id),
                    "url": url,
                }
            }
        )
        payload = await self._get_json(url)
        odds_set = self._parse_odds_set(payload)
        bt.logging.debug(
            {
                "sdio_line_history_response": {
                    "league": cfg.code.value,
                    "game_id": int(game_id),
                    "pregame_count": len(odds_set.pregame) if odds_set and odds_set.pregame else 0,
                }
            }
        )
        return odds_set

    async def fetch_team_catalog(
        self,
        league: LeagueConfig | LeagueCode | str,
    ) -> List[Team]:
        cfg = self._resolve_league(league)
        if not cfg.teams_url:
            return []
        payload = await self._get_json(str(cfg.teams_url))
        return self._parse_teams(payload)

    async def fetch_soccer_competitions(self) -> List[SoccerCompetition]:
        """Fetch all soccer competitions available to this API key.
        
        Tries v4 endpoint first, falls back to v3 if unavailable.
        """
        urls = [
            "https://api.sportsdata.io/v4/soccer/scores/json/Competitions",
            "https://api.sportsdata.io/v3/soccer/scores/json/Competitions",
        ]
        for url in urls:
            try:
                payload = await self._get_json(url)
                if payload:
                    return self._parse_soccer_competitions(payload)
            except Exception as exc:
                bt.logging.debug({"sdio_competitions_fallback": {"url": url, "error": str(exc)}})
                continue
        return []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_league(self, league: LeagueConfig | LeagueCode | str) -> LeagueConfig:
        if isinstance(league, LeagueConfig):
            return league
        cfg = self.config.league_by_code(league)
        if cfg is None:
            raise ValueError(f"Unknown league config for {league}")
        return cfg

    async def _get_json(self, url: str) -> Any:
        if not self.api_key:
            raise RuntimeError("SDIO_API_KEY is required to query SportsDataIO.")
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        delimiter = "&" if "?" in url else "?"
        url_with_key = f"{url}{delimiter}key={self.api_key}"
        attempt = 0
        backoff = 0.5
        while True:
            try:
                resp = await self._client.get(url_with_key, headers=headers)
                resp.raise_for_status()
                return resp.json()
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 404:
                    bt.logging.debug({"sdio_http_404": {"url": url}})
                    return []
                if status in (401, 403):
                    bt.logging.error({
                        "sdio_auth_error": {
                            "status": status,
                            "url": url.split("?")[0],
                            "body": exc.response.text[:200],
                        }
                    })
                    raise
                if attempt >= self.max_retries:
                    bt.logging.warning({
                        "sdio_http_exhausted": {
                            "status": status,
                            "url": url.split("?")[0],
                            "attempts": attempt + 1,
                        }
                    })
                    raise
                await asyncio.sleep(backoff)
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                if attempt >= self.max_retries:
                    bt.logging.warning({
                        "sdio_request_exhausted": {
                            "error": str(exc),
                            "type": type(exc).__name__,
                            "url": url.split("?")[0],
                            "attempts": attempt + 1,
                        }
                    })
                    raise
                await asyncio.sleep(backoff)
            attempt += 1
            backoff *= 2

    def _parse_games(self, payload: Any) -> List[Game]:
        games: List[Game] = []
        for item in payload or []:
            home_candidate = (
                item.get("HomeTeam")
                or item.get("HomeTeamKey")
                or item.get("HomeTeamName")
                or item.get("HomeTeamId")
                or item.get("HomeTeamID")
            )
            away_candidate = (
                item.get("AwayTeam")
                or item.get("AwayTeamKey")
                or item.get("AwayTeamName")
                or item.get("AwayTeamId")
                or item.get("AwayTeamID")
            )
            try:
                away = str(away_candidate or "").upper()
            except Exception:
                away = ""
            has_identity = any(item.get(key) for key in ("GameID", "GameId", "ScoreID", "GlobalGameID", "GameKey"))
            if not has_identity or away == "BYE":
                bt.logging.debug(
                    {
                        "sdio_skip_game": {
                            "reason": "missing_identity_or_bye",
                            "home": home_candidate,
                            "away": away_candidate,
                            "season": item.get("Season"),
                            "week": item.get("Week"),
                        }
                    }
                )
                continue
            if not home_candidate or not away_candidate:
                bt.logging.debug(
                    {
                        "sdio_skip_game": {
                            "reason": "missing_team_identity",
                            "game_id": item.get("GameID") or item.get("GameId") or item.get("ScoreID"),
                            "home": home_candidate,
                            "away": away_candidate,
                            "status": item.get("Status"),
                        }
                    }
                )
                continue
            try:
                games.append(Game.model_validate(item))
            except Exception as exc:
                bt.logging.debug({"sdio_parse_game_error": str(exc)})
        return games

    def _parse_odds_set(self, payload: Any) -> Optional[GameOddsSet]:
        if not payload:
            return None
        if isinstance(payload, list):
            for item in payload:
                try:
                    return GameOddsSet.model_validate(item)
                except Exception as exc:
                    bt.logging.debug({"sdio_parse_odds_error": str(exc)})
            return None
        try:
            return GameOddsSet.model_validate(payload)
        except Exception as exc:
            bt.logging.debug({"sdio_parse_odds_error": str(exc)})
            return None

    def _parse_teams(self, payload: Any) -> List[Team]:
        teams: List[Team] = []
        for item in payload or []:
            try:
                teams.append(Team.model_validate(item))
            except Exception as exc:
                bt.logging.debug({"sdio_parse_team_error": str(exc)})
        return teams

    def _parse_soccer_competitions(self, payload: Any) -> List[SoccerCompetition]:
        comps: List[SoccerCompetition] = []
        for item in payload or []:
            try:
                comps.append(SoccerCompetition.model_validate(item))
            except Exception as exc:
                bt.logging.debug({"sdio_parse_competition_error": str(exc)})
        return comps


__all__ = [
    "SportsDataIOClient",
    "format_sdio_date",
]

