"""Pydantic models for SportsDataIO provider payloads.

Cross-sport aliasing and normalization are handled here so that downstream
mapping to storage can rely on a consistent shape.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator
from sparket.shared.probability import eu_to_implied_prob

from .enums import GameStatus, League, MarketWindow, SeasonType


def _parse_dt(value: str | datetime | None) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # Normalize common Z/offset forms for Python 3.10
    s = str(value)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except Exception:
        # Last resort: strip timezone
        if "+" in s:
            s = s.split("+", 1)[0]
        parsed = datetime.fromisoformat(s)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _normalize_key(value: Any, *, drop_slashes: bool = False) -> str:
    key = str(value).strip().lower()
    for ch in (" ", "-", "_"):
        key = key.replace(ch, "")
    if drop_slashes:
        key = key.replace("/", "")
    return key


_SEASON_TYPE_ALIASES: Dict[str, SeasonType] = {
    # SportsDataIO uses: 1=Preseason, 2=Regular, 3=Postseason (NFL at least)
    "1": SeasonType.PRE,
    "pre": SeasonType.PRE,
    "preseason": SeasonType.PRE,
    "2": SeasonType.REG,
    "reg": SeasonType.REG,
    "regular": SeasonType.REG,
    "regularseason": SeasonType.REG,
    "3": SeasonType.POST,
    "post": SeasonType.POST,
    "postseason": SeasonType.POST,
    "playoffs": SeasonType.POST,
}


def _coerce_season_type_value(value: Any) -> Any:
    if value is None or isinstance(value, SeasonType):
        return value
    key = _normalize_key(value)
    return _SEASON_TYPE_ALIASES.get(key, value)


_STATUS_ALIASES: Dict[str, GameStatus] = {
    "scheduled": GameStatus.SCHEDULED,
    "sched": GameStatus.SCHEDULED,
    "inprogress": GameStatus.IN_PROGRESS,
    "inprogressgame": GameStatus.IN_PROGRESS,
    "live": GameStatus.IN_PROGRESS,
    "playing": GameStatus.IN_PROGRESS,
    "final": GameStatus.FINAL,
    "f": GameStatus.FINAL,
    "fot": GameStatus.FINAL,
    "finalot": GameStatus.FINAL,
    "fot2": GameStatus.FINAL,
    "finalot2": GameStatus.FINAL,
    "completed": GameStatus.FINAL,
    "complete": GameStatus.FINAL,
    "postponed": GameStatus.POSTPONED,
    "pp": GameStatus.POSTPONED,
    "ppd": GameStatus.POSTPONED,
    "postponedweather": GameStatus.POSTPONED,
    "canceled": GameStatus.CANCELED,
    "cancelled": GameStatus.CANCELED,
    "cnl": GameStatus.CANCELED,
}


def _coerce_status_value(value: Any) -> Any:
    if value is None or isinstance(value, GameStatus):
        return value
    key = _normalize_key(value, drop_slashes=True)
    key = key.replace(".", "")
    return _STATUS_ALIASES.get(key, value)


def american_to_decimal(american: float | int | None) -> Optional[float]:
    """Convert American odds to decimal odds.

    Examples:
        -110 -> 1 + 100/110 = 1.9091
        +120 -> 1 + 120/100 = 2.2
    """
    if american is None:
        return None
    a = float(american)
    # If already decimal odds (most decimal odds > 1.01), pass through
    if a > 1.01 and a < 50 and not (a >= 100 or a <= -100):
        return a
    if a >= 100:
        return 1.0 + (a / 100.0)
    if a <= -100:
        return 1.0 + (100.0 / abs(a))
    # Guard against malformed values; fall back to None
    return None


def decimal_to_implied_prob(decimal_odds: float | None) -> Optional[float]:
    """Backward-compat wrapper delegating to shared probability utils."""
    if decimal_odds is None:
        return None
    try:
        return eu_to_implied_prob(float(decimal_odds))
    except Exception:
        return None


class Team(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    team_id: int = Field(validation_alias=AliasChoices("TeamID", "TeamId"))
    key: str = Field(default="", validation_alias=AliasChoices("Key", "TeamKey", "ShortName"))
    city: Optional[str] = Field(default=None, alias="City")
    name: Optional[str] = Field(default=None, alias="Name")
    conference: Optional[str] = Field(default=None, alias="Conference")
    division: Optional[str] = Field(default=None, alias="Division")


class Location(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    stadium_id: Optional[int] = Field(default=None, alias="StadiumID")
    name: Optional[str] = Field(default=None, alias="Name")
    city: Optional[str] = Field(default=None, alias="City")
    state: Optional[str] = Field(default=None, alias="State")


class Game(BaseModel):
    """Minimal game object covering schedule + identity used across feeds.

    Based on SportsDataIO NFL endpoints (Schedules, Games by date/week).
    We only include fields necessary to map to our validator `event` + external refs.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    game_id: int = Field(alias="GameID")
    season: int = Field(alias="Season")
    season_type: SeasonType = Field(alias="SeasonType")
    week: Optional[int] = Field(default=None, alias="Week")
    date_time: Optional[datetime] = Field(default=None, alias="Date")
    status: Optional[GameStatus] = Field(default=None, alias="Status")
    home_team: str = Field(alias="HomeTeam")
    away_team: str = Field(alias="AwayTeam")
    location: Optional[Location] = Field(default=None)
    # Score fields - populated when game is Final/completed
    home_score: Optional[int] = Field(default=None, alias="HomeScore")
    away_score: Optional[int] = Field(default=None, alias="AwayScore")

    # Cross-sport variants occasionally use different keys; use model-level
    # pre-processing to fill the normalized fields above.
    @classmethod
    def model_validate(cls, obj):  # type: ignore[override]
        if isinstance(obj, dict):
            data = dict(obj)
            # Normalize date/time
            data.setdefault("Date", data.get("DateTime") or data.get("Day"))
            if not data.get("GameID"):
                # Soccer uses GameId (lowercase d), US sports use GameID
                for alias in ("GameId", "ScoreID", "GlobalGameID", "GameKey"):
                    alt_value = data.get(alias)
                    if alt_value not in (None, "", 0):
                        data["GameID"] = alt_value
                        break
            # Normalize team codes - soccer uses TeamName, US sports use Team/TeamKey
            data.setdefault("HomeTeam", data.get("HomeTeamName") or data.get("HomeTeamKey") or data.get("HomeTeamId") or data.get("HomeTeamID"))
            data.setdefault("AwayTeam", data.get("AwayTeamName") or data.get("AwayTeamKey") or data.get("AwayTeamId") or data.get("AwayTeamID"))
            # Map provider variants into our neutral `location` field
            if "location" not in data:
                if "StadiumDetails" in data:
                    data["location"] = data["StadiumDetails"]
                elif "Stadium" in data:
                    data["location"] = data["Stadium"]
                elif "Venue" in data:
                    data["location"] = data["Venue"]
            obj = data
        return super().model_validate(obj)

    @field_validator("season_type", mode="before")
    @classmethod
    def _coerce_season_type(cls, v: Any) -> Any:
        return _coerce_season_type_value(v)

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: Any) -> Any:
        return _coerce_status_value(v)

    @field_validator("date_time", mode="before")
    @classmethod
    def _coerce_dt(cls, v: str | datetime | None) -> Optional[datetime]:
        return _parse_dt(v)


class MoneylinePrice(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    # SportsDataIO common: MoneyLineHome, MoneyLineAway (NBA/NHL/MLB use moneyline naming),
    # Some soccer endpoints include MoneyLineDraw
    home: Optional[float] = Field(default=None, alias="MoneyLineHome")
    away: Optional[float] = Field(default=None, alias="MoneyLineAway")
    draw: Optional[float] = Field(default=None, alias="MoneyLineDraw")

    def as_decimal(self) -> Dict[str, Optional[float]]:
        """Return decimal odds mapping for available sides."""
        return {
            "home": american_to_decimal(self.home),
            "away": american_to_decimal(self.away),
            "draw": american_to_decimal(self.draw),
        }

    def implied_prob(self) -> Dict[str, Optional[float]]:
        dec = self.as_decimal()
        return {k: decimal_to_implied_prob(v) for k, v in dec.items()}


class SpreadPrice(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    point_spread: Optional[float] = Field(default=None, alias="PointSpread")
    home: Optional[float] = Field(default=None, alias="PointSpreadHome")
    away: Optional[float] = Field(default=None, alias="PointSpreadAway")

    def as_decimal(self) -> Dict[str, Optional[float]]:
        return {
            "home": american_to_decimal(self.home),
            "away": american_to_decimal(self.away),
        }

    def implied_prob(self) -> Dict[str, Optional[float]]:
        dec = self.as_decimal()
        return {k: decimal_to_implied_prob(v) for k, v in dec.items()}


class TotalPrice(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    # Some sports may alias as Total, OverUnder, etc. Primary key: OverUnder
    total_points: Optional[float] = Field(default=None, alias="OverUnder")
    over: Optional[float] = Field(default=None, alias="OverPayout")
    under: Optional[float] = Field(default=None, alias="UnderPayout")

    def as_decimal(self) -> Dict[str, Optional[float]]:
        return {
            "over": american_to_decimal(self.over),
            "under": american_to_decimal(self.under),
        }

    def implied_prob(self) -> Dict[str, Optional[float]]:
        dec = self.as_decimal()
        return {k: decimal_to_implied_prob(v) for k, v in dec.items()}


class GameOdds(BaseModel):
    """Odds snapshot for a game from SDIO (pre/live/close)."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    game_id: int = Field(alias="GameID", validation_alias=AliasChoices("GameID", "GameId", "ScoreId"))
    game_odd_id: Optional[int] = Field(default=None, alias="GameOddId")
    sportsbook: Optional[str] = Field(default=None, alias="Sportsbook")
    updated: Optional[datetime] = Field(default=None, alias="Updated")
    window: Optional[MarketWindow] = None

    moneyline: MoneylinePrice = Field(default_factory=MoneylinePrice)
    spread: SpreadPrice = Field(default_factory=SpreadPrice)
    total: TotalPrice = Field(default_factory=TotalPrice)

    # Map known key aliases into nested models
    @field_validator("moneyline", mode="before")
    @classmethod
    def _moneyline_from_parent(cls, v, values):
        if isinstance(v, dict):
            return v
        # Build from top-level fields (cross-sport)
        data = {
            "MoneyLineHome": values.get("MoneyLineHome") or values.get("HomeMoneyLine"),
            "MoneyLineAway": values.get("MoneyLineAway") or values.get("AwayMoneyLine"),
            "MoneyLineDraw": values.get("MoneyLineDraw") or values.get("DrawMoneyLine"),
        }
        return {k: val for k, val in data.items() if val is not None}

    @field_validator("spread", mode="before")
    @classmethod
    def _spread_from_parent(cls, v, values):
        if isinstance(v, dict):
            return v
        data = {
            "PointSpread": values.get("PointSpread") or values.get("Spread"),
            "PointSpreadHome": values.get("PointSpreadHome") or values.get("HomePointSpreadPayout"),
            "PointSpreadAway": values.get("PointSpreadAway") or values.get("AwayPointSpreadPayout"),
        }
        return {k: val for k, val in data.items() if val is not None}

    @field_validator("total", mode="before")
    @classmethod
    def _total_from_parent(cls, v, values):
        if isinstance(v, dict):
            return v
        data = {
            "OverUnder": values.get("OverUnder") or values.get("Total"),
            "OverPayout": values.get("OverPayout") or values.get("OverOdds"),
            "UnderPayout": values.get("UnderPayout") or values.get("UnderOdds"),
        }
        return {k: val for k, val in data.items() if val is not None}

    @field_validator("updated", mode="before")
    @classmethod
    def _coerce_updated(cls, v: str | datetime | None) -> Optional[datetime]:
        return _parse_dt(v)

    @model_validator(mode="before")
    @classmethod
    def _normalize_odds_fields(cls, data: Any) -> Any:
        """Pre-process input dict to normalize field aliases from various SDIO endpoints."""
        if not isinstance(data, dict):
            return data
        data = dict(data)  # Make a copy to avoid mutating input
        
        # Normalize game ID aliases
        data.setdefault("GameID", data.get("GameId") or data.get("ScoreId") or data.get("GlobalGameId"))
        # Normalize alternative sportsbook/vendor keys
        data.setdefault("Updated", data.get("LastUpdated") or data.get("Created"))
        # Some feeds embed sportsbook name under `SportsbookId`/`SportsBook`
        data.setdefault("Sportsbook", data.get("SportsBook") or data.get("SportsbookName"))
        
        # Build nested odds blocks from top-level aliases if not present
        if "moneyline" not in data:
            ml = {
                "MoneyLineHome": data.get("MoneyLineHome") or data.get("HomeMoneyLine") or data.get("HomeMoneyline"),
                "MoneyLineAway": data.get("MoneyLineAway") or data.get("AwayMoneyLine") or data.get("AwayMoneyline"),
                "MoneyLineDraw": data.get("MoneyLineDraw") or data.get("DrawMoneyLine") or data.get("DrawMoneyline"),
            }
            if any(v is not None for v in ml.values()):
                data["moneyline"] = {k: v for k, v in ml.items() if v is not None}
        if "spread" not in data:
            sp = {
                "PointSpread": data.get("PointSpread") or data.get("Spread") or data.get("HomePointSpread"),
                "PointSpreadHome": data.get("PointSpreadHome") or data.get("HomePointSpreadPayout") or data.get("HomeSpread") or data.get("HomeSpreadPayout"),
                "PointSpreadAway": data.get("PointSpreadAway") or data.get("AwayPointSpreadPayout") or data.get("AwaySpread") or data.get("AwaySpreadPayout"),
            }
            if any(v is not None for v in sp.values()):
                data["spread"] = {k: v for k, v in sp.items() if v is not None}
        if "total" not in data:
            tp = {
                "OverUnder": data.get("OverUnder") or data.get("Total"),
                "OverPayout": data.get("OverPayout") or data.get("OverOdds") or data.get("OverLine") or data.get("OverPrice"),
                "UnderPayout": data.get("UnderPayout") or data.get("UnderOdds") or data.get("UnderLine") or data.get("UnderPrice"),
            }
            if any(v is not None for v in tp.values()):
                data["total"] = {k: v for k, v in tp.items() if v is not None}
        return data


class GameOddsSet(BaseModel):
    """Container mapping to endpoints that return lists of lines per game.

    Many SDIO endpoints return `PregameOdds` (and possibly `LiveOdds`) arrays.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    game_id: int = Field(alias="GameID", validation_alias=AliasChoices("GameID", "GameId", "ScoreId"))
    pregame: List[GameOdds] = Field(default_factory=list, alias="PregameOdds")
    live: Optional[List[GameOdds]] = Field(default=None, alias="LiveOdds")


class Outcome(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    game_id: int = Field(alias="GameID")
    winner: Optional[str] = Field(default=None, alias="Winner")
    home_score: Optional[int] = Field(default=None, alias="HomeScore")
    away_score: Optional[int] = Field(default=None, alias="AwayScore")
    finalized_at: Optional[datetime] = Field(default=None, alias="Updated")

    @field_validator("finalized_at", mode="before")
    @classmethod
    def _coerce_finalized_at(cls, v: str | datetime | None) -> Optional[datetime]:
        return _parse_dt(v)


