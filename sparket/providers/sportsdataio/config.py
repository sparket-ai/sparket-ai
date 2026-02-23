from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


class LeagueCode(str, Enum):
    # Core US sports (typically included in base SDIO subscription)
    NFL = "nfl"
    NBA = "nba"
    MLB = "mlb"
    NHL = "nhl"
    # Soccer competitions (require separate SDIO soccer subscription)
    EPL = "epl"          # English Premier League
    UCL = "ucl"          # UEFA Champions League
    UEL = "uel"          # UEFA Europa League
    LALIGA = "esp"       # La Liga (Spain)
    BUNDESLIGA = "deb"   # Bundesliga (Germany)
    SERIEA = "itsa"      # Serie A (Italy)
    LIGUE1 = "frl1"      # Ligue 1 (France)
    MLS = "mls"          # Major League Soccer (US)
    EREDIVISIE = "nle"   # Eredivisie (Netherlands)


class LeagueConfig(BaseModel):
    code: LeagueCode
    league_code: str = Field(default_factory=str)
    sport_code: str
    schedule_url: str
    odds_url: str
    delta_url: Optional[str] = None
    teams_url: Optional[str] = None
    schedule_refresh_minutes: int = 60
    odds_refresh_minutes: int = 15
    hot_odds_refresh_minutes: int = 5
    delta_minutes: int = 10
    hot_delta_minutes: int = 2
    track_days_ahead: int = 7
    schedule_mode: str = Field(default="date")
    season_format: Optional[str] = None
    season_type: Optional[str] = None
    season_types: Optional[List[str]] = None
    season_year_offset: int = 0
    off_season_months: Optional[List[int]] = None
    transition_months: Optional[List[int]] = None

    @model_validator(mode="after")
    def _validate_intervals(self) -> "LeagueConfig":
        if not self.league_code:
            self.league_code = self.code.value
        for field_name in (
            "schedule_refresh_minutes",
            "odds_refresh_minutes",
            "hot_odds_refresh_minutes",
            "delta_minutes",
            "hot_delta_minutes",
            "track_days_ahead",
        ):
            value = getattr(self, field_name)
            if value <= 0:
                raise ValueError(f"{field_name} must be > 0")
        if self.schedule_mode not in ("date", "season"):
            raise ValueError("schedule_mode must be 'date' or 'season'")
        if self.schedule_mode == "season" and not self.season_format:
            raise ValueError("season_format required when schedule_mode='season'")
        return self


class SportsDataIOConfig(BaseModel):
    provider_code: str = "SDIO"
    leagues: List[LeagueConfig] = Field(default_factory=list)

    def league_by_code(self, code: LeagueCode | str) -> Optional[LeagueConfig]:
        if isinstance(code, LeagueCode):
            normalized = code
        else:
            normalized = LeagueCode(code)
        for league in self.leagues:
            if league.code == normalized:
                return league
        return None

    def by_league_id(self, league_id: int) -> Optional[LeagueConfig]:
        for league in self.leagues:
            if league.league_id == league_id:
                return league
        return None


def _soccer_league_config(
    code: LeagueCode, 
    slug: str, 
    *, 
    season_year_offset: int = 1,  # European leagues use ending year
) -> LeagueConfig:
    """Helper to build a soccer league config with standard settings.
    
    Args:
        code: The league code enum.
        slug: SDIO competition slug (e.g., "EPL", "ESP").
        season_year_offset: Year offset for season. European leagues (Aug-May) 
            use 1 (ending year), MLS uses 0 (current year).
    """
    base = "https://api.sportsdata.io/v4"
    return LeagueConfig(
        code=code,
        league_code=code.value,
        sport_code="soccer",
        schedule_url=f"{base}/soccer/scores/json/SchedulesBasic/{slug}/{{SEASON}}",
        odds_url=f"{base}/soccer/odds/json/GameOddsLineMovement/{slug}/{{GAMEID}}",
        delta_url=None,
        teams_url=f"{base}/soccer/scores/json/Teams/{slug}",
        schedule_refresh_minutes=120,
        odds_refresh_minutes=20,
        hot_odds_refresh_minutes=6,
        track_days_ahead=10,
        schedule_mode="season",
        season_format="{year}",
        season_year_offset=season_year_offset,
    )


def build_default_config() -> SportsDataIOConfig:
    """Build default SDIO config with all supported leagues."""
    base = "https://api.sportsdata.io/v3"
    leagues = [
        LeagueConfig(
            code=LeagueCode.NFL,
            league_code="nfl",
            sport_code="football",
            schedule_url=f"{base}/nfl/scores/json/SchedulesBasic/{{SEASON}}",
            odds_url=f"{base}/nfl/odds/json/GameOddsLineMovement/{{GAMEID}}",
            delta_url=None,
            teams_url=f"{base}/nfl/scores/json/Teams",
            schedule_mode="season",
            season_format="{year}{season_type}",
            season_types=["PRE", "REG", "POST"],
            season_year_offset=-1,
            off_season_months=[4, 5, 6, 7],
            transition_months=[2, 3, 8, 9],
        ),
        LeagueConfig(
            code=LeagueCode.NBA,
            league_code="nba",
            sport_code="basketball",
            schedule_url=f"{base}/nba/scores/json/Games/{{SEASON}}",
            odds_url=f"{base}/nba/odds/json/GameOddsLineMovement/{{GAMEID}}",
            delta_url=None,
            teams_url=f"{base}/nba/scores/json/Teams",
            schedule_refresh_minutes=30,
            odds_refresh_minutes=10,
            hot_odds_refresh_minutes=3,
            track_days_ahead=3,
            schedule_mode="season",
            season_format="{year}",
            season_year_offset=1,
            off_season_months=[7, 8, 9],
            transition_months=[6, 10],
        ),
        LeagueConfig(
            code=LeagueCode.MLB,
            league_code="mlb",
            sport_code="baseball",
            schedule_url=f"{base}/mlb/scores/json/Games/{{SEASON}}",
            odds_url=f"{base}/mlb/odds/json/GameOddsLineMovement/{{GAMEID}}",
            delta_url=None,
            teams_url=f"{base}/mlb/scores/json/Teams",
            schedule_refresh_minutes=45,
            odds_refresh_minutes=12,
            hot_odds_refresh_minutes=4,
            track_days_ahead=3,
            schedule_mode="season",
            season_format="{year}",
            off_season_months=[11, 12, 1, 2],
            transition_months=[3, 10],
        ),
        LeagueConfig(
            code=LeagueCode.NHL,
            league_code="nhl",
            sport_code="hockey",
            schedule_url=f"{base}/nhl/scores/json/Games/{{SEASON}}",
            odds_url=f"{base}/nhl/odds/json/GameOddsLineMovement/{{GAMEID}}",
            delta_url=None,
            teams_url=f"{base}/nhl/scores/json/Teams",
            schedule_refresh_minutes=45,
            odds_refresh_minutes=10,
            hot_odds_refresh_minutes=3,
            track_days_ahead=4,
            schedule_mode="season",
            season_format="{year}",
            season_year_offset=1,
            off_season_months=[7, 8, 9],
            transition_months=[6, 10],
        ),
    ]
    
    # Soccer leagues - European (Aug-May) use ending year, MLS (March-Dec) uses current year
    european_leagues = [
        (LeagueCode.EPL, "EPL"),
        (LeagueCode.UCL, "UCL"),
        (LeagueCode.LALIGA, "ESP"),
        (LeagueCode.BUNDESLIGA, "DEB"),
        (LeagueCode.SERIEA, "ITSA"),
        (LeagueCode.LIGUE1, "FRL1"),
    ]
    for code, slug in european_leagues:
        leagues.append(_soccer_league_config(code, slug, season_year_offset=1))
    
    # MLS runs within calendar year (March-December)
    leagues.append(_soccer_league_config(LeagueCode.MLS, "MLS", season_year_offset=0))
    
    return SportsDataIOConfig(leagues=leagues)


__all__ = [
    "LeagueCode",
    "LeagueConfig",
    "SportsDataIOConfig",
    "build_default_config",
]

