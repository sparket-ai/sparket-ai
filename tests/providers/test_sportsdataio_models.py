from datetime import datetime

from sparket.providers.sportsdataio import (
    Game,
    GameOdds,
    GameOddsSet,
    GameStatus,
    MoneylinePrice,
    SpreadPrice,
    Team,
    TotalPrice,
    SeasonType,
    map_moneyline_quotes,
    map_spread_quotes,
    map_total_quotes,
    normalize_quotes_by_market_ts,
    resolve_moneyline_result,
    resolve_total_result,
    resolve_spread_result,
)


def test_datetime_parsing_z_suffix():
    g = Game.model_validate({
        "GameID": 1,
        "Season": 2025,
        "SeasonType": "Regular",
        "Date": "2025-09-10T20:20:00Z",
        "Status": "Scheduled",
        "HomeTeam": "KC",
        "AwayTeam": "BAL",
    })
    assert isinstance(g.date_time, datetime)


def test_odds_decimal_passthrough_and_normalization():
    odds = GameOdds.model_validate({
        "GameID": 1,
        "Sportsbook": "BookA",
        "Updated": "2025-09-10T12:00:00Z",
        "MoneyLineHome": 1.9,  # already decimal
        "MoneyLineAway": 2.1,
    })
    quotes = list(map_moneyline_quotes(odds, provider_id=1, market_id=10, ts=None))
    assert quotes and all("odds_eu" in q for q in quotes)
    normed = normalize_quotes_by_market_ts(quotes)
    total = sum(q.get("imp_prob_norm", 0.0) for q in normed)
    assert 0.99 <= total <= 1.01


def test_spread_points_policy_and_resolvers():
    # Spread mapping
    odds = GameOdds.model_validate({
        "GameID": 2,
        "Sportsbook": "BookB",
        "Updated": "2025-09-10T12:05:00Z",
        "PointSpread": 3.5,
        "PointSpreadHome": -110,
        "PointSpreadAway": -110,
    })
    quotes = list(map_spread_quotes(odds, provider_id=1, market_id=20, ts=None))
    assert len(quotes) == 2

    # Spread resolver: home receives +3.5; final 20-17 → push if 17+3.5 < 20 (away wins), here 20 > 20.5 so home loses
    res = resolve_spread_result(3.5, points_team_is_home=True, home_score=20, away_score=17)
    assert res == "home"


def test_total_resolver():
    res = resolve_total_result(45.5, home_score=20, away_score=17)
    assert res == "under"


def test_game_normalizes_enum_aliases():
    game = Game.model_validate(
        {
            "GameID": 42,
            "Season": 2025,
            "SeasonType": 1,  # numeric enum value
            "Status": "F/OT",  # provider shorthand
            "Date": "2025-09-14T20:20:00Z",
            "HomeTeam": "KC",
            "AwayTeam": "BUF",
        }
    )
    assert game.season_type == SeasonType.REG
    assert game.status == GameStatus.FINAL


def test_game_falls_back_to_score_id_and_utc_time():
    game = Game.model_validate(
        {
            "ScoreID": 31415,
            "Season": 2025,
            "SeasonType": "Regular",
            "Status": "Scheduled",
            "Date": "2025-11-10T18:30:00",  # no timezone marker
            "HomeTeam": "DAL",
            "AwayTeam": "PHI",
        }
    )
    assert game.game_id == 31415
    assert game.date_time is not None and game.date_time.tzinfo is not None


def test_game_normalizes_team_score_aliases():
    game = Game.model_validate(
        {
            "GameID": 91,
            "Season": 2026,
            "SeasonType": "Regular",
            "DateTime": "2026-01-10T19:00:00",
            "Status": "Final",
            "HomeTeam": "LAL",
            "AwayTeam": "BOS",
            "HomeTeamScore": 112,
            "AwayTeamScore": 104,
        }
    )
    assert game.home_score == 112
    assert game.away_score == 104


def test_game_normalizes_runs_aliases():
    game = Game.model_validate(
        {
            "GameID": 92,
            "Season": 2025,
            "SeasonType": "Regular",
            "DateTime": "2025-08-02T19:00:00",
            "Status": "Final",
            "HomeTeam": "NYY",
            "AwayTeam": "BOS",
            "HomeTeamRuns": 6,
            "AwayTeamRuns": 4,
        }
    )
    assert game.home_score == 6
    assert game.away_score == 4


def test_game_prefers_team_keys_over_names():
    game = Game.model_validate(
        {
            "GameId": 93,
            "Season": 2026,
            "SeasonType": "Regular",
            "DateTime": "2026-02-14T14:30:00",
            "Status": "Scheduled",
            "HomeTeamName": "BV Borussia 09 Dortmund",
            "AwayTeamName": "1. FSV Mainz 05",
            "HomeTeamKey": "BVB",
            "AwayTeamKey": "MAI",
            "HomeTeamId": 532,
            "AwayTeamId": 528,
        }
    )
    assert game.home_team == "BVB"
    assert game.away_team == "MAI"


def test_game_odds_set_normalizes_score_aliases():
    odds_set = GameOddsSet.model_validate(
        {
            "GameId": 19098,
            "Status": "Final",
            "HomeTeamScore": 9,
            "AwayTeamScore": 20,
            "PregameOdds": [],
        }
    )
    assert odds_set.home_score == 9
    assert odds_set.away_score == 20
    assert odds_set.status == GameStatus.FINAL


def test_soccer_ternary_moneyline_with_draw():
    """Soccer markets support ternary outcomes (home/away/draw)."""
    odds = GameOdds.model_validate({
        "GameID": 999,
        "Sportsbook": "SoccerBook",
        "Updated": "2025-11-25T15:00:00Z",
        "MoneyLineHome": 150,   # +150 American
        "MoneyLineAway": 180,   # +180 American
        "MoneyLineDraw": 220,   # +220 American (draw)
    })
    quotes = list(map_moneyline_quotes(odds, provider_id=1, market_id=100, ts=None))
    # Should have 3 quotes: home, away, draw
    assert len(quotes) == 3
    sides = {q["side"] for q in quotes}
    assert sides == {"home", "away", "draw"}
    # All should have valid decimal odds
    for q in quotes:
        assert q["odds_eu"] is not None and q["odds_eu"] > 1.0
    # Normalize and check probabilities sum to ~1
    normed = normalize_quotes_by_market_ts(quotes)
    total = sum(q.get("imp_prob_norm", 0.0) for q in normed)
    assert 0.99 <= total <= 1.01


def test_moneyline_resolver_handles_draw():
    """Moneyline resolver correctly identifies draw outcome."""
    assert resolve_moneyline_result("MAN", "LIV", "draw") == "draw"
    assert resolve_moneyline_result("MAN", "LIV", "Draw") == "draw"
    assert resolve_moneyline_result("MAN", "LIV", "home") == "home"
    assert resolve_moneyline_result("MAN", "LIV", "MAN") == "home"
    assert resolve_moneyline_result("MAN", "LIV", "LIV") == "away"


