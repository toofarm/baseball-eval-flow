"""Tests for schedule extract: parse_schedule_games.

We now call the MLB Stats API /schedule endpoint directly (the MLB-StatsAPI
library was dropped), so these guard the field mapping and the winner
derivation that downstream load_staging_schedule depends on.
"""

from src.extract.schedule import parse_schedule_games


def _raw(games, date="2026-05-31"):
    """Wrap game dicts in the API's {dates: [{date, games}]} envelope."""
    return {"totalItems": len(games), "dates": [{"date": date, "games": games}]}


def _game(**overrides):
    """Minimal /schedule game with the fields parse_schedule_games reads."""
    base = {
        "gamePk": 824832,
        "gameType": "R",
        "gameDate": "2026-05-31T16:15:00Z",
        "doubleHeader": "N",
        "gameNumber": 1,
        "status": {"detailedState": "Final"},
        "venue": {"id": 2, "name": "Oriole Park at Camden Yards"},
        "teams": {
            "home": {"team": {"id": 110, "name": "Baltimore Orioles"}, "score": 9, "isWinner": True},
            "away": {"team": {"id": 141, "name": "Toronto Blue Jays"}, "score": 5, "isWinner": False},
        },
    }
    base.update(overrides)
    return base


def test_maps_core_fields():
    [g] = parse_schedule_games(_raw([_game()]))
    assert g["game_id"] == 824832
    assert g["game_date"] == "2026-05-31"  # comes from the date wrapper, not gameDate
    assert g["game_type"] == "R"
    assert g["venue_id"] == 2
    assert g["home_id"] == 110
    assert g["away_id"] == 141
    assert g["home_name"] == "Baltimore Orioles"
    assert g["away_name"] == "Toronto Blue Jays"


def test_winner_resolved_for_final_game():
    [g] = parse_schedule_games(_raw([_game()]))
    # load_staging_schedule derives winning_team_id by matching this name to home/away.
    assert g["winning_team"] == "Baltimore Orioles"
    assert g["losing_team"] == "Toronto Blue Jays"


def test_no_winner_when_not_final():
    raw = _raw([_game(status={"detailedState": "In Progress"})])
    [g] = parse_schedule_games(raw)
    assert "winning_team" not in g  # avoids crediting a winner mid-game


def test_doubleheader_yields_two_games():
    raw = _raw([
        _game(gamePk=1, gameNumber=1),
        _game(gamePk=2, gameNumber=2),
    ])
    games = parse_schedule_games(raw)
    assert [g["game_id"] for g in games] == [1, 2]


def test_empty_schedule_returns_empty_list():
    assert parse_schedule_games({"totalItems": 0, "dates": []}) == []
    assert parse_schedule_games({}) == []
