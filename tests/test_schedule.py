"""Tests for schedule extract: parse_schedule_games.

We now call the MLB Stats API /schedule endpoint directly (the MLB-StatsAPI
library was dropped), so these guard the field mapping and the winner
derivation that downstream load_staging_schedule depends on.
"""

from datetime import date
from unittest.mock import patch

from src.extract.schedule import (
    dedupe_games,
    is_ingestible,
    is_pending,
    parse_schedule_games,
    plan_ingest,
)


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


def test_game_date_prefers_official_date():
    # A suspended game resumed on 06-17 is still officially dated 06-16.
    [g] = parse_schedule_games(_raw([_game(officialDate="2026-06-16")], date="2026-06-17"))
    assert g["game_date"] == "2026-06-16"


def test_maps_coded_state_and_if_necessary():
    [g] = parse_schedule_games(_raw([_game(
        status={"detailedState": "Scheduled", "codedGameState": "S"},
        ifNecessary="Y",
    )]))
    assert g["coded_game_state"] == "S"
    assert g["if_necessary"] == "Y"


# --- status classification -------------------------------------------------

def _sched(pk=1, coded="F", game_date="2026-05-31", if_necessary="N", status="Final"):
    """A parsed ScheduleGame with just the fields the classifiers read."""
    return {
        "game_id": pk,
        "game_date": game_date,
        "coded_game_state": coded,
        "if_necessary": if_necessary,
        "status": status,
    }


def test_only_final_games_are_ingestible():
    assert is_ingestible(_sched(coded="F"))
    # Postponed games report abstractGameState "Final" but have coded state D.
    for coded in ("D", "C", "T", "U", "S", "P", "I"):
        assert not is_ingestible(_sched(coded=coded)), coded


def test_postponed_cancelled_suspended_never_pending():
    for coded in ("F", "D", "C", "T", "U"):
        assert not is_pending(_sched(coded=coded)), coded


def test_live_and_delayed_games_are_pending():
    for coded in ("S", "P", "I"):
        assert is_pending(_sched(coded=coded)), coded


def test_unplayed_if_necessary_game_not_pending():
    assert not is_pending(_sched(coded="S", if_necessary="Y"))
    # ...but one that actually started is waited on like any other game.
    assert is_pending(_sched(coded="I", if_necessary="Y"))


def test_dedupe_prefers_final_listing_of_postponed_game():
    games = [
        _sched(pk=7, coded="D", game_date="2026-05-29", status="Postponed"),
        _sched(pk=7, coded="F", game_date="2026-05-30"),
    ]
    [g] = dedupe_games(games)
    assert g["coded_game_state"] == "F"
    assert g["game_date"] == "2026-05-30"


def test_dedupe_keeps_final_listing_when_later_listing_is_live():
    games = [
        _sched(pk=7, coded="F", game_date="2026-05-29"),
        _sched(pk=7, coded="I", game_date="2026-05-29"),
    ]
    [g] = dedupe_games(games)
    assert g["coded_game_state"] == "F"


# --- plan_ingest -----------------------------------------------------------

def _range_response(*days):
    """Schedule-range envelope: days is a sequence of (date, [game dicts])."""
    return {"dates": [{"date": d, "games": gs} for d, gs in days]}


def _api_game(pk, coded, detailed, official, **extra):
    return _game(
        gamePk=pk,
        officialDate=official,
        status={"detailedState": detailed, "codedGameState": coded},
        **extra,
    )


@patch("src.extract.schedule.fetch_schedule_range")
def test_plan_ingest_window_and_classification(mock_fetch):
    mock_fetch.return_value = _range_response(
        ("2026-05-29", [
            _api_game(1, "F", "Final", "2026-05-29"),
            _api_game(2, "D", "Postponed", "2026-05-29"),
        ]),
        ("2026-05-30", [
            _api_game(3, "F", "Final", "2026-05-30"),
            # Earlier-in-window game still live: not waited on.
            _api_game(4, "I", "In Progress", "2026-05-30"),
        ]),
        ("2026-05-31", [
            _api_game(5, "F", "Final", "2026-05-31"),
            _api_game(6, "I", "Delayed: Rain", "2026-05-31"),
            _api_game(7, "T", "Suspended: Rain", "2026-05-31"),
            _api_game(2, "F", "Final", "2026-05-31"),  # makeup of the postponed game
        ]),
    )
    ready, pending = plan_ingest(date(2026, 5, 31), lookback_days=3)

    mock_fetch.assert_called_once_with(date(2026, 5, 29), date(2026, 5, 31))
    assert sorted(g["game_id"] for g in ready) == [1, 2, 3, 5]
    assert [g["game_id"] for g in pending] == [6]


@patch("src.extract.schedule.fetch_schedule_range")
def test_plan_ingest_resumed_game_not_pending_on_resume_day(mock_fetch):
    # Game suspended on 05-29 and resuming (in progress) on 05-31 keeps its
    # 05-29 official date, so it doesn't hold up the 05-31 run.
    mock_fetch.return_value = _range_response(
        ("2026-05-29", [_api_game(9, "T", "Suspended: Rain", "2026-05-29")]),
        ("2026-05-31", [_api_game(9, "I", "In Progress", "2026-05-29")]),
    )
    ready, pending = plan_ingest(date(2026, 5, 31), lookback_days=3)
    assert ready == []
    assert pending == []


@patch("src.extract.schedule.fetch_schedule_range")
def test_plan_ingest_off_day_is_empty_not_error(mock_fetch):
    mock_fetch.return_value = {"totalItems": 0, "dates": []}
    assert plan_ingest(date(2026, 12, 25), lookback_days=3) == ([], [])
