"""Unit tests for src.ml.players."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from src.ml.players import get_players_for_scheduled_games, ScheduledGame


def test_get_players_for_scheduled_games_empty() -> None:
    result = get_players_for_scheduled_games(MagicMock(), [], date(2024, 6, 1))
    assert result == []


def test_get_players_for_scheduled_games_returns_tuples() -> None:
    mock_conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = [(101,), (102,)]
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
    games: list[ScheduledGame] = [
        {"game_pk": 12345, "home_team_id": 1, "away_team_id": 2},
    ]
    result = get_players_for_scheduled_games(mock_conn, games, date(2024, 6, 1))
    assert result == [(12345, 101), (12345, 102)]
    assert cursor.execute.called
