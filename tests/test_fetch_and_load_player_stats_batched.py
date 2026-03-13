import pytest

from src.extract.streaming_boxscore import fetch_and_load_player_stats_batched

from unittest.mock import MagicMock, patch

from typing import cast, List
from dags.mlb_types import ScheduleGame


@patch("src.extract.streaming_boxscore.fetch_boxscore")
def test_fetch_and_load_player_stats_batched(mock_fetch):
    mock_fetch.return_value = {
        "teams": {"home": {"team_id": 1}, "away": {"team_id": 2}}
    }
    conn = MagicMock()
    # schedule_games = cast(List[ScheduleGame], [MagicMock()])
    schedule_games = cast(
        List[ScheduleGame], [{"game_id": "12345", "game_date": "2024-06-01"}]
    )
    fetch_and_load_player_stats_batched(conn, schedule_games)
    assert conn.commit.call_count == 1
