"""Tests for src.extract.streaming_play_by_play.fetch_and_load_play_by_play_batched.

Mirrors test_fetch_and_load_player_stats_batched: HTTP is mocked, we verify
the batching contract (one commit per batch) and the loader is called with
(game_pk, response) tuples.
"""

from typing import List, cast
from unittest.mock import MagicMock, patch

from src.extract.streaming_play_by_play import fetch_and_load_play_by_play_batched
from src.mlb_types import ScheduleGame


@patch("src.extract.streaming_play_by_play.load_staging_play_by_play")
@patch("src.extract.streaming_play_by_play.fetch_play_by_play")
def test_fetch_and_load_play_by_play_batched_single_batch(mock_fetch, mock_load):
    mock_fetch.return_value = {"allPlays": [{"atBatIndex": 0}]}
    mock_load.return_value = 1
    conn = MagicMock()
    games = cast(
        List[ScheduleGame],
        [{"game_id": "12345", "game_date": "2024-06-01"}],
    )

    n = fetch_and_load_play_by_play_batched(conn, games)

    assert n == 1
    assert conn.commit.call_count == 1
    args, _ = mock_load.call_args
    assert args[0] is conn
    assert args[1] == [(12345, {"allPlays": [{"atBatIndex": 0}]})]


@patch("src.extract.streaming_play_by_play.load_staging_play_by_play")
@patch("src.extract.streaming_play_by_play.fetch_play_by_play")
def test_fetch_and_load_commits_per_batch(mock_fetch, mock_load):
    """With batch_size=2 and 5 games, expect 3 commits."""
    mock_fetch.return_value = {"allPlays": [{"atBatIndex": 0}]}
    mock_load.return_value = 2
    conn = MagicMock()
    games = cast(
        List[ScheduleGame],
        [{"game_id": str(i), "game_date": "2024-06-01"} for i in range(1, 6)],
    )

    fetch_and_load_play_by_play_batched(conn, games, batch_size=2)

    assert conn.commit.call_count == 3
    assert mock_fetch.call_count == 5


@patch("src.extract.streaming_play_by_play.load_staging_play_by_play")
@patch("src.extract.streaming_play_by_play.fetch_play_by_play")
def test_no_games_no_calls(mock_fetch, mock_load):
    conn = MagicMock()
    n = fetch_and_load_play_by_play_batched(conn, [])
    assert n == 0
    mock_fetch.assert_not_called()
    mock_load.assert_not_called()
    conn.commit.assert_not_called()
