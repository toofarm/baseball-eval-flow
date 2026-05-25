"""Tests for src.load.staging.load_staging_play_by_play."""

import json
from unittest.mock import MagicMock

from src.load.staging import load_staging_play_by_play


def test_empty_input_returns_zero_and_skips_db():
    conn = MagicMock()
    n = load_staging_play_by_play(conn, [])
    assert n == 0
    conn.cursor.assert_not_called()


def test_skips_games_with_empty_or_null_pbp():
    conn = MagicMock()
    n = load_staging_play_by_play(
        conn,
        [
            (1, None),
            (2, {"allPlays": []}),
            (3, {"allPlays": [{"atBatIndex": 0}]}),
        ],
    )
    # Only game_pk=3 should be loaded.
    assert n == 1
    cur = conn.cursor.return_value.__enter__.return_value
    args, _ = cur.executemany.call_args
    rows = args[1]
    assert len(rows) == 1
    assert rows[0][0] == 3


def test_serializes_all_plays_as_json():
    conn = MagicMock()
    plays = [{"atBatIndex": 0, "playEvents": [{"isPitch": True}]}]
    load_staging_play_by_play(conn, [(42, {"allPlays": plays})])
    cur = conn.cursor.return_value.__enter__.return_value
    args, _ = cur.executemany.call_args
    rows = args[1]
    assert rows[0][0] == 42
    # Round-trip the JSON string to verify it preserves the structure.
    assert json.loads(rows[0][1]) == plays
