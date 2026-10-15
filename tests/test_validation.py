"""Unit tests for src.transform.validation (validate_game_load_count, validate_games_in_db)."""

from unittest.mock import MagicMock

import pytest

from src.transform.validation import validate_game_load_count, validate_games_in_db


def test_validate_game_load_count_match() -> None:
    validate_game_load_count(5, {"games": 5, "fact_rows": 100})
    validate_game_load_count(0, {"games": 0})


def test_validate_game_load_count_mismatch() -> None:
    with pytest.raises(ValueError, match="schedule has 5 games, load reported 3"):
        validate_game_load_count(5, {"games": 3})
    with pytest.raises(ValueError, match="schedule has 0 games, load reported 1"):
        validate_game_load_count(0, {"games": 1})


def test_validate_game_load_count_missing_games_key() -> None:
    with pytest.raises(ValueError, match="must have key 'games'"):
        validate_game_load_count(5, {"fact_rows": 10})


def test_validate_game_load_count_invalid_result_type() -> None:
    with pytest.raises(ValueError, match="load_result must be a dict"):
        validate_game_load_count(5, [1, 2, 3])
    with pytest.raises(ValueError, match="load_result must be a dict"):
        validate_game_load_count(5, None)


def test_validate_game_load_count_invalid_games_value() -> None:
    with pytest.raises(ValueError, match="non-negative int"):
        validate_game_load_count(5, {"games": -1})
    with pytest.raises(ValueError, match="non-negative int"):
        validate_game_load_count(5, {"games": "5"})


def _conn_returning(rows):
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = rows
    return conn, cur


def test_validate_games_in_db_all_present() -> None:
    conn, cur = _conn_returning([(2,), (1,)])
    validate_games_in_db(conn, [1, 2, 2])
    sql, params = cur.execute.call_args.args
    assert params == [1, 2]
    assert sql.count("%s") == 2


def test_validate_games_in_db_reports_missing() -> None:
    conn, _ = _conn_returning([(1,)])
    with pytest.raises(ValueError, match=r"\[2, 3\]"):
        validate_games_in_db(conn, [1, 2, 3])


def test_validate_games_in_db_empty_skips_query() -> None:
    conn, cur = _conn_returning([])
    validate_games_in_db(conn, [])
    cur.execute.assert_not_called()
