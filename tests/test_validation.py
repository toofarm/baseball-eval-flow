"""Unit tests for src.transform.validation (validate_game_load_count)."""

import pytest

from src.transform.validation import validate_game_load_count


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
