"""Unit tests for src.load.predictions."""

from unittest.mock import MagicMock

import pytest

from src.load.predictions import load_predictions


def test_load_predictions_empty() -> None:
    mock_conn = MagicMock()
    assert load_predictions(mock_conn, []) == 0
    mock_conn.cursor.assert_not_called()


def test_load_predictions_executemany_called() -> None:
    mock_conn = MagicMock()
    cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
    rows = [
        {
            "game_pk": 100,
            "player_id": 1,
            "as_of_date": "2024-06-01",
            "pred_bat_woba": 0.35,
            "pred_pit_fip": None,
            "model_version_bat": "2024-06-01T06:00:00",
            "model_version_pit": None,
        },
    ]
    n = load_predictions(mock_conn, rows)
    assert n == 1
    cursor.executemany.assert_called_once()
    args = cursor.executemany.call_args[0]
    assert len(args) == 2
    assert args[1] == [(100, 1, "2024-06-01", 0.35, None, "2024-06-01T06:00:00", None)]
