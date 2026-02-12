"""Unit tests for src.load.audit (record_load_audit, check_freshness)."""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

from src.load.audit import check_freshness, record_load_audit


def test_record_load_audit() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    record_load_audit(conn, "mlb_player_stats", None)
    cursor.execute.assert_called_once()
    args = cursor.execute.call_args[0]
    assert "INSERT INTO pipeline_load_audit" in args[0]
    assert args[1] == ("mlb_player_stats", None)

    record_load_audit(conn, "ml_predictions", date(2024, 6, 1))
    assert cursor.execute.call_count == 2
    assert cursor.execute.call_args_list[1][0][1] == ("ml_predictions", date(2024, 6, 1))


def test_check_freshness_no_row() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    with pytest.raises(ValueError, match="no recorded load"):
        check_freshness(conn, "mlb_player_stats", max_age_hours=24)


def test_check_freshness_too_old() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    old_ts = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    cursor.fetchone.return_value = (old_ts,)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    with pytest.raises(ValueError, match="older than"):
        check_freshness(conn, "mlb_player_stats", max_age_hours=24)


def test_check_freshness_recent_ok() -> None:
    conn = MagicMock()
    cursor = MagicMock()
    recent = datetime.now(timezone.utc) - timedelta(hours=1)
    cursor.fetchone.return_value = (recent,)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    check_freshness(conn, "mlb_player_stats", max_age_hours=24)


def test_check_freshness_naive_timestamp_assumed_utc() -> None:
    """Naive loaded_at is treated as UTC."""
    conn = MagicMock()
    cursor = MagicMock()
    # Naive datetime 1 hour ago (interpreted as UTC)
    recent_naive = (datetime.now(timezone.utc) - timedelta(hours=1)).replace(tzinfo=None)
    cursor.fetchone.return_value = (recent_naive,)
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=None)

    check_freshness(conn, "mlb_player_stats", max_age_hours=24)
