"""Tests for src.load.supabase reverse-ETL loader.

Both connections are mocked. We verify:
  * CREATE SCHEMA / CREATE TABLE bootstrap happens
  * full_refresh runs TRUNCATE then INSERT (no INSERT when source is empty)
  * upsert runs INSERT ... ON CONFLICT DO UPDATE keyed on conflict_columns
  * Misconfigured specs (unknown strategy, upsert without conflict_columns) raise
"""

from typing import Any, cast
from unittest.mock import MagicMock, call

import pytest

from src.load.supabase import (
    ANALYTICS_SCHEMA,
    PITCHER_ARSENAL,
    PLAYER_PREDICTIONS,
    STATS_PIPELINE_TABLES,
    TableSpec,
    ensure_analytics_schema,
    offload_all,
    offload_table,
)


# --- Fixtures ----------------------------------------------------------------


def _mock_conn(fetchall_rows: list[tuple] | None = None) -> Any:
    """Build a mock DB-API connection whose cursor() context returns a cursor
    with a configurable fetchall() result."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = fetchall_rows or []
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


def _last_cursor(conn: Any) -> Any:
    """Return the (single) cursor mock used in this connection's tests."""
    return conn.cursor.return_value.__enter__.return_value


# --- TableSpec ---------------------------------------------------------------


def test_create_ddl_full_refresh_has_no_pk_clause():
    ddl = PITCHER_ARSENAL.create_ddl()
    assert "CREATE TABLE IF NOT EXISTS analytics.pitcher_arsenal" in ddl
    assert "PRIMARY KEY" not in ddl


def test_create_ddl_upsert_includes_pk_clause():
    ddl = PLAYER_PREDICTIONS.create_ddl()
    assert "CREATE TABLE IF NOT EXISTS analytics.player_predictions" in ddl
    assert "PRIMARY KEY (game_pk, player_id, model_type)" in ddl


def test_column_names_returns_ordered_names():
    names = PITCHER_ARSENAL.column_names()
    # First two and last column should match the documented column order.
    assert names[0] == "pitcher_id"
    assert names[1] == "pitcher_name"
    assert names[-1] == "pct_home_run"


def test_stats_pipeline_tables_contain_expected_set():
    assert {t.name for t in STATS_PIPELINE_TABLES} == {
        "pitcher_arsenal",
        "league_pitch_summary",
        "league_batting_summary",
        "player_rolling_stats",
    }


# --- ensure_analytics_schema -------------------------------------------------


def test_ensure_analytics_schema_runs_create_schema():
    sb = _mock_conn()
    ensure_analytics_schema(sb)
    cur = _last_cursor(sb)
    cur.execute.assert_called_once_with(
        f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}"
    )


# --- offload_table: full_refresh --------------------------------------------


def test_offload_table_full_refresh_truncates_and_inserts():
    sf = _mock_conn(fetchall_rows=[(1, "Cy", 2026, "FF") + (None,) * 12])
    sb = _mock_conn()

    n = offload_table(sf, sb, PITCHER_ARSENAL)

    assert n == 1
    sb_cur = _last_cursor(sb)
    # Expected calls, in order: CREATE TABLE, TRUNCATE, then INSERT via executemany.
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS") for s in statements)
    assert any(s.startswith("TRUNCATE TABLE") for s in statements)
    assert sb_cur.executemany.called
    insert_sql, rows = sb_cur.executemany.call_args.args
    assert insert_sql.startswith(
        f"INSERT INTO {ANALYTICS_SCHEMA}.{PITCHER_ARSENAL.name}"
    )
    assert rows == [(1, "Cy", 2026, "FF") + (None,) * 12]


def test_offload_table_full_refresh_with_empty_source_still_truncates():
    """No source rows: we still create+truncate (so the destination reflects
    the empty source), but we don't run executemany."""
    sf = _mock_conn(fetchall_rows=[])
    sb = _mock_conn()

    n = offload_table(sf, sb, PITCHER_ARSENAL)

    assert n == 0
    sb_cur = _last_cursor(sb)
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    assert any(s.startswith("TRUNCATE TABLE") for s in statements)
    sb_cur.executemany.assert_not_called()


# --- offload_table: upsert ---------------------------------------------------


def test_offload_table_upsert_uses_on_conflict_and_skips_truncate():
    row = (
        123, 456, "Cy Young", "lasso",
        "2026-05-25", 0.350, 3.20, "v1", "v1", None,
    )
    sf = _mock_conn(fetchall_rows=[row])
    sb = _mock_conn()

    n = offload_table(sf, sb, PLAYER_PREDICTIONS)

    assert n == 1
    sb_cur = _last_cursor(sb)
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    # No TRUNCATE for upsert.
    assert not any(s.startswith("TRUNCATE TABLE") for s in statements)
    insert_sql, rows = sb_cur.executemany.call_args.args
    assert "ON CONFLICT (game_pk, player_id, model_type)" in insert_sql
    assert "DO UPDATE SET" in insert_sql
    # Conflict columns must not appear in the SET clause.
    set_clause = insert_sql.split("DO UPDATE SET", 1)[1]
    assert "game_pk = EXCLUDED.game_pk" not in set_clause
    assert "player_id = EXCLUDED.player_id" not in set_clause
    # Non-key cols should appear.
    assert "pred_bat_woba = EXCLUDED.pred_bat_woba" in set_clause
    assert rows == [row]


def test_offload_table_upsert_empty_source_does_nothing_to_destination():
    sf = _mock_conn(fetchall_rows=[])
    sb = _mock_conn()

    n = offload_table(sf, sb, PLAYER_PREDICTIONS)

    assert n == 0
    sb_cur = _last_cursor(sb)
    sb_cur.executemany.assert_not_called()
    # CREATE TABLE still ran (idempotent first-touch).
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS") for s in statements)


# --- offload_table: misconfiguration ----------------------------------------


def test_offload_table_rejects_unknown_strategy():
    bad = TableSpec(
        name="x",
        source_query="SELECT 1",
        columns=[("a", "INTEGER")],
        strategy=cast(Any, "weird"),
    )
    sf = _mock_conn(fetchall_rows=[(1,)])
    sb = _mock_conn()
    with pytest.raises(ValueError, match="Unknown strategy"):
        offload_table(sf, sb, bad)


def test_offload_table_rejects_upsert_without_conflict_columns():
    bad = TableSpec(
        name="x",
        source_query="SELECT 1",
        columns=[("a", "INTEGER")],
        strategy="upsert",
        conflict_columns=[],
    )
    sf = _mock_conn(fetchall_rows=[(1,)])
    sb = _mock_conn()
    with pytest.raises(ValueError, match="conflict_columns"):
        offload_table(sf, sb, bad)


# --- offload_all -------------------------------------------------------------


def test_offload_all_bootstraps_schema_then_runs_each_spec():
    sf = _mock_conn(fetchall_rows=[])  # empty source for all specs
    sb = _mock_conn()

    results = offload_all(sf, sb, STATS_PIPELINE_TABLES)

    assert set(results.keys()) == {
        "pitcher_arsenal",
        "league_pitch_summary",
        "league_batting_summary",
        "player_rolling_stats",
    }
    sb_cur = _last_cursor(sb)
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    # First statement must be the schema bootstrap.
    assert statements[0] == f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}"
    # One CREATE TABLE per spec.
    create_tables = [
        s for s in statements if s.startswith("CREATE TABLE IF NOT EXISTS")
    ]
    assert len(create_tables) == len(STATS_PIPELINE_TABLES)
