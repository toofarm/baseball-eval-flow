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
    PLAYER_BATTING_PERCENTILES,
    PLAYER_PREDICTIONS,
    SEARCH_ENTITIES,
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


def test_batting_percentiles_spec_is_upsert_with_aligned_columns():
    spec = PLAYER_BATTING_PERCENTILES
    # Upsert keyed on the (player_id, season) grain so reloading a season
    # updates a player's row in place instead of inserting a duplicate.
    assert spec.strategy == "upsert"
    assert spec.conflict_columns == ["player_id", "season"]
    # The SELECT column list and the DDL column list must line up 1:1 so the
    # positional INSERT loads the right values into the right columns.
    selected = [
        tok.strip()
        for tok in spec.source_query.split("SELECT", 1)[1]
        .split("FROM", 1)[0]
        .replace("\n", " ")
        .split(",")
        if tok.strip()
    ]
    assert len(selected) == len(spec.columns)
    assert spec.column_names()[0] == "player_id"
    assert spec.column_names()[-1] == "bat_wrc_plus_pctl"


def test_stats_pipeline_tables_contain_expected_set():
    assert {t.name for t in STATS_PIPELINE_TABLES} == {
        "pitcher_arsenal",
        "league_pitch_summary",
        "league_batting_summary",
        "player_rolling_stats",
        "player_batting_percentiles",
        "search_entities",
    }


# --- post_create_ddl (trigram search) ----------------------------------------


def test_search_entities_post_create_statements_render_schema_and_table():
    stmts = SEARCH_ENTITIES.post_create_statements()
    # pg_trgm extension is enabled (no placeholders, passed through unchanged).
    assert "CREATE EXTENSION IF NOT EXISTS pg_trgm" in stmts
    # Index statements get {schema}/{table} filled in and target gin_trgm_ops.
    trgm = [s for s in stmts if "gin_trgm_ops" in s]
    assert len(trgm) == 2
    for s in trgm:
        assert f"ON {ANALYTICS_SCHEMA}.search_entities" in s
        assert "USING GIN" in s
    # No unrendered placeholders remain.
    assert all("{" not in s for s in stmts)


def test_post_create_statements_empty_for_plain_spec():
    # Specs without post_create_ddl render nothing (back-compat).
    assert PITCHER_ARSENAL.post_create_statements() == []


def test_offload_table_runs_post_create_ddl_after_create_before_load():
    sf = _mock_conn(fetchall_rows=[("team", 147, "Yankees") + (None,) * 4 + ("yankees nyy",)])
    sb = _mock_conn()

    offload_table(sf, sb, SEARCH_ENTITIES)

    sb_cur = _last_cursor(sb)
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    create_idx = next(i for i, s in enumerate(statements) if s.startswith("CREATE TABLE"))
    ext_idx = statements.index("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    trunc_idx = next(i for i, s in enumerate(statements) if s.startswith("TRUNCATE TABLE"))
    # Order: CREATE TABLE -> extension + indexes -> TRUNCATE -> (INSERT via executemany).
    assert create_idx < ext_idx < trunc_idx
    assert any("gin_trgm_ops" in s for s in statements)


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


def test_offload_table_full_refresh_with_conflict_columns_skips_duplicates():
    """full_refresh + conflict_columns: INSERT ... ON CONFLICT DO NOTHING so a
    duplicate key is skipped rather than aborting the load."""
    row = ("team", 147, "Yankees") + (None,) * 4 + ("yankees nyy",)
    sf = _mock_conn(fetchall_rows=[row])
    sb = _mock_conn()

    offload_table(sf, sb, SEARCH_ENTITIES)

    sb_cur = _last_cursor(sb)
    # Still a full_refresh (truncate first), but the INSERT tolerates dupes.
    statements = [c.args[0] for c in sb_cur.execute.call_args_list]
    assert any(s.startswith("TRUNCATE TABLE") for s in statements)
    insert_sql, _ = sb_cur.executemany.call_args.args
    assert "ON CONFLICT (entity_type, uid) DO NOTHING" in insert_sql


def test_search_entities_create_ddl_has_primary_key():
    ddl = SEARCH_ENTITIES.create_ddl()
    assert "PRIMARY KEY (entity_type, uid)" in ddl


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
        "player_batting_percentiles",
        "search_entities",
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
