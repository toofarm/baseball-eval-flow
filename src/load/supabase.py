"""
Reverse-ETL loader: copy dbt-built consumer views from Snowflake into a
Supabase Postgres ``analytics`` schema for the web app to read.

Pattern:
    * One ``TableSpec`` per destination, defining the Snowflake source query,
      the destination column list + types, and the load strategy.
    * ``offload_table`` runs CREATE SCHEMA / CREATE TABLE IF NOT EXISTS on
      first use, then either TRUNCATE+INSERT (full_refresh) or
      INSERT ... ON CONFLICT DO UPDATE (upsert) for the rows.
    * Callers (DAG tasks) get the Snowflake conn from ``get_offload_hook``
      and the Supabase conn from ``get_supabase_hook``, then commit/close.

The Airflow connection id for Supabase is selected via the
``SUPABASE_CONN_ID`` env var, defaulting to ``supabase-analytics``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Sequence

from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]

DEFAULT_CONN_ID = "supabase-analytics"
ENV_VAR = "SUPABASE_CONN_ID"
ANALYTICS_SCHEMA = "analytics"


def get_supabase_conn_id() -> str:
    """Return the Airflow connection id used for Supabase reverse-ETL writes."""
    return os.environ.get(ENV_VAR, DEFAULT_CONN_ID)


def get_supabase_hook(conn_id: Optional[str] = None) -> PostgresHook:
    """Return a PostgresHook for the configured Supabase connection."""
    return PostgresHook(postgres_conn_id=conn_id or get_supabase_conn_id())


@dataclass(frozen=True)
class TableSpec:
    """Describes one Snowflake → Supabase table offload.

    Attributes:
        name: Destination table name (will live in the analytics schema).
        source_query: SELECT statement against Snowflake; column order must
            match ``columns``.
        columns: List of (column_name, postgres_ddl_type) tuples. Used to
            generate CREATE TABLE and the INSERT column list, in order.
        strategy: ``full_refresh`` (TRUNCATE + INSERT) or ``upsert``
            (INSERT ... ON CONFLICT DO UPDATE).
        conflict_columns: Primary-key-like columns for upsert conflict
            resolution. Ignored for full_refresh.
    """

    name: str
    source_query: str
    columns: list[tuple[str, str]]
    strategy: Literal["full_refresh", "upsert"]
    conflict_columns: list[str] = field(default_factory=list)

    def create_ddl(self, schema: str = ANALYTICS_SCHEMA) -> str:
        cols_sql = ",\n    ".join(
            f"{name} {ddl_type}" for name, ddl_type in self.columns
        )
        pk_clause = ""
        if self.conflict_columns:
            pk_clause = (
                f",\n    PRIMARY KEY ({', '.join(self.conflict_columns)})"
            )
        return (
            f"CREATE TABLE IF NOT EXISTS {schema}.{self.name} (\n    "
            f"{cols_sql}{pk_clause}\n)"
        )

    def column_names(self) -> list[str]:
        return [name for name, _ in self.columns]


# ---------------------------------------------------------------------------
# Table specifications
# ---------------------------------------------------------------------------

PITCHER_ARSENAL = TableSpec(
    name="pitcher_arsenal",
    source_query="""
        SELECT
            pitcher_id, pitcher_name, season, pitch_type_code,
            pitch_type_name, pitch_family, n_pitches, usage_pct,
            avg_start_speed, avg_spin_rate,
            avg_break_vertical_induced, avg_break_horizontal,
            pct_swinging_strike, pct_called_strike, pct_in_play, pct_home_run
        FROM app_pitcher_arsenal
    """,
    columns=[
        ("pitcher_id",                 "INTEGER NOT NULL"),
        ("pitcher_name",               "TEXT"),
        ("season",                     "INTEGER NOT NULL"),
        ("pitch_type_code",            "VARCHAR(4) NOT NULL"),
        ("pitch_type_name",            "TEXT"),
        ("pitch_family",               "VARCHAR(16)"),
        ("n_pitches",                  "INTEGER NOT NULL"),
        ("usage_pct",                  "NUMERIC(5, 2)"),
        ("avg_start_speed",            "NUMERIC(5, 2)"),
        ("avg_spin_rate",              "NUMERIC(7, 1)"),
        ("avg_break_vertical_induced", "NUMERIC(5, 2)"),
        ("avg_break_horizontal",       "NUMERIC(5, 2)"),
        ("pct_swinging_strike",        "NUMERIC(5, 2)"),
        ("pct_called_strike",          "NUMERIC(5, 2)"),
        ("pct_in_play",                "NUMERIC(5, 2)"),
        ("pct_home_run",               "NUMERIC(7, 4)"),
    ],
    strategy="full_refresh",
)


LEAGUE_PITCH_SUMMARY = TableSpec(
    name="league_pitch_summary",
    source_query="""
        SELECT
            season, pitch_type_code, pitch_type_name, pitch_family,
            n_pitches, n_pitchers, pct_of_league_mix,
            avg_start_speed, avg_spin_rate,
            avg_break_vertical_induced, avg_break_horizontal,
            pct_swinging_strike, pct_called_strike, pct_in_play, pct_home_run
        FROM app_league_pitch_summary
    """,
    columns=[
        ("season",                     "INTEGER NOT NULL"),
        ("pitch_type_code",            "VARCHAR(4) NOT NULL"),
        ("pitch_type_name",            "TEXT"),
        ("pitch_family",               "VARCHAR(16)"),
        ("n_pitches",                  "INTEGER NOT NULL"),
        ("n_pitchers",                 "INTEGER NOT NULL"),
        ("pct_of_league_mix",          "NUMERIC(5, 2)"),
        ("avg_start_speed",            "NUMERIC(5, 2)"),
        ("avg_spin_rate",              "NUMERIC(7, 1)"),
        ("avg_break_vertical_induced", "NUMERIC(5, 2)"),
        ("avg_break_horizontal",       "NUMERIC(5, 2)"),
        ("pct_swinging_strike",        "NUMERIC(5, 2)"),
        ("pct_called_strike",          "NUMERIC(5, 2)"),
        ("pct_in_play",                "NUMERIC(5, 2)"),
        ("pct_home_run",               "NUMERIC(7, 4)"),
    ],
    strategy="full_refresh",
)


PLAYER_ROLLING_STATS = TableSpec(
    name="player_rolling_stats",
    source_query="""
        SELECT
            player_id, player_name, as_of_date, window_days,
            bat_games_played, bat_plate_appearances, bat_at_bats, bat_runs,
            bat_hits, bat_doubles, bat_triples, bat_home_runs, bat_rbi,
            bat_strike_outs, bat_base_on_balls, bat_stolen_bases,
            bat_caught_stealing, bat_avg, bat_ops, bat_woba, bat_wrc_plus,
            pit_games_played, pit_innings_pitched, pit_wins, pit_losses,
            pit_saves, pit_hits, pit_earned_runs, pit_strike_outs,
            pit_base_on_balls, pit_era, pit_fip, pit_whip,
            fld_assists, fld_put_outs, fld_errors, fld_chances
        FROM app_player_rolling_stats
    """,
    columns=[
        ("player_id",             "INTEGER NOT NULL"),
        ("player_name",           "TEXT"),
        ("as_of_date",            "DATE NOT NULL"),
        ("window_days",           "SMALLINT NOT NULL"),
        ("bat_games_played",      "INTEGER"),
        ("bat_plate_appearances", "INTEGER"),
        ("bat_at_bats",           "INTEGER"),
        ("bat_runs",              "INTEGER"),
        ("bat_hits",              "INTEGER"),
        ("bat_doubles",           "INTEGER"),
        ("bat_triples",           "INTEGER"),
        ("bat_home_runs",         "INTEGER"),
        ("bat_rbi",               "INTEGER"),
        ("bat_strike_outs",       "INTEGER"),
        ("bat_base_on_balls",     "INTEGER"),
        ("bat_stolen_bases",      "INTEGER"),
        ("bat_caught_stealing",   "INTEGER"),
        ("bat_avg",               "NUMERIC(5, 4)"),
        ("bat_ops",               "NUMERIC(5, 4)"),
        ("bat_woba",              "NUMERIC(5, 4)"),
        ("bat_wrc_plus",          "NUMERIC(6, 2)"),
        ("pit_games_played",      "INTEGER"),
        ("pit_innings_pitched",   "NUMERIC(8, 2)"),
        ("pit_wins",              "INTEGER"),
        ("pit_losses",            "INTEGER"),
        ("pit_saves",             "INTEGER"),
        ("pit_hits",              "INTEGER"),
        ("pit_earned_runs",       "INTEGER"),
        ("pit_strike_outs",       "INTEGER"),
        ("pit_base_on_balls",     "INTEGER"),
        ("pit_era",               "NUMERIC(5, 2)"),
        ("pit_fip",               "NUMERIC(5, 2)"),
        ("pit_whip",              "NUMERIC(5, 2)"),
        ("fld_assists",           "INTEGER"),
        ("fld_put_outs",          "INTEGER"),
        ("fld_errors",            "INTEGER"),
        ("fld_chances",           "INTEGER"),
    ],
    strategy="full_refresh",
)


PLAYER_PREDICTIONS = TableSpec(
    name="player_predictions",
    source_query="""
        SELECT
            game_pk, player_id, player_name, model_type, as_of_date,
            pred_bat_woba, pred_pit_fip,
            model_version_bat, model_version_pit, created_at
        FROM app_player_predictions
    """,
    columns=[
        ("game_pk",           "BIGINT NOT NULL"),
        ("player_id",         "INTEGER NOT NULL"),
        ("player_name",       "TEXT"),
        ("model_type",        "VARCHAR(32) NOT NULL"),
        ("as_of_date",        "DATE NOT NULL"),
        ("pred_bat_woba",     "NUMERIC(5, 4)"),
        ("pred_pit_fip",      "NUMERIC(5, 2)"),
        ("model_version_bat", "VARCHAR(64)"),
        ("model_version_pit", "VARCHAR(64)"),
        ("created_at",        "TIMESTAMP WITH TIME ZONE"),
    ],
    strategy="upsert",
    conflict_columns=["game_pk", "player_id", "model_type"],
)


# Tables offloaded by mlb_player_stats_pipeline (after dbt builds the marts/views).
STATS_PIPELINE_TABLES: list[TableSpec] = [
    PITCHER_ARSENAL,
    LEAGUE_PITCH_SUMMARY,
    PLAYER_ROLLING_STATS,
]

# Tables offloaded by ml_predictions_pipeline (after dbt builds app_player_predictions).
ML_PIPELINE_TABLES: list[TableSpec] = [
    PLAYER_PREDICTIONS,
]


# ---------------------------------------------------------------------------
# Offload implementation
# ---------------------------------------------------------------------------


def ensure_analytics_schema(sb_conn: Any, schema: str = ANALYTICS_SCHEMA) -> None:
    """CREATE SCHEMA IF NOT EXISTS analytics. Caller commits."""
    with sb_conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def offload_table(
    sf_conn: Any,
    sb_conn: Any,
    spec: TableSpec,
    schema: str = ANALYTICS_SCHEMA,
) -> int:
    """Run one table's Snowflake → Supabase offload.

    Idempotently ensures the destination table exists, then either truncates
    and inserts (full_refresh) or upserts on the spec's conflict columns.
    Caller is responsible for commit/close on both connections.
    """
    # 1. Ensure destination table exists.
    with sb_conn.cursor() as cur:
        cur.execute(spec.create_ddl(schema))

    # 2. Pull rows from Snowflake.
    with sf_conn.cursor() as cur:
        cur.execute(spec.source_query)
        rows: Sequence[tuple] = cur.fetchall()

    cols = spec.column_names()
    qualified = f"{schema}.{spec.name}"

    # 3. Load rows into Supabase.
    with sb_conn.cursor() as cur:
        if spec.strategy == "full_refresh":
            cur.execute(f"TRUNCATE TABLE {qualified}")
            if rows:
                placeholders = ", ".join(["%s"] * len(cols))
                insert_sql = (
                    f"INSERT INTO {qualified} ({', '.join(cols)}) "
                    f"VALUES ({placeholders})"
                )
                cur.executemany(insert_sql, rows)
        elif spec.strategy == "upsert":
            if not rows:
                return 0
            if not spec.conflict_columns:
                raise ValueError(
                    f"TableSpec '{spec.name}' uses upsert but defines no "
                    "conflict_columns"
                )
            placeholders = ", ".join(["%s"] * len(cols))
            update_cols = [c for c in cols if c not in spec.conflict_columns]
            set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
            upsert_sql = (
                f"INSERT INTO {qualified} ({', '.join(cols)}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({', '.join(spec.conflict_columns)}) "
                f"DO UPDATE SET {set_clause}"
            )
            cur.executemany(upsert_sql, rows)
        else:
            raise ValueError(f"Unknown strategy: {spec.strategy!r}")

    return len(rows)


def offload_all(
    sf_conn: Any,
    sb_conn: Any,
    specs: Sequence[TableSpec],
    schema: str = ANALYTICS_SCHEMA,
) -> dict[str, int]:
    """Bootstrap schema then offload each spec in sequence. Returns row counts."""
    ensure_analytics_schema(sb_conn, schema)
    return {spec.name: offload_table(sf_conn, sb_conn, spec, schema) for spec in specs}
