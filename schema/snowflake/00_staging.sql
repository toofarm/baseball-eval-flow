-- Snowflake DDL: staging tables written by Airflow DAGs (src/load/staging.py).
-- These tables are NOT owned by dbt; they must exist before any DAG run.
-- Apply once per Snowflake environment (e.g. via Snowsight worksheet or SnowSQL).
-- Idempotent: CREATE TABLE IF NOT EXISTS is a no-op on existing tables.
--
-- Notes on Snowflake dialect vs the Postgres equivalent in ../00_staging.sql:
--   * JSONB             -> VARIANT (loader uses PARSE_JSON to populate)
--   * TIMESTAMP WITH TZ -> TIMESTAMP_TZ
--   * BIGINT/INTEGER    -> NUMBER(38,0); INTEGER alias kept for readability
--   * CREATE INDEX      -> dropped; Snowflake has no user-defined indexes
--   * PRIMARY KEY       -> declared but NOT enforced by Snowflake (informational)

CREATE TABLE IF NOT EXISTS staging_schedule (
    game_pk         INTEGER       NOT NULL,
    game_date       VARCHAR(32)   NOT NULL,
    game_type       VARCHAR(16)   NOT NULL,
    venue_id        INTEGER       NOT NULL,
    home_team_id    INTEGER       NOT NULL,
    away_team_id    INTEGER       NOT NULL,
    home_name       VARCHAR(255),
    away_name       VARCHAR(255),
    winning_team_id INTEGER,
    load_date       TIMESTAMP_TZ  DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (game_pk)
);

CREATE TABLE IF NOT EXISTS staging_player_stats (
    game_pk        INTEGER      NOT NULL,
    player_id      INTEGER      NOT NULL,
    team_id        INTEGER      NOT NULL,
    full_name      VARCHAR(255) NOT NULL,
    position_type  VARCHAR(64),
    position_code  VARCHAR(4),
    position_name  VARCHAR(64),
    batting        VARIANT,
    pitching       VARIANT,
    fielding       VARIANT,
    load_date      TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (game_pk, player_id)
);
