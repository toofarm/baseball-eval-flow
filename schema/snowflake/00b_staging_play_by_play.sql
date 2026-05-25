-- Snowflake staging table for raw MLB play-by-play (Airflow-owned, not dbt).
-- Mirrors ../00b_staging_play_by_play.sql. See ../snowflake/00_staging.sql header
-- for dialect notes (VARIANT for JSONB, TIMESTAMP_TZ, no indexes).

CREATE TABLE IF NOT EXISTS staging_play_by_play (
    game_pk    INTEGER      NOT NULL,
    all_plays  VARIANT      NOT NULL,
    load_date  TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (game_pk)
);
