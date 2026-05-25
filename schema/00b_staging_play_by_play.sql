-- Staging table for raw MLB play-by-play (loaded by Airflow before dbt transform).
-- One row per game; the entire allPlays array is stored as JSONB so we can re-parse
-- without re-fetching when downstream models evolve. Mirror of staging_player_stats.

CREATE TABLE IF NOT EXISTS staging_play_by_play (
    game_pk    BIGINT NOT NULL PRIMARY KEY,
    all_plays  JSONB NOT NULL,
    load_date  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
