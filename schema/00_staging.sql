-- Staging tables for raw MLB data (loaded by Airflow before dbt transform)
-- Run before 01_dims.sql. No FK constraints.

CREATE TABLE IF NOT EXISTS staging_schedule (
    game_pk        BIGINT NOT NULL PRIMARY KEY,
    game_date      VARCHAR(32) NOT NULL,
    game_type      VARCHAR(16) NOT NULL,
    venue_id       INTEGER NOT NULL,
    home_team_id   INTEGER NOT NULL,
    away_team_id   INTEGER NOT NULL,
    home_name      VARCHAR(255),
    away_name      VARCHAR(255),
    winning_team_id INTEGER,
    load_date      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_player_stats (
    game_pk        BIGINT NOT NULL,
    player_id      INTEGER NOT NULL,
    team_id        INTEGER NOT NULL,
    full_name      VARCHAR(255) NOT NULL,
    position_type  VARCHAR(64),
    position_code  VARCHAR(4),
    position_name  VARCHAR(64),
    batting        JSONB,
    pitching       JSONB,
    fielding       JSONB,
    load_date      TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_pk, player_id)
);

CREATE INDEX IF NOT EXISTS idx_staging_player_stats_game_pk ON staging_player_stats (game_pk);
