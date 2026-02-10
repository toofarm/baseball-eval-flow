-- Dimension tables for MLB star schema
-- Run against PostgreSQL; dimensions before fact (fact references dims).

-- One row per player (MLB person.id is stable)
CREATE TABLE IF NOT EXISTS dim_player (
    player_id     INTEGER PRIMARY KEY,
    full_name     VARCHAR(255) NOT NULL,
    boxscore_name VARCHAR(64),
    created_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- One row per team
CREATE TABLE IF NOT EXISTS dim_team (
    team_id       INTEGER PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    abbreviation  VARCHAR(16)
);

-- One row per game
CREATE TABLE IF NOT EXISTS dim_game (
    game_pk        BIGINT PRIMARY KEY,
    game_date      DATE NOT NULL,
    season         INTEGER NOT NULL,
    game_type      VARCHAR(16) NOT NULL,
    venue_id       INTEGER NOT NULL,
    home_team_id   INTEGER NOT NULL REFERENCES dim_team(team_id),
    away_team_id   INTEGER NOT NULL REFERENCES dim_team(team_id),
    winning_team   VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_dim_game_game_date ON dim_game (game_date);
CREATE INDEX IF NOT EXISTS idx_dim_game_season ON dim_game (season);

-- Optional: calendar dimension for rolling windows and reporting
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    DATE PRIMARY KEY,
    season      INTEGER,
    day_of_week INTEGER
);
