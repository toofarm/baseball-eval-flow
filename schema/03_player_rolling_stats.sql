-- Player rolling stats: one row per (player_id, as_of_date, window_days)
-- window_days in (7, 30). Populated by a separate process from fact_game_state (TBD).
-- Run after 01_dims.sql (references dim_player).

CREATE TABLE IF NOT EXISTS player_rolling_stats (
    player_id     INTEGER NOT NULL REFERENCES dim_player(player_id),
    as_of_date    DATE NOT NULL,
    window_days   SMALLINT NOT NULL CHECK (window_days IN (7, 30)),
    -- Batting aggregates (sums over window)
    bat_games_played     INTEGER,
    bat_plate_appearances INTEGER,
    bat_at_bats         INTEGER,
    bat_runs            INTEGER,
    bat_hits            INTEGER,
    bat_doubles         INTEGER,
    bat_triples         INTEGER,
    bat_home_runs       INTEGER,
    bat_rbi             INTEGER,
    bat_strike_outs     INTEGER,
    bat_base_on_balls   INTEGER,
    bat_stolen_bases    INTEGER,
    bat_caught_stealing INTEGER,
    -- Batting rates (computed from sums or stored averages)
    bat_avg             NUMERIC(5, 4),
    bat_ops             NUMERIC(5, 4),
    bat_woba            NUMERIC(5, 4),
    bat_wrc_plus        NUMERIC(6, 2),
    -- Pitching aggregates
    pit_games_played    INTEGER,
    pit_innings_pitched NUMERIC(8, 2),
    pit_wins            INTEGER,
    pit_losses          INTEGER,
    pit_saves           INTEGER,
    pit_hits            INTEGER,
    pit_earned_runs     INTEGER,
    pit_strike_outs     INTEGER,
    pit_base_on_balls   INTEGER,
    pit_era             NUMERIC(5, 2),
    pit_fip             NUMERIC(5, 2),
    pit_whip            NUMERIC(5, 2),
    -- Fielding aggregates
    fld_assists         INTEGER,
    fld_put_outs        INTEGER,
    fld_errors          INTEGER,
    fld_chances         INTEGER,
    PRIMARY KEY (player_id, as_of_date, window_days)
);

CREATE INDEX IF NOT EXISTS idx_player_rolling_stats_as_of_date ON player_rolling_stats (as_of_date);
CREATE INDEX IF NOT EXISTS idx_player_rolling_stats_player_as_of ON player_rolling_stats (player_id, as_of_date);
