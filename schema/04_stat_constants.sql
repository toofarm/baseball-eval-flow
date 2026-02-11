-- Fangraphs-style seasonal constants for wOBA and wRC+ (used by rolling stats).
-- Run after 01_dims.sql. No FK to other tables.
-- Populate via Python script from src/transform/constants.json.

CREATE TABLE IF NOT EXISTS dim_stat_constants (
    season     INTEGER PRIMARY KEY,
    woba       NUMERIC NOT NULL,
    woba_scale NUMERIC NOT NULL,
    w_bb       NUMERIC NOT NULL,
    w_hbp      NUMERIC NOT NULL,
    w_1b       NUMERIC NOT NULL,
    w_2b       NUMERIC NOT NULL,
    w_3b       NUMERIC NOT NULL,
    w_hr       NUMERIC NOT NULL,
    r_per_pa   NUMERIC NOT NULL
);
