-- Reference DDL for pitch-grain star schema additions.
-- dbt owns the prod build (see dbt/models/marts/dim_pitch_type.sql and fact_pitch.sql);
-- this file documents the shape and lets a fresh Postgres dev DB pass FK checks
-- if you bootstrap without dbt.

-- Small conformed dimension. Seeded from dbt/seeds/dim_pitch_type.csv.
CREATE TABLE IF NOT EXISTS dim_pitch_type (
    pitch_type_code  VARCHAR(4) PRIMARY KEY,
    pitch_type_name  VARCHAR(64) NOT NULL,
    pitch_family     VARCHAR(16) NOT NULL  -- 'fastball' | 'breaking' | 'offspeed' | 'other'
);

-- Fact at pitch grain. One row per pitch within a game.
-- Conforms to dim_game (game_pk), dim_player (pitcher_id, batter_id), dim_pitch_type.
CREATE TABLE IF NOT EXISTS fact_pitch (
    game_pk                 BIGINT NOT NULL REFERENCES dim_game(game_pk),
    at_bat_index            INTEGER NOT NULL,
    pitch_number            INTEGER NOT NULL,
    pitcher_id              INTEGER NOT NULL REFERENCES dim_player(player_id),
    batter_id               INTEGER NOT NULL REFERENCES dim_player(player_id),
    pitch_type_code         VARCHAR(4) REFERENCES dim_pitch_type(pitch_type_code),
    -- Velocity and physics
    start_speed             NUMERIC(5, 2),
    end_speed               NUMERIC(5, 2),
    spin_rate               INTEGER,
    spin_direction          INTEGER,
    -- Break measurements (inches/degrees, see MLB Stats API docs)
    break_angle             NUMERIC(6, 2),
    break_length            NUMERIC(6, 2),
    break_vertical          NUMERIC(6, 2),
    break_vertical_induced  NUMERIC(6, 2),
    break_horizontal        NUMERIC(6, 2),
    -- Plate location
    plate_x                 NUMERIC(6, 3),
    plate_z                 NUMERIC(6, 3),
    zone                    INTEGER,
    -- At-bat context (denormalized to avoid join for common slices)
    inning                  INTEGER NOT NULL,
    half_inning             VARCHAR(8) NOT NULL,  -- 'top' | 'bottom'
    balls_before            INTEGER,
    strikes_before          INTEGER,
    outs_before             INTEGER,
    bat_side                VARCHAR(2),  -- 'L' | 'R' | 'S'
    pitch_hand              VARCHAR(2),  -- 'L' | 'R'
    -- Outcome
    is_strike               BOOLEAN,
    is_ball                 BOOLEAN,
    is_in_play              BOOLEAN,
    call_code               VARCHAR(8),    -- B, S, X, F, ...
    at_bat_event            VARCHAR(64),   -- 'Single', 'Strikeout', etc. (same value on every pitch of the AB)
    PRIMARY KEY (game_pk, at_bat_index, pitch_number)
);

CREATE INDEX IF NOT EXISTS idx_fact_pitch_pitcher_type ON fact_pitch (pitcher_id, pitch_type_code);
CREATE INDEX IF NOT EXISTS idx_fact_pitch_batter_type  ON fact_pitch (batter_id, pitch_type_code);
CREATE INDEX IF NOT EXISTS idx_fact_pitch_game_pk      ON fact_pitch (game_pk);
