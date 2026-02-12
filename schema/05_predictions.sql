-- Next-game player performance predictions from ML pipeline.
-- One row per (game_pk, player_id). No FK to dim_game so we can predict for scheduled games not yet loaded.
-- Run after 01_dims.sql (references dim_player).

CREATE TABLE IF NOT EXISTS predictions (
    game_pk           BIGINT NOT NULL,
    player_id         INTEGER NOT NULL REFERENCES dim_player(player_id),
    as_of_date        DATE NOT NULL,
    pred_bat_woba     NUMERIC(5, 4),
    pred_pit_fip      NUMERIC(5, 2),
    model_version_bat VARCHAR(64),
    model_version_pit VARCHAR(64),
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_pk, player_id)
);

CREATE INDEX IF NOT EXISTS idx_predictions_as_of_date ON predictions (as_of_date);
CREATE INDEX IF NOT EXISTS idx_predictions_player_id ON predictions (player_id);
