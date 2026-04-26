-- Snowflake DDL: predictions table written by Airflow ML DAG (src/load/predictions.py).
-- Folds in the Postgres migration 05a_add_model_type.sql so the column and 3-part
-- PK are present from the initial CREATE.
--
-- One row per (game_pk, player_id, model_type). No FK to dim_player even though
-- the Postgres version declares one: dim_player is dbt-owned, and Snowflake does
-- not enforce FKs anyway, so the constraint would be informational only and
-- would create an apply-order dependency on dbt having run first.

CREATE TABLE IF NOT EXISTS predictions (
    game_pk           INTEGER      NOT NULL,
    player_id         INTEGER      NOT NULL,
    model_type        VARCHAR(16)  NOT NULL DEFAULT 'ridge',
    as_of_date        DATE         NOT NULL,
    pred_bat_woba     NUMBER(5, 4),
    pred_pit_fip      NUMBER(5, 2),
    model_version_bat VARCHAR(64),
    model_version_pit VARCHAR(64),
    created_at        TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (game_pk, player_id, model_type)
);
