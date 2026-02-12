-- Pipeline load audit: record successful loads for freshness checks.
-- Dependent DAGs (e.g. ml_predictions) require upstream pipeline (e.g. mlb_player_stats) to have a load within the last 24 hours.

CREATE TABLE IF NOT EXISTS pipeline_load_audit (
    id            SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(64) NOT NULL,
    load_date     DATE,
    loaded_at     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_load_audit_name_loaded ON pipeline_load_audit (pipeline_name, loaded_at);
