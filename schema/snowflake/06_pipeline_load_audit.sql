-- Snowflake DDL: pipeline load audit table written by all Airflow DAGs
-- (src/load/audit.py). Records successful loads; check_freshness() reads it to
-- gate dependent DAGs.
--
-- Snowflake dialect notes:
--   * SERIAL              -> INTEGER AUTOINCREMENT START 1 INCREMENT 1
--   * TIMESTAMP WITH TZ   -> TIMESTAMP_TZ
--   * CREATE INDEX        -> dropped; not supported. Reads filter by
--                            pipeline_name + ORDER BY loaded_at DESC LIMIT 1,
--                            which is fine without an index at this row count.

CREATE TABLE IF NOT EXISTS pipeline_load_audit (
    id            INTEGER      AUTOINCREMENT START 1 INCREMENT 1,
    pipeline_name VARCHAR(64)  NOT NULL,
    load_date     DATE,
    loaded_at     TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id)
);
