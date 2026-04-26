"""
Load prediction rows into the predictions table.

SQL is Snowflake-flavored (``MERGE INTO ... USING (SELECT ...)``). Caller must
supply a Snowflake DB-API connection and is responsible for commit/close.
"""

from typing import Any


_MERGE_PREDICTIONS_SQL = """
    MERGE INTO predictions AS tgt
    USING (
        SELECT
            %s AS game_pk,
            %s AS player_id,
            %s AS model_type,
            %s AS as_of_date,
            %s AS pred_bat_woba,
            %s AS pred_pit_fip,
            %s AS model_version_bat,
            %s AS model_version_pit
    ) AS src
    ON tgt.game_pk = src.game_pk
       AND tgt.player_id = src.player_id
       AND tgt.model_type = src.model_type
    WHEN MATCHED THEN UPDATE SET
        as_of_date = src.as_of_date,
        pred_bat_woba = src.pred_bat_woba,
        pred_pit_fip = src.pred_pit_fip,
        model_version_bat = src.model_version_bat,
        model_version_pit = src.model_version_pit
    WHEN NOT MATCHED THEN INSERT (
        game_pk, player_id, model_type, as_of_date,
        pred_bat_woba, pred_pit_fip,
        model_version_bat, model_version_pit
    ) VALUES (
        src.game_pk, src.player_id, src.model_type, src.as_of_date,
        src.pred_bat_woba, src.pred_pit_fip,
        src.model_version_bat, src.model_version_pit
    )
"""


def load_predictions(conn: Any, rows: list[dict[str, Any]]) -> int:
    """
    Merge prediction rows into the predictions table.

    Each row must have: game_pk, player_id, model_type, as_of_date, pred_bat_woba,
    pred_pit_fip, model_version_bat, model_version_pit. Match key is
    (game_pk, player_id, model_type). Caller must commit and close the connection.
    """
    if not rows:
        return 0

    values = [
        (
            r["game_pk"],
            r["player_id"],
            r["model_type"],
            r["as_of_date"],
            r.get("pred_bat_woba"),
            r.get("pred_pit_fip"),
            r.get("model_version_bat"),
            r.get("model_version_pit"),
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(_MERGE_PREDICTIONS_SQL, values)
    return len(rows)
