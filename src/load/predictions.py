"""
Load prediction rows into the predictions table.
"""

from typing import Any


def load_predictions(conn: Any, rows: list[dict[str, Any]]) -> int:
    """
    Insert or upsert prediction rows into the predictions table.

    Each row must have: game_pk, player_id, as_of_date, pred_bat_woba, pred_pit_fip,
    model_version_bat, model_version_pit. ON CONFLICT (game_pk, player_id) DO UPDATE.
    Caller must commit and close the connection.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO predictions (
            game_pk, player_id, as_of_date,
            pred_bat_woba, pred_pit_fip,
            model_version_bat, model_version_pit
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (game_pk, player_id) DO UPDATE SET
            as_of_date = EXCLUDED.as_of_date,
            pred_bat_woba = EXCLUDED.pred_bat_woba,
            pred_pit_fip = EXCLUDED.pred_pit_fip,
            model_version_bat = EXCLUDED.model_version_bat,
            model_version_pit = EXCLUDED.model_version_pit
    """
    values = [
        (
            r["game_pk"],
            r["player_id"],
            r["as_of_date"],
            r.get("pred_bat_woba"),
            r.get("pred_pit_fip"),
            r.get("model_version_bat"),
            r.get("model_version_pit"),
        )
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    return len(rows)
