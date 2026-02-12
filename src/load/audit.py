"""
Pipeline load audit: record successful loads and check freshness for dependent DAGs.

- record_load_audit: insert a row after a successful pipeline load.
- check_freshness: raise ValueError if the given pipeline has no load within max_age_hours (UTC).
"""

from datetime import datetime, timezone, timedelta
from typing import Any


def record_load_audit(
    conn: Any,
    pipeline_name: str,
    load_date: Any = None,
) -> None:
    """
    Insert one row into pipeline_load_audit after a successful load.
    Caller must commit and close the connection.

    :param conn: DB connection (e.g. from PostgresHook.get_conn()).
    :param pipeline_name: e.g. "mlb_player_stats", "ml_predictions".
    :param load_date: optional date of the load (Python date or None); stored as load_date.
    """
    sql = """
        INSERT INTO pipeline_load_audit (pipeline_name, load_date)
        VALUES (%s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(sql, (pipeline_name, load_date))


def check_freshness(
    conn: Any,
    pipeline_name: str,
    max_age_hours: int = 24,
) -> None:
    """
    Ensure the given pipeline has at least one load within the last max_age_hours (UTC).
    Raises ValueError if no row exists or the latest loaded_at is too old.

    :param conn: DB connection (e.g. from PostgresHook.get_conn()).
    :param pipeline_name: e.g. "mlb_player_stats".
    :param max_age_hours: maximum age in hours of the latest load (default 24).
    """
    sql = """
        SELECT loaded_at FROM pipeline_load_audit
        WHERE pipeline_name = %s
        ORDER BY loaded_at DESC
        LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (pipeline_name,))
        row = cur.fetchone()
    if not row or row[0] is None:
        raise ValueError(
            f"Pipeline {pipeline_name!r} has no recorded load in pipeline_load_audit"
        )
    loaded_at = row[0]
    # If loaded_at is naive, assume UTC
    if loaded_at.tzinfo is None:
        loaded_at = loaded_at.replace(tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
    if loaded_at < cutoff:
        raise ValueError(
            f"Pipeline {pipeline_name!r} last load at {loaded_at} is older than "
            f"{max_age_hours}h (cutoff {cutoff})"
        )
