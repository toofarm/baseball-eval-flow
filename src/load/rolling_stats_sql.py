"""
Run incremental rolling-stats SQL to update player_rolling_stats for one as_of_date.

The SQL file at src/load/sql/rolling_stats_incremental.sql expects the as_of_date
parameter repeated for each placeholder (6 times). This module loads the file and
executes with the correct parameter tuple.
"""

from pathlib import Path
from typing import Any

_SQL_FILE = Path(__file__).resolve().parent / "sql" / "rolling_stats_incremental.sql"


def run_rolling_stats_incremental(conn: Any, as_of_date: Any) -> int:
    """
    Execute the incremental rolling-stats SQL for the given as_of_date.

    Only players who had a game on as_of_date are updated. Windows 7 and 30 days.
    Returns the number of rows inserted or updated (cursor.rowcount).
    """
    sql = _SQL_FILE.read_text()
    # All %s placeholders take as_of_date; count them so param tuple length matches
    params = (as_of_date,) * sql.count("%s")
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount
