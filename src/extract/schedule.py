"""Schedule extraction: get games for a date and check MLB data readiness."""

from typing import Any, List

import statsapi

from mlb_types import ScheduleGame


def get_schedule_for_date(date_str: str) -> List[ScheduleGame]:
    """Pull schedule data for the given date (MM/DD/YYYY). Returns list of schedule games."""
    games = statsapi.schedule(date=date_str)
    return games  # type: ignore[return-value]


def check_mlb_data_ready(data_interval_start: Any) -> tuple[bool, Any]:
    """
    Check if MLB has schedule data for the data interval's "yesterday" (UTC).
    Returns (is_done, xcom_value). xcom_value is the date string when done.
    """
    if data_interval_start is None:
        raise ValueError("data_interval_start is required")
    yesterday = data_interval_start.in_timezone("UTC").strftime("%m/%d/%Y")
    try:
        games = statsapi.schedule(date=yesterday)
        if games and len(games) > 0:
            return (True, yesterday)
        return (False, None)
    except Exception:
        return (False, None)
