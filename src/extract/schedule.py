"""Schedule extraction: get games for a date and check MLB data readiness.

Calls the MLB Stats API schedule endpoint directly (mirrors boxscore.py and
play_by_play.py) rather than going through the MLB-StatsAPI library: we only
consume a handful of top-level fields, and the library gave no way to set the
browser-like headers the CDN requires (see boxscore.py).
"""

from typing import Any, List

import requests

from src.extract.boxscore import MLB_API_HEADERS
from src.mlb_types import ScheduleGame

# MLB schedule API: https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=MM/DD/YYYY
MLB_SCHEDULE_BASE = "https://statsapi.mlb.com/api/v1/schedule"

# Statuses for which the MLB API exposes a final winner.
_FINAL_STATUSES = {"Final", "Game Over", "Completed Early"}


def fetch_schedule(date_str: str, timeout: int = 30) -> dict:
    """HTTP GET the schedule for a date (MM/DD/YYYY). Returns raw JSON. Raises on HTTP error."""
    resp = requests.get(
        MLB_SCHEDULE_BASE,
        params={"sportId": 1, "date": date_str},
        headers=MLB_API_HEADERS,
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()


def parse_schedule_games(raw: dict) -> List[ScheduleGame]:
    """Flatten the schedule response into the ScheduleGame shape consumed downstream."""
    games: List[ScheduleGame] = []
    for date_obj in raw.get("dates") or []:
        for game in date_obj.get("games") or []:
            teams = game.get("teams") or {}
            home = teams.get("home") or {}
            away = teams.get("away") or {}
            home_team = home.get("team") or {}
            away_team = away.get("team") or {}
            venue = game.get("venue") or {}
            home_name = home_team.get("name", "")
            away_name = away_team.get("name", "")
            status = (game.get("status") or {}).get("detailedState", "")

            game_info: ScheduleGame = {
                "game_id": game.get("gamePk"),
                "game_datetime": game.get("gameDate", ""),
                "game_date": date_obj.get("date", ""),
                "game_type": game.get("gameType", ""),
                "status": status,
                "home_name": home_name,
                "away_name": away_name,
                "home_id": home_team.get("id"),
                "away_id": away_team.get("id"),
                "home_score": home.get("score", 0),
                "away_score": away.get("score", 0),
                "venue_id": venue.get("id"),
                "venue_name": venue.get("name", ""),
                "double_header": game.get("doubleHeader", "N"),
                "game_num": game.get("gameNumber", 1),
            }  # type: ignore[typeddict-item]

            # Only resolve a winner once the game is final (matches prior behavior;
            # load_staging_schedule derives winning_team_id from winning_team name).
            if status in _FINAL_STATUSES:
                if home.get("isWinner"):
                    game_info["winning_team"] = home_name
                    game_info["losing_team"] = away_name
                elif away.get("isWinner"):
                    game_info["winning_team"] = away_name
                    game_info["losing_team"] = home_name

            games.append(game_info)
    return games


def get_schedule_for_date(date_str: str) -> List[ScheduleGame]:
    """Pull schedule data for the given date (MM/DD/YYYY). Returns list of schedule games."""
    return parse_schedule_games(fetch_schedule(date_str))


def check_mlb_data_ready(data_interval_start: Any) -> tuple[bool, Any]:
    """
    Check if MLB has schedule data for the data interval's "yesterday" (UTC).
    Returns (is_done, xcom_value). xcom_value is the date string when done.

    - If the API returns one or more games: returns (True, yesterday).
    - If the API returns an empty list (no games for that date): raises ValueError
      so the sensor task fails and the DAG run fails (avoids rescheduling forever
      on off-days or missing data during backfill).
    - On API exception: returns (False, None) so the sensor keeps poking.
    """
    if data_interval_start is None:
        raise ValueError("data_interval_start is required")
    yesterday = data_interval_start.in_timezone("UTC").strftime("%m/%d/%Y")
    try:
        games = get_schedule_for_date(yesterday)
        if games and len(games) > 0:
            return (True, yesterday)
        # Empty response = no games for this date; fail so we don't reschedule forever
        raise ValueError(
            f"MLB API returned no games for date {yesterday}. "
            "Cannot proceed (off-day or missing data)."
        )
    except ValueError:
        raise
    except Exception:
        return (False, None)
