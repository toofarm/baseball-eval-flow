"""Schedule extraction: get games for a date and check MLB data readiness.

Calls the MLB Stats API schedule endpoint directly (mirrors boxscore.py and
play_by_play.py) rather than going through the MLB-StatsAPI library: we only
consume a handful of top-level fields, and the library gave no way to set the
browser-like headers the CDN requires (see boxscore.py).
"""

from datetime import date, timedelta
from typing import List

from src.extract.http import get_json
from src.mlb_types import ScheduleGame

# MLB schedule API: https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=MM/DD/YYYY
MLB_SCHEDULE_BASE = "https://statsapi.mlb.com/api/v1/schedule"

# Statuses for which the MLB API exposes a final winner.
_FINAL_STATUSES = {"Final", "Game Over", "Completed Early"}

# status.codedGameState values that decide whether a game is ingested. Match on
# the coded state, not abstractGameState: postponed games report
# abstractGameState "Final" even though no stats exist for them.
#   F    final (incl. "Completed Early") -> boxscore is complete, ingest it
#   D, C postponed / cancelled           -> nothing to ingest; a postponed game
#                                           keeps its gamePk and shows up as F
#                                           on its makeup date
#   T, U suspended                       -> partial stats until resumed; the
#                                           resumed game goes F under the same
#                                           gamePk and is picked up then
# Anything else (S scheduled, P pre-game, I in progress, delayed variants) is
# still live and may finish tonight.
_INGESTIBLE_CODED_STATES = {"F"}
_SETTLED_CODED_STATES = {"F", "D", "C", "T", "U"}


def fetch_schedule(date_str: str, timeout: int = 30) -> dict:
    """HTTP GET the schedule for a date (MM/DD/YYYY). Returns raw JSON. Raises on HTTP error."""
    return get_json(
        MLB_SCHEDULE_BASE,
        params={"sportId": 1, "date": date_str},
        timeout=timeout,
    )


def fetch_schedule_range(start: date, end: date, timeout: int = 30) -> dict:
    """HTTP GET the schedule for an inclusive date range. Returns raw JSON. Raises on HTTP error."""
    return get_json(
        MLB_SCHEDULE_BASE,
        params={
            "sportId": 1,
            "startDate": start.strftime("%m/%d/%Y"),
            "endDate": end.strftime("%m/%d/%Y"),
        },
        timeout=timeout,
    )


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
            status_obj = game.get("status") or {}
            status = status_obj.get("detailedState", "")

            game_info: ScheduleGame = {
                "game_id": game.get("gamePk"),
                "game_datetime": game.get("gameDate", ""),
                # officialDate is the date stats are credited to. It differs from
                # the wrapper date for a suspended game resumed on a later day.
                "game_date": game.get("officialDate") or date_obj.get("date", ""),
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
                "coded_game_state": status_obj.get("codedGameState", ""),
                "if_necessary": game.get("ifNecessary", "N"),
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


def is_ingestible(game: ScheduleGame) -> bool:
    """True when the game is final and its boxscore can be loaded."""
    return game.get("coded_game_state", "") in _INGESTIBLE_CODED_STATES


def is_pending(game: ScheduleGame) -> bool:
    """True when the game could still finish and is worth waiting for.

    Postponed, cancelled and suspended games won't produce final stats tonight,
    so they never hold up a run. Unplayed "if necessary" postseason games stay
    Scheduled forever once the series ends, so they don't either.
    """
    state = game.get("coded_game_state", "")
    if state in _SETTLED_CODED_STATES:
        return False
    if state == "S" and game.get("if_necessary") == "Y":
        return False
    return True


def dedupe_games(games: List[ScheduleGame]) -> List[ScheduleGame]:
    """Collapse games listed on more than one date to one entry per gamePk.

    A postponed game is listed as D on its original date and F on its makeup
    date; a suspended game is listed on both its original and resume dates.
    Prefer an ingestible listing, then the latest one (the API returns dates in
    ascending order).
    """
    by_pk: dict[int, ScheduleGame] = {}
    for g in games:
        pk = g["game_id"]
        current = by_pk.get(pk)  # type: ignore[call-overload]
        if current is None or is_ingestible(g) or not is_ingestible(current):
            by_pk[pk] = g  # type: ignore[index]
    return list(by_pk.values())


def plan_ingest(
    target_date: date, lookback_days: int
) -> tuple[List[ScheduleGame], List[ScheduleGame]]:
    """Split the schedule window ending on ``target_date`` into (ready, pending).

    The window covers ``target_date`` and the ``lookback_days - 1`` days before
    it, so each run re-pulls recent games and picks up scoring changes and
    games that weren't final on an earlier run.

    - ready: every final game in the window, one entry per gamePk.
    - pending: games officially dated ``target_date`` that may still finish
      (see is_pending). Games earlier in the window are never waited on; if
      one hasn't settled yet, a later run's lookback will catch it.
    """
    start = target_date - timedelta(days=lookback_days - 1)
    games = dedupe_games(parse_schedule_games(fetch_schedule_range(start, target_date)))
    target_iso = target_date.isoformat()
    ready = [g for g in games if is_ingestible(g)]
    pending = [g for g in games if g.get("game_date") == target_iso and is_pending(g)]
    return ready, pending
