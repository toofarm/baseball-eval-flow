"""Play-by-play extraction: fetch and parse MLB Stats API playByPlay endpoint.

The DAG path stores the raw ``allPlays`` array in staging and lets dbt flatten
it to ``fact_pitch``. ``parse_play_by_play_pitches`` produces the same flattened
rows in Python — used by unit tests and available for callers that want pitch
rows without going through dbt.
"""

from typing import List, Optional

import requests

from src.extract.boxscore import MLB_API_HEADERS
from src.mlb_types import PitchRow, Play, PlayByPlayResponse

# MLB play-by-play API: https://statsapi.mlb.com/api/v1/game/{gamePk}/playByPlay
MLB_PLAY_BY_PLAY_BASE = "https://statsapi.mlb.com/api/v1/game"


def fetch_play_by_play(game_pk: int, timeout: int = 30) -> PlayByPlayResponse:
    """HTTP GET playByPlay for game_pk. Returns raw JSON. Raises on HTTP error."""
    url = f"{MLB_PLAY_BY_PLAY_BASE}/{game_pk}/playByPlay"
    resp = requests.get(url, headers=MLB_API_HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _get(d: Optional[dict], *path: str):
    """Safe nested-get that returns None at the first missing key."""
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _build_pitch_row(
    play: Play,
    event: dict,
    game_pk: int,
    balls_before: int,
    strikes_before: int,
    outs_before: Optional[int],
) -> Optional[PitchRow]:
    matchup = play.get("matchup") or {}
    about = play.get("about") or {}
    result = play.get("result") or {}
    details = event.get("details") or {}
    pitch_data = event.get("pitchData") or {}
    breaks = pitch_data.get("breaks") or {}
    coords = pitch_data.get("coordinates") or {}

    pitcher_id = _get(matchup, "pitcher", "id")
    batter_id = _get(matchup, "batter", "id")
    at_bat_index = play.get("atBatIndex")
    pitch_number = event.get("pitchNumber")
    inning = about.get("inning")
    half_inning = about.get("halfInning")

    # Required for fact_pitch PK / FKs. Skip malformed.
    if (
        pitcher_id is None
        or batter_id is None
        or at_bat_index is None
        or pitch_number is None
        or inning is None
        or half_inning is None
    ):
        return None

    return {
        "game_pk": game_pk,
        "at_bat_index": int(at_bat_index),
        "pitch_number": int(pitch_number),
        "pitcher_id": int(pitcher_id),
        "batter_id": int(batter_id),
        "pitch_type_code": _get(details, "type", "code"),
        "start_speed": pitch_data.get("startSpeed"),
        "end_speed": pitch_data.get("endSpeed"),
        "spin_rate": breaks.get("spinRate"),
        "spin_direction": breaks.get("spinDirection"),
        "break_angle": breaks.get("breakAngle"),
        "break_length": breaks.get("breakLength"),
        "break_vertical": breaks.get("breakVertical"),
        "break_vertical_induced": breaks.get("breakVerticalInduced"),
        "break_horizontal": breaks.get("breakHorizontal"),
        "plate_x": coords.get("pX"),
        "plate_z": coords.get("pZ"),
        "zone": pitch_data.get("zone"),
        "inning": int(inning),
        "half_inning": str(half_inning),
        "balls_before": balls_before,
        "strikes_before": strikes_before,
        "outs_before": outs_before,
        "bat_side": _get(matchup, "batSide", "code"),
        "pitch_hand": _get(matchup, "pitchHand", "code"),
        "is_strike": details.get("isStrike"),
        "is_ball": details.get("isBall"),
        "is_in_play": details.get("isInPlay"),
        "call_code": _get(details, "call", "code"),
        "at_bat_event": result.get("event"),
    }


def parse_play_by_play_pitches(
    pbp: PlayByPlayResponse, game_pk: int
) -> List[PitchRow]:
    """Flatten allPlays[].playEvents pitches into one PitchRow per pitch.

    MLB reports balls/strikes/outs AFTER each pitch; we walk events in order
    and emit the pre-pitch count on each row.

    Outs reset to 0 at each half-inning boundary and only change when an
    at-bat ends, so outs_before for every pitch in a play is the outs count
    entering that play. We track that across plays in the same half-inning.
    """
    rows: List[PitchRow] = []
    plays = pbp.get("allPlays") or []

    cur_inning: Optional[int] = None
    cur_half: Optional[str] = None
    outs_entering_play = 0

    for play in plays:
        about = play.get("about") or {}
        inning = about.get("inning")
        half = about.get("halfInning")

        # Reset outs at each half-inning boundary.
        if (inning, half) != (cur_inning, cur_half):
            cur_inning, cur_half = inning, half
            outs_entering_play = 0

        prev_balls, prev_strikes = 0, 0
        last_post_outs: Optional[int] = outs_entering_play

        for event in play.get("playEvents") or []:
            if not event.get("isPitch"):
                continue
            row = _build_pitch_row(
                play,
                event,
                game_pk,
                balls_before=prev_balls,
                strikes_before=prev_strikes,
                outs_before=outs_entering_play,
            )
            if row is not None:
                rows.append(row)
            ev_count = event.get("count") or {}
            prev_balls = int(ev_count.get("balls", prev_balls))
            prev_strikes = int(ev_count.get("strikes", prev_strikes))
            if ev_count.get("outs") is not None:
                last_post_outs = int(ev_count["outs"])

        outs_entering_play = (
            last_post_outs if last_post_outs is not None else outs_entering_play
        )

    return rows
