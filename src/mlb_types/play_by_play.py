"""
Type definitions for MLB play-by-play API responses.

Based on /game/{gamePk}/playByPlay. See docs:
https://github.com/pseudo-r/Public-MLB-API/blob/main/docs/game.md

Pitch-level fields live under playEvents[].pitchData and playEvents[].details.
TypedDicts are hints only (no runtime validation); the API returns nullable
fields liberally, so callers must handle missing keys defensively.
"""

from typing import List, TypedDict


class PitchCall(TypedDict, total=False):
    code: str           # 'B', 'S', 'X', ...
    description: str


class PitchTypeRef(TypedDict, total=False):
    code: str           # 'FF', 'SL', 'CH', ...
    description: str    # 'Four-Seam Fastball', ...


class PitchDetails(TypedDict, total=False):
    call: PitchCall
    description: str
    code: str
    type: PitchTypeRef
    isInPlay: bool
    isStrike: bool
    isBall: bool
    isOut: bool


class PitchBreaks(TypedDict, total=False):
    breakAngle: float
    breakLength: float
    breakY: float
    breakVertical: float
    breakVerticalInduced: float
    breakHorizontal: float
    spinRate: int
    spinDirection: int


class PitchCoordinates(TypedDict, total=False):
    pX: float
    pZ: float
    x: float
    y: float
    # Plus aX/aY/aZ/vX0/vY0/vZ0/x0/y0/z0/pfxX/pfxZ — unused for now.


class PitchData(TypedDict, total=False):
    startSpeed: float
    endSpeed: float
    strikeZoneTop: float
    strikeZoneBottom: float
    coordinates: PitchCoordinates
    breaks: PitchBreaks
    zone: int
    plateTime: float
    extension: float


class PitchCount(TypedDict, total=False):
    balls: int
    strikes: int
    outs: int


class PlayEvent(TypedDict, total=False):
    """One entry in allPlays[i].playEvents. Pitches have isPitch=True."""

    details: PitchDetails
    count: PitchCount
    pitchData: PitchData
    index: int
    playId: str
    pitchNumber: int
    startTime: str
    endTime: str
    isPitch: bool
    type: str           # 'pitch' | 'action' | ...


class PlayerRef(TypedDict, total=False):
    id: int
    fullName: str
    link: str


class HandRef(TypedDict, total=False):
    code: str           # 'L' | 'R' | 'S'
    description: str


class PlayMatchup(TypedDict, total=False):
    batter: PlayerRef
    pitcher: PlayerRef
    batSide: HandRef
    pitchHand: HandRef


class PlayAbout(TypedDict, total=False):
    atBatIndex: int
    halfInning: str     # 'top' | 'bottom'
    inning: int
    isComplete: bool
    isScoringPlay: bool


class PlayResult(TypedDict, total=False):
    type: str           # 'atBat' | 'action'
    event: str          # 'Single', 'Strikeout', ...
    eventType: str
    description: str
    rbi: int
    isOut: bool


class Play(TypedDict, total=False):
    """One entry in allPlays. Represents a complete at-bat (or a non-AB action)."""

    result: PlayResult
    about: PlayAbout
    matchup: PlayMatchup
    playEvents: List[PlayEvent]
    atBatIndex: int


class PlayByPlayResponse(TypedDict, total=False):
    allPlays: List[Play]


class PitchRow(TypedDict, total=False):
    """Flattened pitch record produced by parse_play_by_play_pitches.

    One row per pitch event; parent at-bat context is denormalized onto each row
    so downstream loads/marts don't need to re-join.
    """

    game_pk: int
    at_bat_index: int
    pitch_number: int
    pitcher_id: int
    batter_id: int
    pitch_type_code: str | None
    start_speed: float | None
    end_speed: float | None
    spin_rate: int | None
    spin_direction: int | None
    break_angle: float | None
    break_length: float | None
    break_vertical: float | None
    break_vertical_induced: float | None
    break_horizontal: float | None
    plate_x: float | None
    plate_z: float | None
    zone: int | None
    inning: int
    half_inning: str
    balls_before: int | None
    strikes_before: int | None
    outs_before: int | None
    bat_side: str | None
    pitch_hand: str | None
    is_strike: bool | None
    is_ball: bool | None
    is_in_play: bool | None
    call_code: str | None
    at_bat_event: str | None
