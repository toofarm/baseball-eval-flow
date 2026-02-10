"""
Type definitions for MLB boxscore API responses.

Based on boxscore.json from statsapi.game_boxscore(game_pk).
"""

from typing import Any, TypedDict

from pydantic import BaseModel, ConfigDict


# --- TypedDict (hints only, no validation) ---
# Stats objects have many optional fields and mixed types (int | str for rates like ".242").
# TypedDict provides IDE hints without runtime validation overhead.


class BattingStats(TypedDict, total=True):
    """Batting statistics. Hints only - values may be int or str (e.g. avg=".278")."""

    summary: str
    gamesPlayed: int
    flyOuts: int
    groundOuts: int
    airOuts: int
    runs: int
    doubles: int
    triples: int
    homeRuns: int
    strikeOuts: int
    baseOnBalls: int
    intentionalWalks: int
    hits: int
    hitByPitch: int
    avg: str
    atBats: int
    obp: str
    slg: str
    ops: str
    caughtStealing: int
    stolenBases: int
    stolenBasePercentage: str
    caughtStealingPercentage: str
    groundIntoDoublePlay: int
    groundIntoTriplePlay: int
    plateAppearances: int
    totalBases: int
    rbi: int
    leftOnBase: int
    sacBunts: int
    sacFlies: int
    catchersInterference: int
    pickoffs: int
    atBatsPerHomeRun: str
    popOuts: int
    lineOuts: int
    babip: str
    groundOutsToAirouts: str


class PitchingStats(TypedDict, total=True):
    """Pitching statistics. Hints only."""

    summary: str
    gamesPlayed: int
    gamesStarted: int
    flyOuts: int
    groundOuts: int
    airOuts: int
    runs: int
    doubles: int
    triples: int
    homeRuns: int
    strikeOuts: int
    baseOnBalls: int
    intentionalWalks: int
    hits: int
    hitByPitch: int
    atBats: int
    obp: str
    caughtStealing: int
    stolenBases: int
    stolenBasePercentage: str
    caughtStealingPercentage: str
    numberOfPitches: int
    era: str
    inningsPitched: str
    wins: int
    losses: int
    saves: int
    saveOpportunities: int
    holds: int
    blownSaves: int
    earnedRuns: int
    whip: str
    battersFaced: int
    outs: int
    gamesPitched: int
    completeGames: int
    shutouts: int
    pitchesThrown: int
    balls: int
    strikes: int
    strikePercentage: str
    hitBatsmen: int
    balks: int
    wildPitches: int
    pickoffs: int
    groundOutsToAirouts: str
    rbi: int
    winPercentage: str
    pitchesPerInning: str
    gamesFinished: int
    strikeoutWalkRatio: str
    strikeoutsPer9Inn: str
    walksPer9Inn: str
    hitsPer9Inn: str
    runsScoredPer9: str
    homeRunsPer9: str
    inheritedRunners: int
    inheritedRunnersScored: int
    passedBall: int
    sacBunts: int
    sacFlies: int
    popOuts: int
    lineOuts: int


class FieldingStats(TypedDict, total=True):
    """Fielding statistics. Hints only."""

    gamesStarted: int
    caughtStealing: int
    stolenBases: int
    stolenBasePercentage: str
    caughtStealingPercentage: str
    assists: int
    putOuts: int
    errors: int
    chances: int
    fielding: str
    passedBall: int
    pickoffs: int


class PlayerStats(TypedDict, total=False):
    """Per-game stats for a player (batting, pitching, fielding). Hints only."""

    batting: BattingStats | dict[str, Any]
    pitching: PitchingStats | dict[str, Any]
    fielding: FieldingStats | dict[str, Any]


class PlayerBoxscore(TypedDict, total=False):
    """A single player entry in the boxscore. Hints only."""

    person: dict[str, Any]
    jerseyNumber: str
    position: dict[str, Any]
    status: dict[str, Any]
    parentTeamId: int
    battingOrder: str
    stats: PlayerStats
    seasonStats: PlayerStats
    gameStatus: dict[str, Any]
    allPositions: list[dict[str, Any]]


# --- Pydantic models (validation + extra="ignore") ---


class LeagueRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    link: str
    abbreviation: str = ""


class VenueRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    link: str


class LeagueRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wins: int
    losses: int
    ties: int = 0
    pct: str


class TeamRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    gamesPlayed: int
    wins: int
    losses: int
    winningPercentage: str
    divisionLeader: bool = False
    leagueRecord: LeagueRecord


class TeamBoxscoreSide(BaseModel):
    """
    One side (away or home) of the boxscore.
    teamStats.batting/pitching/fielding and players are left as dicts -
    use BattingStats, PitchingStats, FieldingStats, PlayerBoxscore TypedDicts
    for type hints when accessing those fields.
    """

    model_config = ConfigDict(extra="ignore")

    team: dict[str, Any]
    teamStats: dict[str, Any]  # batting, pitching, fielding
    players: list[PlayerBoxscore]  # "ID123456": PlayerBoxscore


class BoxscoreResponse(BaseModel):
    """
    Full boxscore response from statsapi.game_boxscore(game_pk).
    """

    model_config = ConfigDict(extra="ignore")

    copyright: str = ""
    teams: dict[str, TeamBoxscoreSide]  # {"away": {...}, "home": {...}}


# Transformed player data - Extracted data from statsapi.game_boxscore()
class TransformedPitchingStats(PitchingStats):

    fip: float
    babip: float
    home_run_rate: float


class TransformedBattingStats(BattingStats):

    babip: float
    home_run_rate: float
    wrc_plus: float
    woba: float
    ops: float


class TransformedFieldingStats(FieldingStats):

    fielding_runs: float


class TransformedPlayerData(TypedDict, total=False):

    pitching: TransformedPitchingStats | None
    batting: TransformedBattingStats | None
    fielding: TransformedFieldingStats | None
