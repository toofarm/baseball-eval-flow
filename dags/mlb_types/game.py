"""
Type definitions for MLB game and schedule API responses.

Based on game_meta.json (game feed) and statsapi.schedule() responses.
"""

from typing import Any, NotRequired, TypedDict

from pydantic import BaseModel, ConfigDict


# --- TypedDict (hints only, no validation) ---
# Used for statsapi.schedule() response - we trust the API and don't need validation


class ScheduleGame(TypedDict):
    """A single game from statsapi.schedule(date=...). Hints only."""

    # Required fields (always present in API response)
    game_pk: int
    game_id: str
    game_type: str
    status: str
    home_name: str
    away_name: str
    home_id: int
    away_id: int
    game_date: str
    venue_id: int
    season: int
    # Optional fields (may be absent for scheduled/postponed games)
    home_score: NotRequired[int]
    away_score: NotRequired[int]
    winning_team: NotRequired[str]
    losing_team: NotRequired[str]
    winning_team_id: NotRequired[int]
    losing_team_id: NotRequired[int]
    summary: NotRequired[str]
    double_header: NotRequired[str]
    game_num: NotRequired[int]
    home_probable_pitcher: NotRequired[str]
    away_probable_pitcher: NotRequired[str]
    venue_name: NotRequired[str]
    rescheduled_from: NotRequired[str]
    rescheduled_from_date: NotRequired[str]


# --- Pydantic models (validation + extra="ignore") ---
# Used for game_meta.json / game feed responses


class GameStatus(BaseModel):
    model_config = ConfigDict(extra="ignore")

    abstractGameState: str
    codedGameState: str
    detailedState: str
    statusCode: str
    startTimeTBD: bool = False
    abstractGameCode: str


class LeagueRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    wins: int
    losses: int
    pct: str


class TeamRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    link: str


class SideInfo(BaseModel):
    """Away or home team info in game metadata."""

    model_config = ConfigDict(extra="ignore")

    score: int
    isWinner: bool
    splitSquad: bool = False
    seriesNumber: int = 1
    leagueRecord: LeagueRecord
    team: TeamRef


class TeamsInfo(BaseModel):
    model_config = ConfigDict(extra="ignore")

    away: SideInfo
    home: SideInfo


class VenueRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    name: str
    link: str


class ContentRef(BaseModel):
    model_config = ConfigDict(extra="ignore")

    link: str


class GameMetadata(BaseModel):
    """
    Full game metadata from MLB game feed API (game_meta.json structure).
    """

    model_config = ConfigDict(extra="ignore")

    game_pk: int
    link: str
    gameType: str
    season: str
    gameDate: str
    officialDate: str
    isTie: bool = False
    gameNumber: int = 1
    publicFacing: bool = True
    doubleHeader: str = "N"
    gamedayType: str = "P"
    tiebreaker: str = "N"
    calendarEventID: str = ""
    seasonDisplay: str = ""
    dayNight: str = "D"
    scheduledInnings: int = 9
    reverseHomeAwayStatus: bool = False
    inningBreakLength: int = 120
    gamesInSeries: int = 1
    seriesGameNumber: int = 1
    seriesDescription: str = ""
    recordSource: str = "N"
    ifNecessary: str = "N"
    ifNecessaryDescription: str = ""
    status: GameStatus
    teams: TeamsInfo
    venue: VenueRef
    content: ContentRef | None = None


# Transformed game - Extracted data from statsapi.schedule()
class TransformedGameData(BaseModel):

    game_pk: int
    home_team: str
    away_team: str
    winning_team: str
    season: int
    game_date: str
    game_type: str
    venue_id: int
