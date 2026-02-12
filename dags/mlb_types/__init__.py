"""
MLB API type definitions.

Import from here for convenience:
    from mlb_types import GameMetadata, ScheduleGame, BoxscoreResponse
"""

from dags.mlb_types.boxscore import (
    BattingStats,
    BoxscoreResponse,
    FieldingStats,
    LeagueRecord,
    LeagueRef,
    LoadReadyPlayerGame,
    PitchingStats,
    PlayerBoxscore,
    PlayerStats,
    PlayerStatsWithContext,
    TeamBoxscoreSide,
    TeamRecord,
    VenueRef,
    TransformedPitchingStats,
    TransformedBattingStats,
    TransformedFieldingStats,
    TransformedPlayerData,
)
from dags.mlb_types.game import (
    ContentRef,
    GameMetadata,
    GameStatus,
    LeagueRecord as GameLeagueRecord,
    ScheduleGame,
    SideInfo,
    TeamRef,
    TeamsInfo,
    VenueRef as GameVenueRef,
    TransformedGameData,
)

__all__ = [
    # game
    "ScheduleGame",
    "GameMetadata",
    "GameStatus",
    "GameLeagueRecord",
    "TeamRef",
    "SideInfo",
    "TeamsInfo",
    "GameVenueRef",
    "ContentRef",
    "TransformedGameData",
    # boxscore
    "BoxscoreResponse",
    "TeamBoxscoreSide",
    "BattingStats",
    "PitchingStats",
    "FieldingStats",
    "PlayerStats",
    "PlayerBoxscore",
    "LeagueRef",
    "LeagueRecord",  # from boxscore (has ties field)
    "TeamRecord",
    "VenueRef",
    "TransformedPitchingStats",
    "TransformedBattingStats",
    "TransformedFieldingStats",
    "TransformedPlayerData",
    "PlayerStatsWithContext",
    "LoadReadyPlayerGame",
]
