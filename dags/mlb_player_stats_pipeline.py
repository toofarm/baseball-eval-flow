import pendulum
import statsapi  # The MLB-StatsAPI wrapper
from airflow.sdk import dag, task


# Types
from mlb_types import (
    PlayerStats,
    ScheduleGame,
    TransformedGameData,
    TransformedFieldingStats,
    TransformedPlayerData,
)
from mlb_types import BoxscoreResponse

# Advanced metrics utilities
from lib.pitching_advanced_metrics import transform_pitching_stats
from lib.batting_advanced_metrics import transform_batting_stats
from lib.fielding_advanced_metrics import calculate_fielding_runs


from typing import List, cast


@dag(
    schedule="0 2 * * *",  # Runs daily at 2 AM ET [5]
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics"],
)
def mlb_player_stats_pipeline():

    @task()
    def extract_yesterdays_games() -> List[ScheduleGame]:
        """
        Use statsapi.schedule to get game info from the previous day [6, 7].
        """
        # Calculate yesterday's date
        yesterday = pendulum.yesterday().strftime("%m/%d/%Y")

        # Pull schedule data for all teams [7, 8]
        games = statsapi.schedule(date=yesterday)
        return cast(List[ScheduleGame], games)

    @task()
    def transform_game_data(games: List[ScheduleGame]) -> List[TransformedGameData]:
        """
        Clean the raw JSON data and prepare it for loading [9].
        """
        cleaned_data: List[TransformedGameData] = []
        for game in games:
            game = cast(ScheduleGame, game)
            cleaned_data.append(
                TransformedGameData(
                    game_pk=game["game_pk"],
                    home_team=game["home_name"],
                    away_team=game["away_name"],
                    winning_team=game.get("winning_team", ""),
                    season=game["season"],
                    game_date=game["game_date"],
                    game_type=game["game_type"],
                    venue_id=game["venue_id"],
                )
            )
        return cleaned_data

    @task()
    def fetch_player_stats(games: List[TransformedGameData]) -> List[PlayerStats]:
        """
        Fetch player stats for a given game_pk [10].
        """
        res: List[PlayerStats] = []
        for game in games:
            game = cast(TransformedGameData, game)
            boxscore_data: BoxscoreResponse = statsapi.boxscore(game.game_pk)
            for team in boxscore_data.teams:
                for player in boxscore_data.teams[team].players:
                    player_stats = cast(PlayerStats, player.get("stats"))
                    if player_stats:
                        res.append(player_stats)
        return res

    def transform_fielding_stats(
        player_stats: PlayerStats,
        game: TransformedGameData,
    ) -> TransformedFieldingStats:
        """
        Transform fielding stats for a given player [11].
        """
        enriched_stats = cast(TransformedFieldingStats, player_stats.get("fielding"))

        enriched_stats["fielding_runs"] = calculate_fielding_runs(
            enriched_stats["assists"],
            enriched_stats["errors"],
            enriched_stats["chances"],
        )

        return TransformedFieldingStats(enriched_stats)

    @task()
    def tranform_player_stats(
        player_stats: List[PlayerStats],
        game: TransformedGameData,
    ) -> List[TransformedPlayerData]:
        """
        Transform player stats for a given player [11].
        """
        transformed_stats: List[TransformedPlayerData] = []
        for stat in player_stats:
            stat = cast(PlayerStats, stat)
            enriched_stats = {}
            if stat.get("pitching"):
                enriched_stats["pitching"] = transform_pitching_stats(stat, game)
            if stat.get("batting"):
                enriched_stats["batting"] = transform_batting_stats(stat, game)
            if stat.get("fielding"):
                enriched_stats["fielding"] = transform_fielding_stats(stat, game)

            transformed_stats.append(TransformedPlayerData(**enriched_stats))
        return transformed_stats

    # Build the flow by calling the functions [10, 11]
    # TaskFlow automatically handles the dependency: transform depends on extract [12]
    raw_games = extract_yesterdays_games()
    transformed_games = transform_game_data(cast(List[ScheduleGame], raw_games))


# Instantiate the DAG [13]
mlb_player_stats_pipeline()
