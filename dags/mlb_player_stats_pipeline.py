import pendulum
import statsapi  # The MLB-StatsAPI wrapper
from airflow.sdk import PokeReturnValue, dag, task
from lib.utils import normalize_date

# Types
from mlb_types import (
    PlayerStatsWithContext,
    LoadReadyPlayerGame,
    ScheduleGame,
    TransformedGameData,
    TransformedPlayerData,
)
from mlb_types import BoxscoreResponse

# Advanced metrics and load-ready builder
from lib.pitching_advanced_metrics import transform_pitching_stats
from lib.batting_advanced_metrics import transform_batting_stats
from lib.fielding_advanced_metrics import transform_fielding_stats
from lib.load_ready import to_load_ready_row

# Validators
from lib.validation import (
    validate_schedule_games,
    validate_transformed_games,
    validate_player_stats_with_context_list,
)

from typing import List, cast


@dag(
    schedule="0 2 * * *",  # Runs daily at 2 AM ET [5]
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics"],
)
def mlb_player_stats_pipeline():

    # First check to make sure we have new data in order to preempt failed runs
    @task.sensor(poke_interval=300, timeout=3600 * 6, mode="reschedule")
    def check_mlb_data_readiness() -> PokeReturnValue:
        yesterday = pendulum.yesterday()
        try:
            games = statsapi.schedule(date=yesterday)
            if games and len(games) > 0:
                return PokeReturnValue(is_done=True, xcom_value=yesterday)
            else:
                return PokeReturnValue(is_done=False)
        except Exception:
            return PokeReturnValue(is_done=False)

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
                    game_date=normalize_date(game["game_date"]),
                    game_type=game["game_type"],
                    venue_id=game["venue_id"],
                    home_team_id=game["home_id"],
                    away_team_id=game["away_id"],
                )
            )
        return cleaned_data

    @task()
    def validate_schedule_data(games: List[ScheduleGame]) -> List[ScheduleGame]:
        """Validate raw schedule data from extract. Fails task on invalid data. Returns games for downstream."""
        validate_schedule_games(games, min_games=1)
        return games

    @task()
    def validate_game_transforms(
        raw_games: List[ScheduleGame],
        transformed_games: List[TransformedGameData],
    ) -> None:
        """Validate transformed games and consistency with extract. Fails task on invalid data."""
        expected_pks = [g["game_pk"] for g in raw_games]
        validate_transformed_games(
            transformed_games,
            min_games=1,
            expected_game_pks=expected_pks,
        )

    @task()
    def validate_fetched_player_stats(
        stats_with_context: List[PlayerStatsWithContext],
    ) -> List[PlayerStatsWithContext]:
        """Validate player stats with context from fetch. Fails task on invalid data. Returns for downstream."""
        validate_player_stats_with_context_list(stats_with_context, min_count=0)
        return stats_with_context

    @task()
    def fetch_player_stats(
        games: List[TransformedGameData],
    ) -> List[PlayerStatsWithContext]:
        """
        Fetch player stats per game with game_pk, player_id, team_id, position.
        Returns load-ready context for each player appearance.
        """
        res: List[PlayerStatsWithContext] = []
        for game in games:
            game = cast(TransformedGameData, game)
            boxscore_data: BoxscoreResponse = statsapi.boxscore(game.game_pk)
            for team in boxscore_data.teams:
                for player in boxscore_data.teams[team].players:
                    player_stats = player.get("stats")
                    if not player_stats:
                        continue
                    person = player.get("person") or {}
                    position = player.get("position") or {}
                    player_id = person.get("id")
                    team_id = player.get("parentTeamId")
                    if player_id is None or team_id is None:
                        continue
                    res.append(
                        {
                            "game_pk": game.game_pk,
                            "player_id": player_id,
                            "team_id": team_id,
                            "position_code": str(position.get("code", "")),
                            "position_name": str(position.get("name", "")),
                            "stats": player_stats,
                        }
                    )
        return res

    @task()
    def transform_player_stats_to_load_ready(
        transformed_games: List[TransformedGameData],
        stats_with_context: List[PlayerStatsWithContext],
    ) -> List[LoadReadyPlayerGame]:
        """
        Transform each player's stats with correct game context and flatten to
        load-ready rows (one per game_pk, player_id) for fact_game_state.
        """
        game_by_pk = {g.game_pk: g for g in transformed_games}
        load_ready: List[LoadReadyPlayerGame] = []
        for item in stats_with_context:
            game = game_by_pk.get(item["game_pk"])
            if not game:
                continue
            stat = item["stats"]
            enriched: dict = {}
            if stat.get("pitching"):
                enriched["pitching"] = transform_pitching_stats(stat, game)
            if stat.get("batting"):
                enriched["batting"] = transform_batting_stats(stat, game)
            if stat.get("fielding"):
                enriched["fielding"] = transform_fielding_stats(stat, game)
            transformed = TransformedPlayerData(**enriched)
            row = to_load_ready_row(
                game_pk=item["game_pk"],
                player_id=item["player_id"],
                team_id=item["team_id"],
                position_code=item["position_code"],
                position_name=item["position_name"],
                transformed=transformed,
            )
            load_ready.append(row)
        return load_ready

    # Build the flow by calling the functions [10, 11]
    # TaskFlow automatically handles the dependency: transform depends on extract [12]
    check_mlb_data_readiness()
    raw_games = extract_yesterdays_games()
    validate_schedule_data(cast(List[ScheduleGame], raw_games))
    transformed_games = transform_game_data(cast(List[ScheduleGame], raw_games))
    validate_game_transforms(
        cast(List[ScheduleGame], raw_games),
        cast(List[TransformedGameData], transformed_games),
    )
    stats_with_context = fetch_player_stats(
        cast(List[TransformedGameData], transformed_games)
    )
    validated_stats = validate_fetched_player_stats(stats_with_context)  # type: ignore[arg-type]
    # Output consumed by load stage when implemented (TBD)
    # At parse time task outputs are XComArg; they resolve to list types at runtime.
    transform_player_stats_to_load_ready(
        cast(List[TransformedGameData], transformed_games),
        validated_stats,  # type: ignore[arg-type]
    )


# Instantiate the DAG [13]
mlb_player_stats_pipeline()
