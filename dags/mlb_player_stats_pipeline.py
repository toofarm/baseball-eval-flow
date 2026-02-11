import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]
from pendulum import DateTime
from typing import List, Optional, cast

from mlb_types import (
    LoadReadyPlayerGame,
    PlayerStatsWithContext,
    ScheduleGame,
    TransformedGameData,
)

from src.extract import (
    check_mlb_data_ready,
    fetch_player_stats_for_games,
    get_schedule_for_date,
)
from src.load.postgres import load_to_postgres
from src.load.rolling_stats import (
    fetch_game_rows_for_rolling,
    upsert_rolling_stats,
)
from src.transform.games import transform_games
from src.transform.player_stats import transform_player_stats_to_load_ready
from src.transform.rolling_stats import ROLLING_WINDOW_DAYS, compute_rolling_stats
from src.transform.validation import (
    validate_schedule_games,
    validate_transformed_games,
    validate_player_stats_with_context_list,
)


@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics"],
)
def mlb_player_stats_pipeline():

    @task.sensor(poke_interval=300, timeout=3600 * 6, mode="reschedule")
    def check_mlb_data_readiness(
        data_interval_start: Optional[DateTime] = None,
    ) -> PokeReturnValue:
        is_done, xcom_value = check_mlb_data_ready(data_interval_start)
        return PokeReturnValue(is_done=is_done, xcom_value=xcom_value)

    @task()
    def extract_yesterdays_games(
        data_interval_start: Optional[DateTime] = None,
    ) -> List[ScheduleGame]:
        if data_interval_start is None:
            raise ValueError("data_interval_start is required")
        yesterday = data_interval_start.in_timezone("UTC").strftime("%m/%d/%Y")
        return cast(List[ScheduleGame], get_schedule_for_date(yesterday))

    @task()
    def validate_schedule_data(games: List[ScheduleGame]) -> List[ScheduleGame]:
        validate_schedule_games(games, min_games=1)
        return games

    @task()
    def transform_game_data(games: List[ScheduleGame]) -> List[TransformedGameData]:
        return transform_games(cast(List[ScheduleGame], games))

    @task()
    def validate_game_transforms(
        raw_games: List[ScheduleGame],
        transformed_games: List[TransformedGameData],
    ) -> None:
        expected_pks = [int(g["game_id"]) for g in raw_games]
        validate_transformed_games(
            transformed_games,
            min_games=1,
            expected_game_pks=expected_pks,
        )

    @task()
    def validate_fetched_player_stats(
        stats_with_context: List[PlayerStatsWithContext],
    ) -> List[PlayerStatsWithContext]:
        validate_player_stats_with_context_list(stats_with_context, min_count=0)
        return stats_with_context

    @task()
    def fetch_player_stats(
        games: List[TransformedGameData],
    ) -> List[PlayerStatsWithContext]:
        return fetch_player_stats_for_games(cast(List[TransformedGameData], games))

    @task()
    def transform_player_stats_to_load_ready_task(
        transformed_games: List[TransformedGameData],
        stats_with_context: List[PlayerStatsWithContext],
    ) -> List[LoadReadyPlayerGame]:
        return transform_player_stats_to_load_ready(
            cast(List[TransformedGameData], transformed_games),
            cast(List[PlayerStatsWithContext], stats_with_context),
        )

    @task()
    def load_to_postgres_task(
        transformed_games: List[TransformedGameData],
        load_ready_rows: List[LoadReadyPlayerGame],
        conn_id: str = "mlb_postgres",
    ) -> dict:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            counts = load_to_postgres(
                conn,
                cast(List[TransformedGameData], transformed_games),
                cast(List[LoadReadyPlayerGame], load_ready_rows),
            )
            conn.commit()
            return counts
        finally:
            conn.close()

    @task()
    def compute_and_load_rolling_stats(
        conn_id: str = "mlb_postgres",
        lookback_days: int = 31,
    ) -> int:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            rows = fetch_game_rows_for_rolling(conn, lookback_days)
            if not rows:
                return 0
            as_of_date = max(r["game_date"] for r in rows)
            rolling_rows = compute_rolling_stats(
                rows,
                as_of_dates=[as_of_date],
                window_days=ROLLING_WINDOW_DAYS,
            )
            if not rolling_rows:
                return 0
            n = upsert_rolling_stats(conn, rolling_rows)
            conn.commit()
            return n
        finally:
            conn.close()

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
    load_ready_task = transform_player_stats_to_load_ready_task(
        cast(List[TransformedGameData], transformed_games),
        validated_stats,  # type: ignore[arg-type]
    )
    load_result = load_to_postgres_task(
        cast(List[TransformedGameData], transformed_games),
        load_ready_task,  # type: ignore[arg-type]
        conn_id="mlb_postgres",
    )
    compute_and_load_rolling_stats(conn_id="mlb_postgres").set_upstream(load_result)


mlb_player_stats_pipeline()
