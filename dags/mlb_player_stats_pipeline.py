import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
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
from src.load.audit import record_load_audit
from src.load.postgres import load_to_postgres
from src.load.rolling_stats_sql import run_rolling_stats_incremental
from src.transform.games import transform_games
from src.transform.player_stats import transform_player_stats_to_load_ready
from src.transform.validation import (
    validate_game_load_count,
    validate_schedule_games,
    validate_transformed_games,
    validate_player_stats_with_context_list,
)

# Recipients for pipeline failure alerts. Configure SMTP connection (e.g. smtp_default) in Airflow.
FAILURE_ALERT_EMAILS = ["alerts@example.com"]


@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics"],
    default_args={
        "on_failure_callback": [
            send_smtp_notification(
                from_email="airflow@example.com",
                to=FAILURE_ALERT_EMAILS,
                subject="[MLB Pipeline] Task {{ ti.task_id }} failed in {{ dag.dag_id }}",
                html_content=(
                    "<p>Task <strong>{{ ti.task_id }}</strong> failed.</p>"
                    "<p><strong>DAG:</strong> {{ dag.dag_id }}</p>"
                    "<p><strong>Logical date:</strong> {{ data_interval_start }}</p>"
                    "<p><strong>Log:</strong> <a href='{{ ti.log_url }}'>View log</a></p>"
                    "{% if exception %}<p><strong>Exception:</strong> <pre>{{ exception }}</pre></p>{% endif %}"
                ),
            )
        ],
    },
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
    def validate_game_row_count(
        schedule_games: List[ScheduleGame],
        load_result: dict,
    ) -> None:
        """Compare games loaded to daily schedule; raise if mismatch."""
        validate_game_load_count(len(schedule_games), load_result)

    @task()
    def record_load_audit_task(
        data_interval_start: Optional[DateTime] = None,
        conn_id: str = "mlb_postgres",
    ) -> None:
        """Record successful mlb_player_stats load for freshness checks."""
        if data_interval_start is None:
            raise ValueError("data_interval_start is required")
        yesterday = data_interval_start.in_timezone("UTC").date()
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            record_load_audit(conn, "mlb_player_stats", yesterday)
            conn.commit()
        finally:
            conn.close()

    @task()
    def compute_and_load_rolling_stats(
        data_interval_start: Optional[DateTime] = None,
        conn_id: str = "mlb_postgres",
    ) -> int:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            if data_interval_start is not None:
                as_of_date = data_interval_start.in_timezone("UTC").date()
            else:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(game_date) FROM dim_game")
                    row = cur.fetchone()
                as_of_date = row[0] if row and row[0] is not None else None
            if as_of_date is None:
                return 0
            n = run_rolling_stats_incremental(conn, as_of_date)
            conn.commit()
            return n
        finally:
            conn.close()

    # Check data readiness (sensor fails on empty API response so run doesn't reschedule forever)
    sensor_task = check_mlb_data_readiness()

    # Extract and validate schedule data (only after sensor succeeds)
    raw_games = extract_yesterdays_games()
    raw_games.set_upstream(sensor_task)
    validate_schedule_data(cast(List[ScheduleGame], raw_games))

    # Transform and validate game data
    transformed_games = transform_game_data(cast(List[ScheduleGame], raw_games))
    validate_game_transforms(
        cast(List[ScheduleGame], raw_games),
        cast(List[TransformedGameData], transformed_games),
    )

    # Fetch and validate player stats
    stats_with_context = fetch_player_stats(
        cast(List[TransformedGameData], transformed_games)
    )
    validated_stats = validate_fetched_player_stats(stats_with_context)  # type: ignore[arg-type]

    # Transform player stats to load ready
    load_ready_task = transform_player_stats_to_load_ready_task(
        cast(List[TransformedGameData], transformed_games),
        validated_stats,  # type: ignore[arg-type]
    )

    # Load player stats to postgres
    load_result = load_to_postgres_task(
        cast(List[TransformedGameData], transformed_games),
        load_ready_task,  # type: ignore[arg-type]
        conn_id="mlb_postgres",
    )

    # Row count validation: games loaded must match daily schedule
    row_count_ok = validate_game_row_count(
        cast(List[ScheduleGame], raw_games),
        cast(dict, load_result),
    )
    row_count_ok.set_upstream(load_result)

    # Record load for freshness checks, then compute rolling stats
    record_load_audit_task(conn_id="mlb_postgres").set_upstream(row_count_ok)
    compute_and_load_rolling_stats(conn_id="mlb_postgres").set_upstream(row_count_ok)


mlb_player_stats_pipeline()
