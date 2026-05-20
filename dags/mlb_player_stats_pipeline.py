import os
import subprocess
from pathlib import Path

import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from pendulum import DateTime
from typing import List, Optional, cast

from src.mlb_types import (
    ScheduleGame,
)

from src.extract import (
    check_mlb_data_ready,
    get_schedule_for_date,
)
from src.load.audit import record_load_audit
from src.load.connection import get_offload_hook
from src.load.staging import load_staging_schedule
from src.transform.validation import (
    validate_game_count_from_db,
    validate_schedule_games,
)

from src.extract.streaming_boxscore import fetch_and_load_player_stats_batched

# Environment variables for batch streaming
GAME_BATCH_SIZE = int(os.environ.get("MLB_GAME_BATCH_SIZE", "5"))
LOAD_ROW_BATCH_SIZE = int(os.environ.get("MLB_LOAD_ROW_BATCH_SIZE", "1000"))

# Recipients for pipeline failure alerts. Configure SMTP connection (e.g. smtp_default) in Airflow.
FAILURE_ALERT_EMAILS = ["alerts@example.com"]

# dbt project path (mounted at /opt/airflow/app in Docker)
DBT_PROJECT_DIR = Path(os.environ.get(
    "AIRFLOW_PROJ_DIR", "/opt/airflow/app")) / "dbt"


@dag(
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["mlb_analytics"],
    default_args={
        "on_failure_callback": [
            send_smtp_notification(
                from_email="noreply@shanemadethat.com",
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
    def load_staging_task(
        schedule_games: List[ScheduleGame],
        conn_id: Optional[str] = None,
    ) -> dict:
        """Load raw schedule and player stats into staging tables."""
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            n_schedule = load_staging_schedule(conn, schedule_games)
            n_stats = fetch_and_load_player_stats_batched(
                conn, schedule_games, GAME_BATCH_SIZE
            )
            conn.commit()
            return {"schedule": n_schedule, "player_stats": n_stats}
        finally:
            conn.close()

    @task()
    def run_dbt_task(
        data_interval_start: Optional[DateTime] = None,
        **context: object,
    ) -> None:
        """Run dbt seed and dbt run with as_of_date var."""
        ds = context.get("data_interval_start") or data_interval_start
        if ds is None:
            raise ValueError("data_interval_start is required")
        as_of_date = (
            ds.in_timezone(  # pyright: ignore[reportAttributeAccessIssue]
                "UTC"
            ).strftime("%Y-%m-%d")
            if hasattr(ds, "in_timezone")
            else str(ds)[:10]
        )
        dbt_dir = str(DBT_PROJECT_DIR)
        dbt_target = os.environ.get("DBT_TARGET", "prod")
        vars_json = f'{{"as_of_date": "{as_of_date}"}}'
        for cmd in [
            ["dbt", "seed", "--project-dir", dbt_dir, "--target", dbt_target],
            [
                "dbt",
                "run",
                "--project-dir",
                dbt_dir,
                "--target",
                dbt_target,
                "--vars",
                vars_json,
            ],
        ]:
            result = subprocess.run(
                cmd,
                cwd=dbt_dir,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    f"dbt command failed (exit {result.returncode}): "
                    f"{' '.join(cmd)} — see task log for dbt output"
                )

    @task()
    def validate_game_row_count(
        schedule_games: List[ScheduleGame],
        conn_id: Optional[str] = None,
        data_interval_start: Optional[DateTime] = None,
        **context: object,
    ) -> None:
        """Compare dim_game count to schedule count; raise if mismatch."""
        ds = context.get("data_interval_start") or data_interval_start
        if ds is None:
            raise ValueError("data_interval_start is required")
        if hasattr(ds, "in_timezone"):
            yesterday = ds.in_timezone(  # pyright: ignore[reportAttributeAccessIssue]
                "UTC"
            ).date()
        elif hasattr(ds, "date"):
            yesterday = (
                ds.date()  # pyright: ignore[reportAttributeAccessIssue]
                if callable(getattr(ds, "date"))
                else ds
            )
        else:
            yesterday = ds
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            validate_game_count_from_db(conn, len(schedule_games), yesterday)
        finally:
            conn.close()

    @task()
    def record_load_audit_task(
        data_interval_start: Optional[DateTime] = None,
        conn_id: Optional[str] = None,
    ) -> None:
        """Record successful mlb_player_stats load for freshness checks."""
        if data_interval_start is None:
            raise ValueError("data_interval_start is required")
        yesterday = data_interval_start.in_timezone("UTC").date()
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            record_load_audit(conn, "mlb_player_stats", yesterday)
            conn.commit()
        finally:
            conn.close()

    # Check data readiness (sensor fails on empty API response so run doesn't reschedule forever)
    sensor_task = check_mlb_data_readiness()

    # Extract and validate schedule data (only after sensor succeeds)
    raw_games = extract_yesterdays_games()
    raw_games.set_upstream(sensor_task)
    validated_schedule = validate_schedule_data(
        cast(List[ScheduleGame], raw_games))

    # Load raw data to staging tables
    load_result = load_staging_task(
        cast(List[ScheduleGame], validated_schedule),
    )

    # Run dbt: seed constants, then build dims, fact, rolling stats
    dbt_task = run_dbt_task()
    dbt_task.set_upstream(load_result)

    # Validate game count in dim_game matches schedule
    validate_task = validate_game_row_count(
        cast(List[ScheduleGame], validated_schedule),
    )
    validate_task.set_upstream(dbt_task)

    # Record load for freshness checks
    record_load_audit_task().set_upstream(validate_task)


mlb_player_stats_pipeline()
