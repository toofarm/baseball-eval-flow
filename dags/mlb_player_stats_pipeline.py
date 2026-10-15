import logging
import os
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import DateTime
from typing import List, Optional, cast

from src.mlb_types import (
    ScheduleGame,
)

from src.extract import plan_ingest
from src.load.audit import record_load_audit
from src.load.connection import get_offload_hook
from src.load.staging import load_staging_schedule
from src.transform.validation import (
    validate_games_in_db,
    validate_schedule_games,
)

from src.extract.streaming_boxscore import fetch_and_load_player_stats_batched
from src.extract.streaming_play_by_play import fetch_and_load_play_by_play_batched
from src.load.supabase import (
    STATS_PIPELINE_TABLES,
    get_supabase_hook,
    offload_all,
)

# Environment variables for batch streaming
GAME_BATCH_SIZE = int(os.environ.get("MLB_GAME_BATCH_SIZE", "5"))
LOAD_ROW_BATCH_SIZE = int(os.environ.get("MLB_LOAD_ROW_BATCH_SIZE", "1000"))

# Recipients for pipeline failure alerts. Configure SMTP connection (e.g. smtp_default) in Airflow.
FAILURE_ALERT_EMAILS = ["alerts@example.com"]

# dbt project path (mounted at /opt/airflow/app in Docker)
DBT_PROJECT_DIR = Path(os.environ.get(
    "AIRFLOW_PROJ_DIR", "/opt/airflow/app")) / "dbt"

# The run for a given day's games starts at 1am Eastern the next morning.
# Game dates are taken in this timezone too.
PIPELINE_TZ = "America/New_York"

# Each run re-pulls this many days of games (the target day plus the days
# before it), so scoring changes and late-finishing games get picked up.
LOOKBACK_DAYS = int(os.environ.get("MLB_LOOKBACK_DAYS", "3"))

# How long past the scheduled start the sensor waits for the target day's
# games to finish before ingesting without the stragglers (they get picked up
# by the next run's lookback).
MAX_WAIT_HOURS = float(os.environ.get("MLB_MAX_WAIT_HOURS", "4"))

log = logging.getLogger(__name__)


def _target_date(data_interval_start: Optional[DateTime]) -> date:
    """The game date a run covers: the start of its data interval, in Eastern time.

    With CronDataIntervalTimetable the run that starts at 1am on day N+1 has
    data_interval_start = 1am on day N, so this is "yesterday" relative to
    when the run executes, for scheduled runs and backfills alike.
    """
    if data_interval_start is None:
        raise ValueError("data_interval_start is required")
    return data_interval_start.in_timezone(PIPELINE_TZ).date()


@dag(
    # Data-interval timetable (not the Airflow 3 default CronTriggerTimetable,
    # where data_interval_start is the run time itself). Set here rather than
    # via create_cron_data_intervals so ml_predictions_pipeline is unaffected.
    schedule=CronDataIntervalTimetable("0 1 * * *", timezone=PIPELINE_TZ),
    start_date=pendulum.datetime(2024, 1, 1, tz=PIPELINE_TZ),
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

    # Hard timeout is a backstop for a schedule API that stays down; the soft
    # MAX_WAIT_HOURS deadline below is what normally ends the wait.
    @task.sensor(
        poke_interval=600,
        timeout=int(3600 * (MAX_WAIT_HOURS + 2)),
        mode="reschedule",
    )
    def wait_for_final_games(
        data_interval_start: Optional[DateTime] = None,
        data_interval_end: Optional[DateTime] = None,
    ) -> PokeReturnValue:
        """Wait for the target day's games to finish; return the games to ingest.

        Returns every final game in the lookback window. Postponed, cancelled
        and suspended games never hold the run up. Games that are still live
        (in progress, delayed) are waited on until MAX_WAIT_HOURS past the
        scheduled start, then left for the next run. An off day or the
        offseason returns an empty list rather than failing.
        """
        if data_interval_end is None:
            raise ValueError("data_interval_end is required")
        target = _target_date(data_interval_start)
        try:
            ready, pending = plan_ingest(target, LOOKBACK_DAYS)
        except Exception:
            log.exception("Schedule fetch for %s failed; will poke again", target)
            return PokeReturnValue(is_done=False)

        if pending:
            summary = ", ".join(f"{g['game_id']} ({g['status']})" for g in pending)
            deadline = data_interval_end + timedelta(hours=MAX_WAIT_HOURS)
            if pendulum.now("UTC") < deadline:
                log.info(
                    "Waiting on %d game(s) dated %s until %s: %s",
                    len(pending), target, deadline, summary,
                )
                return PokeReturnValue(is_done=False)
            log.warning(
                "Wait deadline %s passed; ingesting without %d unfinished game(s) "
                "dated %s (the next run's lookback will pick them up): %s",
                deadline, len(pending), target, summary,
            )

        log.info(
            "Ingesting %d final game(s) for %s through %s",
            len(ready), target - timedelta(days=LOOKBACK_DAYS - 1), target,
        )
        return PokeReturnValue(is_done=True, xcom_value=ready)

    @task()
    def validate_schedule_data(games: List[ScheduleGame]) -> List[ScheduleGame]:
        # min_games=0: an off day or the offseason legitimately has nothing to load.
        validate_schedule_games(games, min_games=0)
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
    def load_play_by_play_task(
        schedule_games: List[ScheduleGame],
        conn_id: Optional[str] = None,
    ) -> int:
        """Fetch and load play-by-play (one JSONB row per game) into staging."""
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            n_games = fetch_and_load_play_by_play_batched(
                conn, schedule_games, GAME_BATCH_SIZE
            )
            conn.commit()
            return n_games
        finally:
            conn.close()

    @task()
    def run_dbt_task(
        data_interval_start: Optional[DateTime] = None,
    ) -> None:
        """Run dbt seed and dbt run, then refresh rolling stats for the lookback days.

        The full run (as_of_date = target day) rebuilds every table from
        staging. player_rolling_stats is the one incremental model, keyed on
        as_of_date, so it is re-run for each earlier day in the lookback window
        to pick up games that changed since that day was first processed.
        """
        target = _target_date(data_interval_start)
        dbt_dir = str(DBT_PROJECT_DIR)
        dbt_target = os.environ.get("DBT_TARGET", "prod")
        base = ["--project-dir", dbt_dir, "--target", dbt_target]

        def vars_for(d: date) -> list[str]:
            return ["--vars", f'{{"as_of_date": "{d.isoformat()}"}}']

        cmds = [
            ["dbt", "seed", *base],
            ["dbt", "run", *base, *vars_for(target)],
        ]
        for days_back in range(1, LOOKBACK_DAYS):
            cmds.append([
                "dbt", "run", *base,
                "--select", "player_rolling_stats",
                *vars_for(target - timedelta(days=days_back)),
            ])
        for cmd in cmds:
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
    def validate_games_loaded(
        schedule_games: List[ScheduleGame],
        conn_id: Optional[str] = None,
    ) -> None:
        """Check every ingested game reached dim_game and fact_game_state; raise if not."""
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            validate_games_in_db(conn, [int(g["game_id"]) for g in schedule_games])
        finally:
            conn.close()

    @task()
    def record_load_audit_task(
        data_interval_start: Optional[DateTime] = None,
        conn_id: Optional[str] = None,
    ) -> None:
        """Record successful mlb_player_stats load for freshness checks."""
        target = _target_date(data_interval_start)
        hook = get_offload_hook(conn_id)
        conn = hook.get_conn()
        try:
            record_load_audit(conn, "mlb_player_stats", target)
            conn.commit()
        finally:
            conn.close()

    @task()
    def offload_to_supabase_task(
        conn_id: Optional[str] = None,
        supabase_conn_id: Optional[str] = None,
    ) -> dict[str, int]:
        """Copy app-facing dbt views from Snowflake into Supabase analytics schema.

        Runs after the freshness audit so a Supabase outage doesn't block
        downstream pipelines (ml_predictions) that gate on the audit.
        """
        sf_conn = get_offload_hook(conn_id).get_conn()
        sb_conn = get_supabase_hook(supabase_conn_id).get_conn()
        try:
            results = offload_all(sf_conn, sb_conn, STATS_PIPELINE_TABLES)
            sb_conn.commit()
            return results
        finally:
            sf_conn.close()
            sb_conn.close()

    # Wait for the target day's games to finish; returns the final games in
    # the lookback window (empty on off days, never fails on postponements)
    ready_games = wait_for_final_games()
    validated_schedule = validate_schedule_data(
        cast(List[ScheduleGame], ready_games))

    # Load raw data to staging tables (boxscore + play-by-play run in parallel)
    load_result = load_staging_task(
        cast(List[ScheduleGame], validated_schedule),
    )
    pbp_result = load_play_by_play_task(
        cast(List[ScheduleGame], validated_schedule),
    )

    # Run dbt: seed constants, build dims, fact, rolling stats, fact_pitch,
    # then refresh rolling stats for the earlier lookback days
    dbt_task = run_dbt_task()
    dbt_task.set_upstream(load_result)
    dbt_task.set_upstream(pbp_result)

    # Validate every ingested game reached dim_game / fact_game_state
    validate_task = validate_games_loaded(
        cast(List[ScheduleGame], validated_schedule),
    )
    validate_task.set_upstream(dbt_task)

    # Record load for freshness checks
    audit_task = record_load_audit_task()
    audit_task.set_upstream(validate_task)

    # Reverse-ETL: push consumer-facing dbt views into Supabase analytics schema.
    # Runs downstream of audit so a Supabase outage doesn't block ml_predictions
    # (which only gates on pipeline_load_audit).
    offload_task = offload_to_supabase_task()
    offload_task.set_upstream(audit_task)


mlb_player_stats_pipeline()
