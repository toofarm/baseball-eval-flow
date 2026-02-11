import pendulum
import requests
import statsapi  # The MLB-StatsAPI wrapper (schedule only)
from airflow.sdk import PokeReturnValue, dag, task

# Types
from mlb_types import (
    PlayerStatsWithContext,
    LoadReadyPlayerGame,
    ScheduleGame,
    TransformedGameData,
    TransformedPlayerData,
)
from pendulum import DateTime
from typing import Optional

# Advanced metrics and load-ready builder
from lib.pitching_advanced_metrics import transform_pitching_stats
from lib.batting_advanced_metrics import transform_batting_stats
from lib.fielding_advanced_metrics import transform_fielding_stats
from lib.load_ready import to_load_ready_row
from lib.rolling_stats import compute_rolling_stats, ROLLING_WINDOW_DAYS
from lib.load_to_postgres import load_to_postgres as load_to_postgres_fn

# Validators
from lib.validation import (
    validate_schedule_games,
    validate_transformed_games,
    validate_player_stats_with_context_list,
)

from typing import List, cast

from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore[import-untyped]

# MLB boxscore API: https://statsapi.mlb.com/api/{ver}/game/{gamePk}/boxscore
MLB_BOXSCORE_BASE = "https://statsapi.mlb.com/api/v1/game"


@dag(
    schedule="0 2 * * *",  # Runs daily at 2 AM ET [5]
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["mlb_analytics"],
)
def mlb_player_stats_pipeline():

    # First check to make sure we have new data in order to preempt failed runs
    @task.sensor(poke_interval=300, timeout=3600 * 6, mode="reschedule")
    def check_mlb_data_readiness(
        data_interval_start: Optional[DateTime] = None,
    ) -> PokeReturnValue:
        if data_interval_start is None:
            raise ValueError("data_interval_start is required")

        yesterday = data_interval_start.in_timezone("UTC").strftime("%m/%d/%Y")

        try:
            games = statsapi.schedule(date=yesterday)
            if games and len(games) > 0:
                return PokeReturnValue(is_done=True, xcom_value=yesterday)
            else:
                return PokeReturnValue(is_done=False)
        except Exception:
            return PokeReturnValue(is_done=False)

    @task()
    def extract_yesterdays_games(
        data_interval_start: Optional[DateTime] = None,
    ) -> List[ScheduleGame]:
        """
        Use statsapi.schedule to get game info from the previous day [6, 7].
        """

        if data_interval_start is None:
            raise ValueError("data_interval_start is required")

        # Calculate yesterday's date
        # yesterday = pendulum.yesterday().strftime("%m/%d/%Y")
        yesterday = data_interval_start.in_timezone("UTC").strftime("%m/%d/%Y")

        # Pull schedule data for all teams [7, 8]
        games = statsapi.schedule(date=yesterday)
        return cast(List[ScheduleGame], games)

    @task()
    def validate_schedule_data(games: List[ScheduleGame]) -> List[ScheduleGame]:
        """Validate raw schedule data from extract. Fails task on invalid data. Returns games for downstream."""
        validate_schedule_games(games, min_games=1)
        return games

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
                    game_pk=int(game["game_id"]),
                    home_team=game["home_name"],
                    away_team=game["away_name"],
                    winning_team=game.get("winning_team", ""),
                    season=int(game["game_date"][:4]),
                    game_date=game["game_date"],
                    game_type=game["game_type"],
                    venue_id=game["venue_id"],
                    home_team_id=game["home_id"],
                    away_team_id=game["away_id"],
                )
            )
        return cleaned_data

    @task()
    def validate_game_transforms(
        raw_games: List[ScheduleGame],
        transformed_games: List[TransformedGameData],
    ) -> None:
        """Validate transformed games and consistency with extract. Fails task on invalid data."""
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
        """Validate player stats with context from fetch. Fails task on invalid data. Returns for downstream."""
        validate_player_stats_with_context_list(stats_with_context, min_count=0)
        return stats_with_context

    @task()
    def fetch_player_stats(
        games: List[TransformedGameData],
    ) -> List[PlayerStatsWithContext]:
        """
        Fetch player stats per game via MLB Stats API boxscore endpoint.
        Returns load-ready context for each player appearance.
        """
        res: List[PlayerStatsWithContext] = []
        for game in games:
            game = cast(TransformedGameData, game)
            url = f"{MLB_BOXSCORE_BASE}/{game.game_pk}/boxscore"
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            teams_data = data.get("teams") or {}
            for team_key, team_obj in teams_data.items():
                players = team_obj.get("players")
                if not players or not isinstance(players, dict):
                    continue
                for player in players.values():
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

    @task()
    def load_to_postgres(
        transformed_games: List[TransformedGameData],
        load_ready_rows: List[LoadReadyPlayerGame],
        conn_id: str = "mlb_postgres",
    ) -> dict:
        """
        Load transformed games and load-ready player rows into PostgreSQL.
        Upserts dim_team, dim_player, dim_game, and fact_game_state using
        PostgresHook. Returns counts: teams, players, games, fact_rows.
        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            counts = load_to_postgres_fn(conn, transformed_games, load_ready_rows)
            conn.commit()
            return counts
        except Exception:
            raise
        finally:
            conn.close()

    @task()
    def compute_and_load_rolling_stats(
        conn_id: str = "mlb_postgres",
        lookback_days: int = 31,
    ) -> int:
        """
        Query fact_game_state + dim_game for the last lookback_days, compute 7- and
        30-day rolling aggregates and rate stats (bat_avg, bat_ops, bat_woba,
        bat_wrc_plus, pit_era, pit_fip, pit_whip), then upsert into player_rolling_stats.
        Requires fact_game_state and dim_game to be populated (e.g. by a load step).
        """
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        # Fetch game-level rows with game_date and season for rolling window
        query = """
            SELECT
                f.player_id,
                g.game_date,
                g.season,
                f.bat_games_played, f.bat_plate_appearances, f.bat_at_bats,
                f.bat_runs, f.bat_hits, f.bat_doubles, f.bat_triples, f.bat_home_runs,
                f.bat_rbi, f.bat_strike_outs, f.bat_base_on_balls,
                f.bat_stolen_bases, f.bat_caught_stealing,
                f.bat_intentional_walks, f.bat_hit_by_pitch, f.bat_sac_flies,
                f.bat_total_bases,
                f.pit_games_played, f.pit_innings_pitched, f.pit_wins, f.pit_losses,
                f.pit_saves, f.pit_hits, f.pit_earned_runs, f.pit_strike_outs,
                f.pit_base_on_balls, f.pit_fip, f.pit_hit_batsmen,
                f.fld_assists, f.fld_put_outs, f.fld_errors, f.fld_chances
            FROM fact_game_state f
            JOIN dim_game g ON f.game_pk = g.game_pk
            WHERE g.game_date > CURRENT_DATE - %s
        """
        with conn.cursor() as cur:
            cur.execute(query, (lookback_days,))
            columns = [c.name for c in cur.description]
            rows = [dict(zip(columns, r)) for r in cur.fetchall()]

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

        cols = [
            "player_id",
            "as_of_date",
            "window_days",
            "bat_games_played",
            "bat_plate_appearances",
            "bat_at_bats",
            "bat_runs",
            "bat_hits",
            "bat_doubles",
            "bat_triples",
            "bat_home_runs",
            "bat_rbi",
            "bat_strike_outs",
            "bat_base_on_balls",
            "bat_stolen_bases",
            "bat_caught_stealing",
            "bat_avg",
            "bat_ops",
            "bat_woba",
            "bat_wrc_plus",
            "pit_games_played",
            "pit_innings_pitched",
            "pit_wins",
            "pit_losses",
            "pit_saves",
            "pit_hits",
            "pit_earned_runs",
            "pit_strike_outs",
            "pit_base_on_balls",
            "pit_era",
            "pit_fip",
            "pit_whip",
            "fld_assists",
            "fld_put_outs",
            "fld_errors",
            "fld_chances",
        ]
        placeholders = ", ".join("%s" for _ in cols)
        col_list = ", ".join(cols)
        upsert_sql = f"""
            INSERT INTO player_rolling_stats ({col_list})
            VALUES ({placeholders})
            ON CONFLICT (player_id, as_of_date, window_days)
            DO UPDATE SET
                bat_games_played = EXCLUDED.bat_games_played,
                bat_plate_appearances = EXCLUDED.bat_plate_appearances,
                bat_at_bats = EXCLUDED.bat_at_bats,
                bat_runs = EXCLUDED.bat_runs,
                bat_hits = EXCLUDED.bat_hits,
                bat_doubles = EXCLUDED.bat_doubles,
                bat_triples = EXCLUDED.bat_triples,
                bat_home_runs = EXCLUDED.bat_home_runs,
                bat_rbi = EXCLUDED.bat_rbi,
                bat_strike_outs = EXCLUDED.bat_strike_outs,
                bat_base_on_balls = EXCLUDED.bat_base_on_balls,
                bat_stolen_bases = EXCLUDED.bat_stolen_bases,
                bat_caught_stealing = EXCLUDED.bat_caught_stealing,
                bat_avg = EXCLUDED.bat_avg,
                bat_ops = EXCLUDED.bat_ops,
                bat_woba = EXCLUDED.bat_woba,
                bat_wrc_plus = EXCLUDED.bat_wrc_plus,
                pit_games_played = EXCLUDED.pit_games_played,
                pit_innings_pitched = EXCLUDED.pit_innings_pitched,
                pit_wins = EXCLUDED.pit_wins,
                pit_losses = EXCLUDED.pit_losses,
                pit_saves = EXCLUDED.pit_saves,
                pit_hits = EXCLUDED.pit_hits,
                pit_earned_runs = EXCLUDED.pit_earned_runs,
                pit_strike_outs = EXCLUDED.pit_strike_outs,
                pit_base_on_balls = EXCLUDED.pit_base_on_balls,
                pit_era = EXCLUDED.pit_era,
                pit_fip = EXCLUDED.pit_fip,
                pit_whip = EXCLUDED.pit_whip,
                fld_assists = EXCLUDED.fld_assists,
                fld_put_outs = EXCLUDED.fld_put_outs,
                fld_errors = EXCLUDED.fld_errors,
                fld_chances = EXCLUDED.fld_chances
        """
        with conn.cursor() as cur:
            for r in rolling_rows:
                cur.execute(
                    upsert_sql,
                    [r.get(c) for c in cols],
                )
        conn.commit()
        conn.close()
        return len(rolling_rows)

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
    load_ready_task = transform_player_stats_to_load_ready(
        cast(List[TransformedGameData], transformed_games),
        validated_stats,  # type: ignore[arg-type]
    )
    load_result = load_to_postgres(
        cast(List[TransformedGameData], transformed_games),
        load_ready_task,  # type: ignore[arg-type]
        conn_id="mlb_postgres",
    )
    # Rolling stats: reads fact_game_state + dim_game from DB after load
    compute_and_load_rolling_stats(conn_id="mlb_postgres").set_upstream(load_result)


# Instantiate the DAG [13]
mlb_player_stats_pipeline()
