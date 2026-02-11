"""
Build load-ready player-game rows for fact_game_state.

Maps TransformedPlayerData (nested batting/pitching/fielding) plus context
to LoadReadyPlayerGame (flat keys matching schema columns).
"""

from typing import Any

from dags.mlb_types import LoadReadyPlayerGame, TransformedPlayerData


def _parse_innings_pitched(value: Any) -> float | None:
    """Parse innings pitched string (e.g. '5.1', '6') to float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def to_load_ready_row(
    game_pk: int,
    player_id: int,
    team_id: int,
    position_code: str,
    position_name: str,
    transformed: TransformedPlayerData,
) -> LoadReadyPlayerGame:
    """Flatten context + TransformedPlayerData into one LoadReadyPlayerGame row."""
    row: LoadReadyPlayerGame = {
        "game_pk": game_pk,
        "player_id": player_id,
        "team_id": team_id,
        "position_code": position_code,
        "position_name": position_name,
    }

    bat = transformed.get("batting")
    if bat and isinstance(bat, dict):
        b = bat
        row["bat_games_played"] = b.get("gamesPlayed")
        row["bat_runs"] = b.get("runs")
        row["bat_hits"] = b.get("hits")
        row["bat_doubles"] = b.get("doubles")
        row["bat_triples"] = b.get("triples")
        row["bat_home_runs"] = b.get("homeRuns")
        row["bat_strike_outs"] = b.get("strikeOuts")
        row["bat_base_on_balls"] = b.get("baseOnBalls")
        row["bat_at_bats"] = b.get("atBats")
        row["bat_plate_appearances"] = b.get("plateAppearances")
        row["bat_rbi"] = b.get("rbi")
        row["bat_stolen_bases"] = b.get("stolenBases")
        row["bat_caught_stealing"] = b.get("caughtStealing")
        row["bat_woba"] = b.get("woba")
        row["bat_wrc_plus"] = b.get("wrc_plus")
        row["bat_ops"] = (
            b.get("ops") if isinstance(b.get("ops"), (int, float)) else None
        )
        row["bat_babip"] = (
            b.get("babip") if isinstance(b.get("babip"), (int, float)) else None
        )
        row["bat_home_run_rate"] = b.get("home_run_rate")
        row["bat_fly_outs"] = b.get("flyOuts")
        row["bat_ground_outs"] = b.get("groundOuts")
        row["bat_air_outs"] = b.get("airOuts")
        row["bat_intentional_walks"] = b.get("intentionalWalks")
        row["bat_hit_by_pitch"] = b.get("hitByPitch")
        row["bat_ground_into_double_play"] = b.get("groundIntoDoublePlay")
        row["bat_total_bases"] = b.get("totalBases")
        row["bat_left_on_base"] = b.get("leftOnBase")
        row["bat_sac_bunts"] = b.get("sacBunts")
        row["bat_sac_flies"] = b.get("sacFlies")

    pit = transformed.get("pitching")
    if pit and isinstance(pit, dict):
        p = pit
        row["pit_games_played"] = p.get("gamesPlayed")
        row["pit_games_started"] = p.get("gamesStarted")
        row["pit_innings_pitched"] = (
            _parse_innings_pitched(p.get("inningsPitched")) or 0.0
        )
        row["pit_wins"] = p.get("wins")
        row["pit_losses"] = p.get("losses")
        row["pit_saves"] = p.get("saves")
        row["pit_hits"] = p.get("hits")
        row["pit_earned_runs"] = p.get("earnedRuns")
        row["pit_strike_outs"] = p.get("strikeOuts")
        row["pit_base_on_balls"] = p.get("baseOnBalls")
        row["pit_fip"] = p.get("fip")
        row["pit_babip"] = p.get("babip")
        row["pit_home_run_rate"] = p.get("home_run_rate")
        row["pit_batters_faced"] = p.get("battersFaced")
        row["pit_outs"] = p.get("outs")
        row["pit_holds"] = p.get("holds")
        row["pit_blown_saves"] = p.get("blownSaves")
        row["pit_save_opportunities"] = p.get("saveOpportunities")
        row["pit_pitches_thrown"] = p.get("pitchesThrown")
        row["pit_balls"] = p.get("balls")
        row["pit_strikes"] = p.get("strikes")
        row["pit_hit_batsmen"] = p.get("hitBatsmen")
        row["pit_balks"] = p.get("balks")
        row["pit_wild_pitches"] = p.get("wildPitches")
        row["pit_pickoffs"] = p.get("pickoffs")
        row["pit_inherited_runners"] = p.get("inheritedRunners")
        row["pit_inherited_runners_scored"] = p.get("inheritedRunnersScored")

    fld = transformed.get("fielding")
    if fld and isinstance(fld, dict):
        f = fld
        row["fld_assists"] = f.get("assists")
        row["fld_put_outs"] = f.get("putOuts")
        row["fld_errors"] = f.get("errors")
        row["fld_chances"] = f.get("chances")
        row["fld_fielding_runs"] = f.get("fielding_runs")
        row["fld_passed_ball"] = f.get("passedBall")
        row["fld_pickoffs"] = f.get("pickoffs")

    return row
