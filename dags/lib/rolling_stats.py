"""
Compute 7- and 30-day rolling stats for player_rolling_stats.

Consumes game-level rows (e.g. from fact_game_state joined with dim_game)
and produces one row per (player_id, as_of_date, window_days) with aggregates
and rate stats (bat_avg, bat_ops, bat_woba, bat_wrc_plus, pit_era, pit_fip, pit_whip).
"""

from datetime import date, timedelta
from decimal import Decimal
from typing import Any

import lib.constants as constants


# Default windows to compute
ROLLING_WINDOW_DAYS = (7, 30)


def _n(value: Any) -> float:
    """Coerce to float for arithmetic; None -> 0."""
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _int_val(value: Any) -> int:
    """Coerce to int for counts; None -> 0."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _bat_avg(hits: float, at_bats: float) -> float | None:
    if at_bats <= 0:
        return None
    return round(hits / at_bats, 4)


def _bat_ops_from_aggregates(
    bb: float,
    hbp: float,
    hits: float,
    doubles: float,
    triples: float,
    hr: float,
    ab: float,
    ibb: float,
    sf: float,
    total_bases: float,
) -> float | None:
    """OBP + SLG from aggregated counting stats."""
    if ab <= 0:
        return None
    pa_denom = ab + bb - ibb + sf + hbp
    if pa_denom <= 0:
        return None
    obp = (bb + hbp + hits) / pa_denom
    slg = total_bases / ab
    return round(obp + slg, 4)


def _bat_woba_from_aggregates(
    bb: float,
    hbp: float,
    hr: float,
    one_b: float,
    two_b: float,
    three_b: float,
    ibb: float,
    ab: float,
    sf: float,
    season: int,
) -> float | None:
    """wOBA from aggregated stats using seasonal constants."""
    denom = ab + bb - ibb + sf + hbp
    if denom <= 0:
        return None
    c = constants.get(season)
    woba = (
        c["wBB"] * bb
        + c["wHBP"] * hbp
        + c["w1B"] * one_b
        + c["w2B"] * two_b
        + c["w3B"] * three_b
        + c["wHR"] * hr
    ) / denom
    return round(woba, 4)


def _bat_wrc_plus_from_woba_pa(woba: float, pa: float, season: int) -> float | None:
    """wRC+ from wOBA and PA (same formula as batting_advanced_metrics)."""
    if pa <= 0:
        return None
    c = constants.get(season)
    return round(
        ((woba - c["wOBA"]) / c["wOBAScale"]) + (c["R/PA"]) * pa,
        2,
    )


def _pit_era(earned_runs: float, innings_pitched: float) -> float | None:
    if innings_pitched <= 0:
        return None
    return round(9.0 * earned_runs / innings_pitched, 2)


def _pit_fip_weighted_avg(fip_times_ip: float, ip: float) -> float | None:
    """IP-weighted average FIP (fact_game_state has pit_fip per game but not HR allowed)."""
    if ip <= 0:
        return None
    return round(fip_times_ip / ip, 2)


def _pit_whip(hits: float, walks: float, innings_pitched: float) -> float | None:
    if innings_pitched <= 0:
        return None
    return round((hits + walks) / innings_pitched, 2)


def compute_rolling_stats(
    game_rows: list[dict[str, Any]],
    as_of_dates: list[date] | None = None,
    window_days: tuple[int, ...] = ROLLING_WINDOW_DAYS,
) -> list[dict[str, Any]]:
    """
    Compute rolling aggregates and rate stats for each (player_id, as_of_date, window_days).

    game_rows: each dict must have at least:
      - player_id (int)
      - game_date (date)
      - season (int)
      - bat_* / pit_* / fld_* keys matching fact_game_state (values may be None).

    as_of_dates: dates to compute rolling stats for. If None, uses the max game_date
      present in game_rows.

    window_days: e.g. (7, 30). A game is included if game_date in (as_of_date - window_days, as_of_date].

    Returns list of dicts with keys matching player_rolling_stats table columns.
    """
    if not game_rows:
        return []

    if as_of_dates is None:
        max_date = max(r["game_date"] for r in game_rows if r.get("game_date"))
        as_of_dates = [max_date]

    result: list[dict[str, Any]] = []

    for as_of_date in as_of_dates:
        for window_d in window_days:
            window_start = as_of_date - timedelta(days=window_d)
            # Include games where window_start < game_date <= as_of_date
            in_window = [
                r
                for r in game_rows
                if r.get("game_date") and window_start < r["game_date"] <= as_of_date
            ]
            player_ids = {r["player_id"] for r in in_window}
            for player_id in player_ids:
                rows = [r for r in in_window if r["player_id"] == player_id]
                # Use latest game's season for rate constants
                season = max(
                    (r.get("season") or 0 for r in rows if r.get("season")),
                    default=2024,
                )

                # --- Batting aggregates ---
                bat_gp = sum(_int_val(r.get("bat_games_played")) for r in rows)
                bat_pa = sum(_int_val(r.get("bat_plate_appearances")) for r in rows)
                bat_ab = sum(_int_val(r.get("bat_at_bats")) for r in rows)
                bat_runs = sum(_int_val(r.get("bat_runs")) for r in rows)
                bat_hits = sum(_int_val(r.get("bat_hits")) for r in rows)
                bat_2b = sum(_int_val(r.get("bat_doubles")) for r in rows)
                bat_3b = sum(_int_val(r.get("bat_triples")) for r in rows)
                bat_hr = sum(_int_val(r.get("bat_home_runs")) for r in rows)
                bat_rbi = sum(_int_val(r.get("bat_rbi")) for r in rows)
                bat_so = sum(_int_val(r.get("bat_strike_outs")) for r in rows)
                bat_bb = sum(_int_val(r.get("bat_base_on_balls")) for r in rows)
                bat_sb = sum(_int_val(r.get("bat_stolen_bases")) for r in rows)
                bat_cs = sum(_int_val(r.get("bat_caught_stealing")) for r in rows)
                bat_ibb = sum(_int_val(r.get("bat_intentional_walks")) for r in rows)
                bat_hbp = sum(_int_val(r.get("bat_hit_by_pitch")) for r in rows)
                bat_sf = sum(_int_val(r.get("bat_sac_flies")) for r in rows)
                bat_tb = sum(_int_val(r.get("bat_total_bases")) for r in rows)
                bat_1b = bat_hits - bat_hr - bat_2b - bat_3b

                # --- Pitching aggregates ---
                pit_gp = sum(_int_val(r.get("pit_games_played")) for r in rows)
                pit_ip = sum(_n(r.get("pit_innings_pitched")) for r in rows)
                pit_w = sum(_int_val(r.get("pit_wins")) for r in rows)
                pit_l = sum(_int_val(r.get("pit_losses")) for r in rows)
                pit_sv = sum(_int_val(r.get("pit_saves")) for r in rows)
                pit_h = sum(_int_val(r.get("pit_hits")) for r in rows)
                pit_er = sum(_int_val(r.get("pit_earned_runs")) for r in rows)
                pit_so = sum(_int_val(r.get("pit_strike_outs")) for r in rows)
                pit_bb = sum(_int_val(r.get("pit_base_on_balls")) for r in rows)
                # FIP: use IP-weighted avg of game-level pit_fip (fact has no pit_home_runs)
                pit_fip_times_ip = sum(
                    _n(r.get("pit_fip")) * _n(r.get("pit_innings_pitched")) for r in rows
                )

                # --- Fielding aggregates ---
                fld_a = sum(_int_val(r.get("fld_assists")) for r in rows)
                fld_po = sum(_int_val(r.get("fld_put_outs")) for r in rows)
                fld_e = sum(_int_val(r.get("fld_errors")) for r in rows)
                fld_ch = sum(_int_val(r.get("fld_chances")) for r in rows)

                # Build output row (only include if player had any activity in window)
                if bat_gp == 0 and pit_gp == 0 and fld_ch == 0:
                    continue

                out: dict[str, Any] = {
                    "player_id": player_id,
                    "as_of_date": as_of_date,
                    "window_days": window_d,
                    "bat_games_played": bat_gp or None,
                    "bat_plate_appearances": bat_pa or None,
                    "bat_at_bats": bat_ab or None,
                    "bat_runs": bat_runs or None,
                    "bat_hits": bat_hits or None,
                    "bat_doubles": bat_2b or None,
                    "bat_triples": bat_3b or None,
                    "bat_home_runs": bat_hr or None,
                    "bat_rbi": bat_rbi or None,
                    "bat_strike_outs": bat_so or None,
                    "bat_base_on_balls": bat_bb or None,
                    "bat_stolen_bases": bat_sb or None,
                    "bat_caught_stealing": bat_cs or None,
                    "pit_games_played": pit_gp or None,
                    "pit_innings_pitched": round(pit_ip, 2) if pit_ip else None,
                    "pit_wins": pit_w or None,
                    "pit_losses": pit_l or None,
                    "pit_saves": pit_sv or None,
                    "pit_hits": pit_h or None,
                    "pit_earned_runs": pit_er or None,
                    "pit_strike_outs": pit_so or None,
                    "pit_base_on_balls": pit_bb or None,
                    "fld_assists": fld_a or None,
                    "fld_put_outs": fld_po or None,
                    "fld_errors": fld_e or None,
                    "fld_chances": fld_ch or None,
                }

                # Batting rates
                out["bat_avg"] = _bat_avg(bat_hits, bat_ab)
                out["bat_ops"] = _bat_ops_from_aggregates(
                    bat_bb, bat_hbp, bat_hits, bat_2b, bat_3b, bat_hr, bat_ab, bat_ibb, bat_sf, bat_tb
                )
                bat_woba = _bat_woba_from_aggregates(
                    bat_bb, bat_hbp, bat_hr, bat_1b, bat_2b, bat_3b, bat_ibb, bat_ab, bat_sf, season
                )
                out["bat_woba"] = bat_woba
                out["bat_wrc_plus"] = _bat_wrc_plus_from_woba_pa(bat_woba or 0.0, bat_pa, season) if bat_woba is not None else None

                # Pitching rates
                out["pit_era"] = _pit_era(pit_er, pit_ip)
                out["pit_fip"] = _pit_fip_weighted_avg(pit_fip_times_ip, pit_ip)
                out["pit_whip"] = _pit_whip(pit_h, pit_bb, pit_ip)

                result.append(out)

    return result
