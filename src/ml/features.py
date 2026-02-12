"""
Feature construction from player_rolling_stats for batter and pitcher models.

- Wide matrices: one row per (player_id, as_of_date) with _7 and _30 suffixed columns.
- Training data: join to fact_game_state targets (bat_woba / pit_fip) with as_of_date = game_date - 1.
"""

from typing import Any, cast

import pandas as pd

# Batting columns from player_rolling_stats (exclude player_id, as_of_date, window_days)
BATTER_ROLLING_COLS = [
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
    "pit_era",
    "pit_fip",
    "pit_whip",
]
# Pitching columns from player_rolling_stats
PITCHER_ROLLING_COLS = [
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
    "bat_games_played",
    "bat_plate_appearances",
    "bat_woba",
    "bat_wrc_plus",
]


def get_batter_feature_column_names() -> list[str]:
    """Column names for batter feature matrix in fixed order (matches _pivot_rolling_to_wide)."""
    return [f"{c}_{w}" for w in (7, 30) for c in BATTER_ROLLING_COLS]


def get_pitcher_feature_column_names() -> list[str]:
    """Column names for pitcher feature matrix in fixed order."""
    return [f"{c}_{w}" for w in (7, 30) for c in PITCHER_ROLLING_COLS]


def _pivot_rolling_to_wide(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    """Pivot long (player_id, as_of_date, window_days, cols) to wide (player_id, as_of_date, col_7, col_30)."""
    if df.empty:
        return pd.DataFrame()
    wide_parts = []
    for w in (7, 30):
        sub: pd.DataFrame = df.loc[
            df["window_days"] == w, ["player_id", "as_of_date"] + value_cols
        ].copy()
        sub = sub.rename(columns={c: f"{c}_{w}" for c in value_cols})
        wide_parts.append(sub)
    out = wide_parts[0]
    for part in wide_parts[1:]:
        out = out.merge(part, on=["player_id", "as_of_date"], how="outer")
    return out


def get_batter_features(conn: Any, as_of_date: Any) -> pd.DataFrame:
    """
    Return wide batter feature matrix for one as_of_date.
    Index: player_id. Columns: bat_*_7, bat_*_30 (and a few pit_* for two-way players).
    """
    cols_str = ", ".join(BATTER_ROLLING_COLS)
    sql = f"""
        SELECT player_id, as_of_date, window_days, {cols_str}
        FROM player_rolling_stats
        WHERE as_of_date = %s AND window_days IN (7, 30)
    """
    df = pd.read_sql(sql, conn, params=(as_of_date,))
    if df.empty:
        return pd.DataFrame()
    out = _pivot_rolling_to_wide(df, BATTER_ROLLING_COLS)
    out = out.set_index("player_id")
    return out


def get_pitcher_features(conn: Any, as_of_date: Any) -> pd.DataFrame:
    """
    Return wide pitcher feature matrix for one as_of_date.
    Index: player_id. Columns: pit_*_7, pit_*_30 (and a few bat_* for two-way players).
    """
    cols_str = ", ".join(PITCHER_ROLLING_COLS)
    sql = f"""
        SELECT player_id, as_of_date, window_days, {cols_str}
        FROM player_rolling_stats
        WHERE as_of_date = %s AND window_days IN (7, 30)
    """
    df = pd.read_sql(sql, conn, params=(as_of_date,))
    if df.empty:
        return pd.DataFrame()
    out = _pivot_rolling_to_wide(df, PITCHER_ROLLING_COLS)
    out = out.set_index("player_id")
    return out


def get_batter_features_date_range(
    conn: Any, min_as_of_date: Any, max_as_of_date: Any
) -> pd.DataFrame:
    """Wide batter features for all (player_id, as_of_date) in the date range."""
    cols_str = ", ".join(BATTER_ROLLING_COLS)
    sql = f"""
        SELECT player_id, as_of_date, window_days, {cols_str}
        FROM player_rolling_stats
        WHERE as_of_date BETWEEN %s AND %s AND window_days IN (7, 30)
    """
    df = pd.read_sql(sql, conn, params=(min_as_of_date, max_as_of_date))
    if df.empty:
        return pd.DataFrame()
    return _pivot_rolling_to_wide(df, BATTER_ROLLING_COLS)


def get_pitcher_features_date_range(
    conn: Any, min_as_of_date: Any, max_as_of_date: Any
) -> pd.DataFrame:
    """Wide pitcher features for all (player_id, as_of_date) in the date range."""
    cols_str = ", ".join(PITCHER_ROLLING_COLS)
    sql = f"""
        SELECT player_id, as_of_date, window_days, {cols_str}
        FROM player_rolling_stats
        WHERE as_of_date BETWEEN %s AND %s AND window_days IN (7, 30)
    """
    df = pd.read_sql(sql, conn, params=(min_as_of_date, max_as_of_date))
    if df.empty:
        return pd.DataFrame()
    return _pivot_rolling_to_wide(df, PITCHER_ROLLING_COLS)


def build_batter_training_data(
    conn: Any, min_date: Any, max_date: Any
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """
    Build (X, y, player_dates) for batter model.
    Only includes rows where the player had batting activity in that game (bat_plate_appearances > 0).
    Target: fact_game_state.bat_woba for that game. Features: rolling stats as_of game_date - 1.
    Drops rows with null target. player_dates has columns (player_id, game_date) for alignment.
    """
    sql_targets = """
        SELECT f.player_id, g.game_date, f.bat_woba
        FROM fact_game_state f
        JOIN dim_game g ON f.game_pk = g.game_pk
        WHERE g.game_date BETWEEN %s AND %s
          AND COALESCE(f.bat_plate_appearances, 0) > 0
          AND f.bat_woba IS NOT NULL
    """
    targets = pd.read_sql(sql_targets, conn, params=(min_date, max_date))
    if targets.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    targets["as_of_date"] = pd.to_datetime(targets["game_date"]) - pd.Timedelta(days=1)
    targets["as_of_date"] = targets["as_of_date"].dt.date

    min_ao = targets["as_of_date"].min()
    max_ao = targets["as_of_date"].max()
    features_wide = get_batter_features_date_range(conn, min_ao, max_ao)
    if features_wide.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    merged = targets.merge(
        features_wide,
        on=["player_id", "as_of_date"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    feature_cols = [
        c
        for c in merged.columns
        if c not in ("player_id", "game_date", "as_of_date", "bat_woba")
    ]
    X = merged[feature_cols]
    y = merged["bat_woba"]
    player_dates = merged[["player_id", "game_date"]].copy()
    return cast(pd.DataFrame, X), cast(pd.Series, y), cast(pd.DataFrame, player_dates)


def build_pitcher_training_data(
    conn: Any, min_date: Any, max_date: Any
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """
    Build (X, y, player_dates) for pitcher model.
    Only includes rows where the player pitched in that game (pit_innings_pitched > 0).
    Target: fact_game_state.pit_fip for that game. Features: rolling stats as_of game_date - 1.
    Drops rows with null target. player_dates has columns (player_id, game_date).
    """
    sql_targets = """
        SELECT f.player_id, g.game_date, f.pit_fip
        FROM fact_game_state f
        JOIN dim_game g ON f.game_pk = g.game_pk
        WHERE g.game_date BETWEEN %s AND %s
          AND COALESCE(f.pit_innings_pitched, 0) > 0
          AND f.pit_fip IS NOT NULL
    """
    targets = pd.read_sql(sql_targets, conn, params=(min_date, max_date))
    if targets.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    targets["as_of_date"] = pd.to_datetime(targets["game_date"]) - pd.Timedelta(days=1)
    targets["as_of_date"] = targets["as_of_date"].dt.date

    min_ao = targets["as_of_date"].min()
    max_ao = targets["as_of_date"].max()
    features_wide = get_pitcher_features_date_range(conn, min_ao, max_ao)
    if features_wide.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    merged = targets.merge(
        features_wide,
        on=["player_id", "as_of_date"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame(), pd.Series(dtype=float), pd.DataFrame()

    feature_cols = [
        c
        for c in merged.columns
        if c not in ("player_id", "game_date", "as_of_date", "pit_fip")
    ]
    X = merged[feature_cols]
    y = merged["pit_fip"]
    player_dates = merged[["player_id", "game_date"]].copy()
    return cast(pd.DataFrame, X), cast(pd.Series, y), cast(pd.DataFrame, player_dates)
