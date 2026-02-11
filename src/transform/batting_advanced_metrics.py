from typing import cast

from dags.mlb_types import PlayerStats, TransformedBattingStats, TransformedGameData

from src.transform import constants


def calculate_woba(
    bb: int,
    hbp: int,
    hr: int,
    one_b: int,
    two_b: int,
    three_b: int,
    ibb: int,
    ab: int,
    sf: int,
    season: int,
) -> float:
    if ab == 0:
        return 0.0

    consts = constants.get(season)

    return (
        consts["wBB"] * bb
        + consts["wHBP"] * hbp
        + consts["w1B"] * one_b
        + consts["w2B"] * two_b
        + consts["w3B"] * three_b
        + consts["wHR"] * hr
    ) / (ab + bb - ibb + sf + hbp)


def calculate_wrc_plus(
    woba: float,
    pa: int,
    season: int,
) -> float:
    if pa == 0:
        return 0.0
    consts = constants.get(season)
    return ((woba - consts["wOBA"]) / consts["wOBAScale"]) + (consts["R/PA"]) * pa


def calculate_babip(
    h: int,
    hr: int,
    ab: int,
    k: int,
    sf: int,
) -> float:
    if ab == 0:
        return 0.0
    return (h - hr) / (ab - k - sf - hr)


def calculate_home_run_rate(
    hr: int,
    fb: int,
) -> float:
    if fb == 0:
        return 0.0
    return hr / fb


def calculate_obp(
    bb: int,
    hbp: int,
    one_b: int,
    two_b: int,
    three_b: int,
    hr: int,
    ab: int,
    ibb: int,
    sf: int,
) -> float:
    if ab == 0:
        return 0.0
    return (bb + hbp + one_b + two_b + three_b + hr) / (ab + bb - ibb + sf + hbp)


def calculate_slg(
    one_b: int,
    two_b: int,
    three_b: int,
    hr: int,
    ab: int,
) -> float:
    if ab == 0:
        return 0.0
    return (one_b + 2 * two_b + 3 * three_b + 4 * hr) / ab


def calculate_ops(
    obp: float,
    slg: float,
) -> float:
    if obp == 0 or slg == 0:
        return 0.0
    return obp + slg


def transform_batting_stats(
    player_stats: PlayerStats,
    game: TransformedGameData,
) -> TransformedBattingStats:
    """Transform batting stats for a given player."""
    if player_stats.get("batting"):
        enriched_stats = cast(TransformedBattingStats, player_stats.get("batting"))

        enriched_stats["woba"] = calculate_woba(
            enriched_stats["baseOnBalls"],
            enriched_stats["hitByPitch"],
            enriched_stats["homeRuns"],
            (
                enriched_stats["hits"]
                - enriched_stats["homeRuns"]
                - enriched_stats["doubles"]
                - enriched_stats["triples"]
            ),
            enriched_stats["doubles"],
            enriched_stats["triples"],
            enriched_stats["intentionalWalks"],
            enriched_stats["atBats"],
            enriched_stats["sacFlies"],
            int(game.season),
        )

        enriched_stats["wrc_plus"] = calculate_wrc_plus(
            enriched_stats["woba"],
            enriched_stats["plateAppearances"],
            int(game.season),
        )
        enriched_stats["ops"] = calculate_ops(
            calculate_obp(
                enriched_stats["baseOnBalls"],
                enriched_stats["hitByPitch"],
                enriched_stats["hits"],
                enriched_stats["doubles"],
                enriched_stats["triples"],
                enriched_stats["homeRuns"],
                enriched_stats["atBats"],
                enriched_stats["intentionalWalks"],
                enriched_stats["sacFlies"],
            ),
            calculate_slg(
                enriched_stats["hits"],
                enriched_stats["doubles"],
                enriched_stats["triples"],
                enriched_stats["homeRuns"],
                enriched_stats["atBats"],
            ),
        )
    return TransformedBattingStats(enriched_stats)
