from mlb_types import TransformedFieldingStats, PlayerStats, TransformedGameData

from typing import cast


def calculate_fielding_runs(
    assists: int,
    errors: int,
    chances: int,
) -> float:
    if chances == 0:
        return 0.0
    return (assists + errors) / chances


# Export to ETL DAG
def transform_fielding_stats(
    player_stats: PlayerStats,
    game: TransformedGameData,
) -> TransformedFieldingStats:
    """
    Transform fielding stats for a given player [11].
    """
    enriched_stats = cast(TransformedFieldingStats, player_stats.get("fielding"))
    enriched_stats["fielding_runs"] = calculate_fielding_runs(
        enriched_stats["assists"],
        enriched_stats["errors"],
        enriched_stats["chances"],
    )

    return TransformedFieldingStats(enriched_stats)
