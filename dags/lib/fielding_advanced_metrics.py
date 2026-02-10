def calculate_fielding_runs(
    assists: int,
    errors: int,
    chances: int,
) -> float:
    return (assists + errors) / chances
