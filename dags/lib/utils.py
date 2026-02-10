import pendulum


def normalize_date(date: str) -> str:
    return pendulum.from_format(
        date, "YYYY-MM-DD:HH:mm:ss", tz="UTC"
    ).to_datetime_string()
