from datetime import datetime, timezone


def parse_utc_timestamp(value: str | datetime) -> datetime:
    """Parse an event timestamp, treating naive values as UTC."""
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def exasol_timestamp_text(value: str | datetime) -> str:
    """Format an event timestamp as a UTC wall-clock value for Exasol TIMESTAMP."""
    return parse_utc_timestamp(value).replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
