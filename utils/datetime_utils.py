from datetime import datetime, timezone

def parse_unix_timestamp_in_ms_to_datetime(unix_timestamp):
    return datetime.fromtimestamp(unix_timestamp / 1000, tz=timezone.utc)