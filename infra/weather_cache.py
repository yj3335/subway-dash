from __future__ import annotations

import time
from datetime import datetime, timezone

from serving.db_clients import get_cassandra_session


_TTL_SECS = 15 * 60
_STALE_SECS = 60 * 60
_cache = {"bucket": "clear", "fetched_at": 0.0}


def get_current_weather_bucket(*, force_refresh: bool = False) -> str:
    """Return the cached current NYC weather bucket from Cassandra.

    Missing, stale, invalid, or unreachable Cassandra data fails closed to
    "clear" so Lambda scoring does not block on the real-time weather poller.
    """
    now = time.time()
    if not force_refresh and now - float(_cache["fetched_at"]) < _TTL_SECS:
        return str(_cache["bucket"])

    bucket = "clear"
    try:
        session = get_cassandra_session()
        row = session.execute(
            "SELECT weather_bucket, updated_at FROM subway_dash.current_weather WHERE scope = 'nyc'"
        ).one()
        if row and row.weather_bucket in {"clear", "rain", "snow"} and _is_fresh(row.updated_at, now):
            bucket = row.weather_bucket
    except Exception as exc:  # noqa: BLE001 - cache must fail safe in streaming jobs.
        print(f"weather cache defaulting to clear: {exc}")

    _cache["bucket"] = bucket
    _cache["fetched_at"] = now
    return bucket


def _is_fresh(updated_at: datetime | None, now_epoch: float) -> bool:
    if updated_at is None:
        return False
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    return now_epoch - updated_at.timestamp() < _STALE_SECS

