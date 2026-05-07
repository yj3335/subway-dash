from __future__ import annotations

import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from serving.db_clients import (
    close_connections,
    get_cassandra_session,
    get_mongo_collection,
    reset_cassandra_session,
)

_NYC = ZoneInfo("America/New_York")

_stations_cache: tuple[float, list] | None = None
_STATIONS_CACHE_TTL = 12.0
_cache_lock = threading.Lock()

# Latest event window per station — used by /stations/all.
#
# `inserted_at` is when Lambda wrote the document, not when the subway signal
# happened. During backlog catch-up, old events can be inserted after newer
# events, so the dashboard should choose recency by `event_timestamp` first.
_LATEST_PER_STATION = [
    {"$sort": {"event_timestamp": -1, "inserted_at": -1}},
    {"$group": {"_id": "$station_complex_id", "doc": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$doc"}},
    {"$project": {"_id": 0}},
]

# Alerts pipeline — $match placed after $replaceRoot so it sees alert_level on
# the already-reduced (latest-per-station) documents, not raw collection docs.
# Sorted by congestion_score descending for deterministic ordering.
_ALERTS_PIPELINE = [
    {"$sort": {"event_timestamp": -1, "inserted_at": -1}},
    {"$group": {"_id": "$station_complex_id", "doc": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$doc"}},
    {"$match": {"alert_level": {"$in": ["MODERATE", "SEVERE"]}}},
    {"$sort": {"congestion_score": -1}},
    {"$project": {"_id": 0}},
]


def _serialize(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in doc.items()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    close_connections()


app = FastAPI(title="Subway Dash API", lifespan=lifespan)


@app.get("/health")
def health():
    status: dict[str, Any] = {}
    try:
        get_mongo_collection("speed_layer").database.client.admin.command("ping")
        status["mongo"] = "ok"
    except Exception as exc:
        status["mongo"] = str(exc)

    try:
        get_cassandra_session().execute("SELECT release_version FROM system.local")
        status["cassandra"] = "ok"
    except Exception as exc:
        status["cassandra"] = str(exc)

    ok = all(v == "ok" for v in status.values())
    return JSONResponse(
        content={"status": "ok" if ok else "degraded", **status},
        status_code=200 if ok else 503,
    )


@app.get("/api/v1/station/{station_id}/congestion")
def station_congestion(station_id: str):
    col = get_mongo_collection("speed_layer")
    doc = col.find_one(
        {"station_complex_id": station_id},
        {"_id": 0},
        sort=[("event_timestamp", -1), ("inserted_at", -1)],
    )
    if doc is None:
        raise HTTPException(status_code=404, detail=f"No data for station {station_id!r}")
    return _serialize(doc)


@app.get("/api/v1/stations/all")
def stations_all():
    global _stations_cache
    now = time.monotonic()
    with _cache_lock:
        if _stations_cache and (now - _stations_cache[0]) < _STATIONS_CACHE_TTL:
            return _stations_cache[1]
        col = get_mongo_collection("speed_layer")
        docs = [_serialize(d) for d in col.aggregate(_LATEST_PER_STATION, allowDiskUse=True)]
        _stations_cache = (now, docs)
        return docs


@app.get("/api/v1/station/{station_id}/history")
def station_history(station_id: str, hours: int = Query(default=24, ge=1, le=48)):
    col = get_mongo_collection("speed_layer")
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    docs = list(
        col.find(
            {"station_complex_id": station_id, "inserted_at": {"$gte": cutoff}},
            {"_id": 0, "event_timestamp": 1, "congestion_score": 1,
             "avg_arrival_delay_secs": 1, "alert_level": 1},
        ).sort("event_timestamp", 1)
    )
    return [_serialize(d) for d in docs]


@app.get("/api/v1/station/{station_id}/forecast")
def station_forecast(station_id: str):
    # Use NYC local time — baseline hour_of_day was recorded in NYC local time
    now = datetime.now(_NYC)
    next_hour = (now.hour + 1) % 24
    dow = now.isoweekday()
    try:
        session = get_cassandra_session()
        row = session.execute(
            "SELECT avg_entries, p95_entries FROM subway_dash.station_capacity_baseline "
            "WHERE station_complex_id = %s AND weather_bucket = 'clear' "
            "AND day_of_week = %s AND hour_of_day = %s",
            (station_id, dow, next_hour),
        ).one()
    except Exception as exc:
        reset_cassandra_session()
        raise HTTPException(status_code=503, detail=f"Cassandra unavailable: {exc}")
    if row is None:
        raise HTTPException(status_code=404, detail=f"No baseline for station {station_id!r}")
    return {
        "station_complex_id": station_id,
        "next_hour": next_hour,
        "day_of_week": dow,
        "avg_entries": row.avg_entries,
        "p95_entries": row.p95_entries,
    }


@app.get("/api/v1/station/{station_id}/baseline")
def station_baseline(station_id: str, weather_bucket: str = "clear"):
    """Full 24-hour hourly baseline for a station (used by the forecast tab)."""
    try:
        session = get_cassandra_session()
        rows = session.execute(
            "SELECT hour_of_day, avg_entries "
            "FROM subway_dash.station_capacity_baseline "
            "WHERE station_complex_id = %s AND weather_bucket = %s",
            (station_id, weather_bucket),
        )
    except Exception as exc:
        reset_cassandra_session()
        raise HTTPException(status_code=503, detail=f"Cassandra unavailable: {exc}")
    result = [{"hour_of_day": r.hour_of_day, "avg_entries": r.avg_entries} for r in rows]
    if not result:
        raise HTTPException(status_code=404, detail=f"No baseline for station {station_id!r}")
    return result


@app.get("/api/v1/alerts")
def alerts():
    col = get_mongo_collection("speed_layer")
    docs = [_serialize(d) for d in col.aggregate(_ALERTS_PIPELINE, allowDiskUse=True)]
    return docs
