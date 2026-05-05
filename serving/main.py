from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

from serving.db_clients import get_cassandra_session, get_mongo_collection

app = FastAPI(title="Subway Dash API")

# Aggregation pipeline: latest document per station_complex_id
_LATEST_PER_STATION = [
    {"$sort": {"inserted_at": -1}},
    {"$group": {"_id": "$station_complex_id", "doc": {"$first": "$$ROOT"}}},
    {"$replaceRoot": {"newRoot": "$doc"}},
    {"$project": {"_id": 0}},
]


def _serialize(doc: dict[str, Any]) -> dict[str, Any]:
    return {k: v.isoformat() if isinstance(v, datetime) else v for k, v in doc.items()}


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
        sort=[("inserted_at", -1)],
    )
    if doc is None:
        raise HTTPException(status_code=404, detail=f"No data for station {station_id!r}")
    return _serialize(doc)


@app.get("/api/v1/stations/all")
def stations_all():
    col = get_mongo_collection("speed_layer")
    docs = [_serialize(d) for d in col.aggregate(_LATEST_PER_STATION)]
    return docs


@app.get("/api/v1/alerts")
def alerts():
    col = get_mongo_collection("speed_layer")
    pipeline = _LATEST_PER_STATION + [
        {"$match": {"alert_level": {"$in": ["MODERATE", "SEVERE"]}}}
    ]
    docs = [_serialize(d) for d in col.aggregate(pipeline)]
    return docs
