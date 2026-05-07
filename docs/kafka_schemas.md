# Kafka Topic Value Schemas

This is the wire-format contract for every Kafka topic in the pipeline.

All values are **UTF-8 JSON** (`json.dumps(...).encode("utf-8")` in the
producer; `CAST(value AS STRING)` + `from_json(...)` on the Spark side).
Keys are UTF-8 strings — usually `route_id` for routing visibility, or the
literal `"nyc"` for `weather-feed`.

Producer: `MessageToDict(..., preserving_proto_field_name=True)` is used for
GTFS-Realtime payloads, so all field names match the Protobuf field names
exactly — **snake_case**, not camelCase. Adding `use_integers_for_enums=False`
would change `current_status` from a string to an integer; we deliberately
keep the string form.

---

## `gtfs-vehicle`

One record per VehiclePosition entity, emitted every 15s per feed.
12 partitions, routing by `ROUTE_PARTITION_MAP` (see [kafka_config.md](kafka_config.md)).

```json
{
  "feed": "main",
  "feed_timestamp": 1714060800,
  "entity_id": "GTFS-RT-VP-1",
  "route_id": "1",
  "trip": {
    "trip_id": "078650_1..S03R",
    "route_id": "1",
    "start_date": "20240426",
    "start_time": "08:30:00"
  },
  "vehicle": { "id": "MTA-1234" },
  "stop_id": "127N",
  "current_status": "STOPPED_AT",
  "current_stop_sequence": 8,
  "timestamp": 1714060812
}
```

### Field reference

| Field | Type | Required | Notes |
|---|---|---|---|
| `feed` | string | yes | `ace`, `bdfm`, `g`, `jz`, `nqrw`, `l`, `main`, `si`, or `synthetic` |
| `feed_timestamp` | int (epoch s) | yes | header timestamp from the GTFS-Realtime payload |
| `entity_id` | string | yes | unique within a feed snapshot |
| `route_id` | string | maybe | extracted from VehiclePosition or its trip; null on rare malformed entries |
| `trip.trip_id` | string | yes | join key with `gtfs-trips` |
| `trip.route_id` | string | yes | usually equal to top-level `route_id` |
| `trip.start_date` | string | yes | `YYYYMMDD` |
| `trip.start_time` | string | yes | `HH:MM:SS` (service date — can exceed 23:59:59 for late-night service) |
| `vehicle.id` | string | maybe | optional in the spec |
| `stop_id` | string | yes | direction-suffixed (`127N` / `127S`); strip `[NS]$` before bridge join |
| `current_status` | string | maybe | enum: `INCOMING_AT`, `STOPPED_AT`, `IN_TRANSIT_TO` |
| `current_stop_sequence` | int | maybe | 1-indexed |
| `timestamp` | int (epoch s) | maybe | per-vehicle update time; falls back to `feed_timestamp` |

The Spark consumer's authoritative parse schema is in
[`processing/gtfs_vehicle_consumer.py`](../processing/gtfs_vehicle_consumer.py)
(`VEHICLE_SCHEMA`). Match it exactly when adding new consumers.

---

## `gtfs-trips`

One record per TripUpdate entity. The `stop_time_update` array is **not**
exploded at the producer — that happens in
[`processing/gtfs_trips_consumer.py`](../processing/gtfs_trips_consumer.py).

```json
{
  "feed": "main",
  "feed_timestamp": 1714060800,
  "entity_id": "GTFS-RT-TU-1",
  "route_id": "1",
  "trip": {
    "trip_id": "078650_1..S03R",
    "route_id": "1",
    "start_date": "20240426",
    "start_time": "08:30:00"
  },
  "stop_time_update": [
    {
      "stop_id": "127N",
      "stop_sequence": 8,
      "arrival":   { "time": 1714060920, "delay": 120 },
      "departure": { "time": 1714060950, "delay": 120 }
    }
  ],
  "timestamp": 1714060812
}
```

### Field reference

| Field | Type | Required | Notes |
|---|---|---|---|
| `feed`, `feed_timestamp`, `entity_id`, `route_id`, `trip.*` | — | yes | identical semantics to `gtfs-vehicle` |
| `stop_time_update[]` | array | yes | one element per scheduled stop in the trip |
| `stop_time_update[].stop_id` | string | yes | direction-suffixed |
| `stop_time_update[].stop_sequence` | int | maybe | aligns with `current_stop_sequence` from VehiclePosition |
| `stop_time_update[].arrival.time` | int (epoch s) | maybe | predicted arrival |
| `stop_time_update[].arrival.delay` | int (seconds) | **the source of truth for "lateness"** | negative = early, 0 = on time, positive = late. Do NOT compute `actual − scheduled` deltas. |
| `stop_time_update[].departure.time` | int (epoch s) | maybe | |
| `stop_time_update[].departure.delay` | int (seconds) | maybe | retained as a coalesce fallback if `arrival.delay` proves unreliable |
| `timestamp` | int (epoch s) | maybe | feed-level update time |

Authoritative parse schema:
[`processing/gtfs_trips_consumer.py`](../processing/gtfs_trips_consumer.py)
(`TRIP_UPDATE_SCHEMA` and `STOP_TIME_UPDATE_SCHEMA`).

---

## `gtfs-alerts`

One record per Alert entity. 2 partitions; routing follows the same
`ROUTE_PARTITION_MAP` modulo 2. Currently consumed only by `gtfs_monitor.py`;
The dashboard can surface alerts in the banner.

```json
{
  "feed": "main",
  "feed_timestamp": 1714060800,
  "entity_id": "lmm:alert:60",
  "route_id": "1",
  "active_period": [{ "start": 1714060800, "end": 1714075200 }],
  "informed_entity": [
    { "route_id": "1", "stop_id": "127N" }
  ],
  "cause": "TECHNICAL_PROBLEM",
  "effect": "SIGNIFICANT_DELAYS",
  "header_text":      { "translation": [{ "text": "1 trains delayed", "language": "en" }] },
  "description_text": { "translation": [{ "text": "...", "language": "en" }] }
}
```

The full Protobuf for Alert has many optional fields (`url`,
`severity_level`, `image`, `tts_*`, `transit_realtime_extension` etc.). The
producer flattens all of them via `MessageToDict`; consumers should treat
unknown fields as ignorable.

---

## `gtfs-dlq`

Failure envelope. 2 partitions, key = feed name.

```json
{
  "feed": "ace",
  "error": "parse_error: Error parsing message",
  "received_at": 1714060800,
  "raw_b64": "Cg0KC2dyb3VwLTEtdmVo..."
}
```

### Field reference

| Field | Type | Notes |
|---|---|---|
| `feed` | string | which MTA feed produced the error |
| `error` | string | one of: `fetch_failed`, `parse_error: ...`, `send_error: ...` |
| `received_at` | int (epoch s) | when the producer caught the failure |
| `raw_b64` | string \| null | base64 of the raw Protobuf payload (null for fetch failures) |

Operator review: consume the DLQ, categorize errors, and patch any recurring
deserialization issues in the producer.

---

## `weather-feed`

One record per NWS poll, ~every 15 min. **Compacted** topic — only the
latest record per key (`"nyc"`) is retained long-term.

```json
{
  "timestamp": "2026-05-05T19:23:39.593000+00:00",
  "weather_bucket": "clear",
  "prcp_in": 0.0,
  "snow_in": 0.0,
  "tmax_f": 78.08
}
```

### Field reference

| Field | Type | Notes |
|---|---|---|
| `timestamp` | ISO-8601 string (UTC) | when the NWS observation was processed |
| `weather_bucket` | string | one of `"clear"`, `"rain"`, `"snow"` — same set as the historical batch path |
| `prcp_in` | double | precipitation in last hour, inches |
| `snow_in` | double | inferred snow accumulation, inches (nonzero only when `weather_bucket = "snow"`) |
| `tmax_f` | double \| null | observed temperature, °F |

The same payload (minus the `timestamp` wrapper field name) is upserted into
the Cassandra `current_weather` table on the same cycle. The speed layer
reads Cassandra; `weather-feed` exists for replay/debug.

---

## Summary of join keys

| Stream A | Stream B | Join keys | Window |
|---|---|---|---|
| `gtfs-vehicle` (after bridge join) | `gtfs-trips` (exploded) | `(trip_id, stop_id)` | 5-min watermark, 10-min sliding agg, 30s slide |
| Speed-layer delays | `station_capacity_baseline` (Cassandra, broadcast) | `(station_complex_id, day_of_week, hour_of_day, weather_bucket)` | per micro-batch |
| Speed-layer delays | `station_max_entries` (Cassandra, broadcast) | `station_complex_id` | per micro-batch |

The first row is the staging contract output. The second and third rows are
used by `lambda_merge.py` and are listed for context only.
