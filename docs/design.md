# Track C — Storage, Serving & Dashboard: Design Document

**Owner:** Yash Jain  
**Last updated:** Week 9

---

## What Track C owns

Track C is responsible for everything downstream of the processing layer:

- The database schemas (Cassandra + MongoDB)
- The FastAPI serving layer that sits between the databases and the dashboard
- The Streamlit dashboard itself

Track A (Arjun) feeds data into Kafka. Track B (Preyansh) runs the PySpark jobs that compute congestion scores and write them to the databases. Track C's job is to make sure the right schemas are there to receive that data, and to surface it to users.

---

## Storage design

Two databases are used deliberately — they serve different access patterns.

### MongoDB — speed layer (`subway_dash.speed_layer`)

Holds the live congestion documents written by Preyansh's `lambda_merge.py` every 30 seconds.

**Why MongoDB here?** The speed layer writes one document per station per micro-batch and the dashboard reads the single latest document per station. MongoDB handles this with a simple `sort + limit 1` query. There's no relational structure, no aggregation across rows — it's a document store used exactly as intended.

A 24-hour TTL index on `inserted_at` automatically expires old documents. Without it the collection grows unboundedly. The index is created by `infra/init_mongo.py` (run once after first startup).

See `docs/mongodb_schema.md` for the full field-level spec.

### Cassandra — batch layer (`subway_dash` keyspace)

Holds the permanent batch views computed by Preyansh's nightly PySpark jobs.

**Why Cassandra here?** The batch baseline is read in a tight pattern: look up a specific `(station_complex_id, weather_bucket, day_of_week, hour_of_day)` cell. Cassandra's partition key is designed for exactly this — a known key → one row. Read latency is consistently low regardless of table size, which matters for the speed-layer micro-batch that broadcast-joins against this table every 30 seconds.

**Four tables and why each exists:**

| Table | Written by | Read by | Purpose |
|---|---|---|---|
| `station_capacity_baseline` | Preyansh (nightly batch) | Speed layer, dashboard forecast | Expected entries per `(station, day, hour, weather)` cell |
| `station_max_entries` | Preyansh (nightly batch) | Speed layer | Per-station historical peak — denominator for `demand_intensity` normalization |
| `current_weather` | Arjun (NWS poller, every 15 min) | Speed layer | Single-row table holding current NYC weather bucket. Kept in Cassandra so the speed layer can read it without touching an external API on every micro-batch |
| `weather_coefficients` | Preyansh (Week 9, one-time) | Analytics report | Historical rain/snow ridership multipliers. Not used at runtime — analytics artifact only |

**Why `station_capacity_baseline` has a composite partition key `(station_complex_id, weather_bucket)`:**  
Weather bucket is included in the partition key because the two most common query patterns are "give me this station's baseline for clear weather" and "give me this station's baseline for rain". Putting `weather_bucket` in the partition key means each weather variant for a station lives on the same node, avoiding scatter-gather reads across the cluster.

---

## Serving layer

### `serving/db_clients.py`

Owns all database connections. Both the FastAPI app and any scripts that need DB access import from here — nothing else creates its own connections.

**Why module-level singletons?** Streamlit reruns the entire script on every user interaction. Without lazy-init singletons, every refresh would open a new MongoDB and Cassandra connection. The module-level globals (`_mongo_client`, `_cassandra_cluster`) are created once on first call and reused. `close_connections()` is provided for clean shutdown in tests or scripts.

Connection strings are read from environment variables (`MONGO_URI`, `CASSANDRA_HOSTS`) so the same code works locally and in Docker without changes.

### `serving/main.py` — FastAPI app

Five routes:

| Route | Method | Description |
|---|---|---|
| `/health` | GET | Pings MongoDB and Cassandra; returns `{"status":"ok"}` or `{"status":"degraded"}` with per-DB details |
| `/api/v1/station/{id}/congestion` | GET | Latest `speed_layer` document for one station; 404 if no data |
| `/api/v1/station/{id}/history` | GET | Last 24h of `speed_layer` docs for one station, sorted by `event_timestamp` asc. `?hours=N` overrides the window |
| `/api/v1/stations/all` | GET | Latest document per station (MongoDB aggregation pipeline: sort → group by station → first) |
| `/api/v1/alerts` | GET | Same as `/stations/all` but filtered to `alert_level ∈ {MODERATE, SEVERE}` |

All routes exclude `_id` from responses; `datetime` fields are serialized to ISO-8601 strings.

Start with: `uvicorn serving.main:app --reload`

---

## Dashboard (`dashboard/app.py`)

### Current state (Week 7 complete)

- **Station map:** 445 stations loaded from `data/bridge/station_bridge.parquet` (deduplicated on `station_complex_id`). Marker color is driven by `alert_level` from the live FastAPI response: green = NORMAL, yellow = MODERATE, red = SEVERE, grey = no data yet.
- **Hover tooltip:** station name, `alert_level`, `congestion_score`, `avg_arrival_delay_secs`.
- **30-second autorefresh:** via `streamlit-autorefresh`. The bridge parquet is cached with `@st.cache_data` (loaded once); the API call runs every refresh cycle.
- **Sidebar line filter:** `st.multiselect` driven by `daytime_routes` from the bridge table. Filters map markers to stations serving the selected lines.
- **Congestion table:** `st.dataframe` showing latest status per station from `/api/v1/stations/all`.
- **24h time-series expander:** station selector + line chart of `congestion_score` over the past 24 hours, calling `/api/v1/station/{id}/history`.
- **Cassandra round-trip section:** Times Sq-42 St hourly capacity baseline (clear weather) as a line chart. Queries Cassandra directly — no API route exists for this.

### Why all DB reads go through FastAPI (except the baseline chart)

The design principle from Week 5 is that the dashboard should not query databases directly. All speed-layer reads go through the FastAPI serving layer. The Times Sq baseline chart (C4.2) is an exception: it's a diagnostic section testing the Cassandra connection, and there's no API route for arbitrary station-hour baseline lookups.

**Why PyDeck?** It renders WebGL maps inside Streamlit with a single function call and supports the `ScatterplotLayer` → `get_fill_color` pattern needed for green/yellow/red markers. Folium was the alternative but requires HTML embedding and doesn't compose as cleanly with Streamlit's reactive model.

---

## Testing

### Synthetic speed-layer inject

To test the dashboard without running the full Kafka → Spark pipeline:

```bash
# Turn Times Sq red (SEVERE)
python -m ingestion.inject_speed_layer --station-id 611 --level SEVERE

# Turn Times Sq yellow (MODERATE)
python -m ingestion.inject_speed_layer --station-id 611 --level MODERATE
```

The dashboard will reflect the change within 30 seconds (one autorefresh cycle).

### Performance test

```bash
python -m unittest tests.test_dashboard_perf -v
```

Tests the local data operations (parquet read + pandas merge + line extraction) against time budgets. All pass at < 500ms combined, which is the local portion of the 2-second end-to-end target.

---

## How the pieces fit together

```
Arjun (Kafka + GTFS producer)
        │
        ▼
Preyansh (PySpark batch + speed layer)
        │                    │
        ▼                    ▼
  Cassandra             MongoDB
  (batch views)         (speed_layer, 24h TTL)
        │                    │
        └─────────┬──────────┘
                  ▼
            FastAPI (serving/main.py)
                  │
                  ▼
            Streamlit dashboard
```

The Lambda merge (`lambda_merge.py`) is the only writer to MongoDB. Cassandra has two writers: Preyansh's batch jobs write the baseline tables; Arjun's NWS poller writes `current_weather`. Track C is read-only at runtime — it never writes to either database.

---

## Setup

```bash
# Start full stack (Kafka, Spark, MongoDB, Cassandra)
cd infra && docker compose up -d && cd ..

# First-time only: create MongoDB TTL index
python infra/init_mongo.py

# Apply Cassandra schema (idempotent — safe to re-run)
docker exec subway_cassandra cqlsh -e "$(cat infra/cassandra_schema.cql)"

# Verify connections
python infra/verify_connections.py

# Start the serving layer
uvicorn serving.main:app --reload

# Start the dashboard (separate terminal)
streamlit run dashboard/app.py
```

---

## Completed

| Week | Task |
|---|---|
| W8 | ✅ Next-hour forecast widget. ✅ Alert banner (SEVERE only). ✅ Data freshness timestamp. ✅ KPI tiles. ✅ UX audit: sorted table, relative timestamps, `st.tabs`, legend caption, clean error messages. |
| W9 | ✅ MTA line branding in tooltips (`lines` column, "A · C · E" format). ✅ Loading spinners (history + forecast tabs). ✅ Pipeline-offline state with "last seen X ago" via `st.session_state`. |

## Remaining

| Task | Blocked on |
|---|---|
| 2-hour UAT with full team | Team availability |
| Cassandra baseline populate | Preyansh: `build_baseline.py --sink cassandra` |
| Empty-state recovery test (35s) | Full pipeline running |
| Demo video (cold start → SEVERE → recovery) | Baseline data for forecast demo |
| Final submission package | All of the above |
