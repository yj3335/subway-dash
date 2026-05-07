# Storage, Serving & Dashboard Design

## Scope

This document covers everything downstream of the processing layer:

- The database schemas (Cassandra + MongoDB)
- The FastAPI serving layer that sits between the databases and the dashboard
- The Streamlit dashboard itself

Live ingestion feeds data into Kafka. PySpark jobs compute congestion scores and write them to the databases. The serving and dashboard layers provide the schemas, APIs, and visualizations that surface that data to users.

---

## Storage design

Two databases are used deliberately — they serve different access patterns.

### MongoDB — speed layer (`subway_dash.speed_layer`)

Holds the live congestion documents written by `lambda_merge.py` every 30 seconds.

**Why MongoDB here?** The speed layer writes one document per station per micro-batch and the dashboard reads the single latest document per station. MongoDB handles this with a simple `sort + limit 1` query. There's no relational structure, no aggregation across rows — it's a document store used exactly as intended.

A 24-hour TTL index on `inserted_at` automatically expires old documents. Without it the collection grows unboundedly. The index is created by `infra/init_mongo.py` (run once after first startup).

See `docs/mongodb_schema.md` for the full field-level spec.

### Cassandra — batch layer (`subway_dash` keyspace)

Holds the permanent batch views computed by PySpark batch jobs.

**Why Cassandra here?** The batch baseline is read in a tight pattern: look up a specific `(station_complex_id, weather_bucket, day_of_week, hour_of_day)` cell. Cassandra's partition key is designed for exactly this — a known key → one row. Read latency is consistently low regardless of table size, which matters for the speed-layer micro-batch that broadcast-joins against this table every 30 seconds.

**Four tables and why each exists:**

| Table | Written by | Read by | Purpose |
|---|---|---|---|
| `station_capacity_baseline` | Batch processing | Speed layer, dashboard forecast | Expected entries per `(station, day, hour, weather)` cell |
| `station_max_entries` | Batch processing | Speed layer | Per-station historical peak — denominator for `demand_intensity` normalization |
| `current_weather` | NWS poller | Speed layer | Single-row table holding current NYC weather bucket. Kept in Cassandra so the speed layer can read it without touching an external API on every micro-batch |
| `weather_coefficients` | Batch processing | Analytics report | Historical rain/snow ridership multipliers. Not used at runtime — analytics artifact only |

**Why `station_capacity_baseline` has a composite partition key `(station_complex_id, weather_bucket)`:**  
Weather bucket is included in the partition key because the two most common query patterns are "give me this station's baseline for clear weather" and "give me this station's baseline for rain". Putting `weather_bucket` in the partition key means each weather variant for a station lives on the same node, avoiding scatter-gather reads across the cluster.

---

## Serving layer

### `serving/db_clients.py`

Owns all database connections. Both the FastAPI app and any scripts that need DB access import from here — nothing else creates its own connections.

**Why module-level singletons?** Streamlit reruns the entire script on every user interaction. Without lazy-init singletons, every refresh would open a new MongoDB and Cassandra connection. The module-level globals (`_mongo_client`, `_cassandra_cluster`) are created once on first call and reused. `close_connections()` is provided for clean shutdown in tests or scripts.

Connection strings are read from environment variables (`MONGO_URI`, `CASSANDRA_HOSTS`) so the same code works locally and in Docker without changes.

### `serving/main.py` — FastAPI app

Six routes:

| Route | Method | Description |
|---|---|---|
| `/health` | GET | Pings MongoDB and Cassandra; returns `{"status":"ok"}` or `{"status":"degraded"}` with per-DB details |
| `/api/v1/station/{id}/congestion` | GET | Latest `speed_layer` document for one station; 404 if no data |
| `/api/v1/station/{id}/history` | GET | Last 24h of `speed_layer` docs for one station, sorted by `event_timestamp` asc. `?hours=N` overrides the window |
| `/api/v1/station/{id}/forecast` | GET | Next-hour capacity forecast from Cassandra baseline; returns `avg_entries`, `p95_entries` for `(station, clear, dow, next_hour)` |
| `/api/v1/stations/all` | GET | Latest document per station (MongoDB aggregation pipeline: sort → group by station → first). **12-second TTL in-memory cache** reduces MongoDB load on every autorefresh cycle |
| `/api/v1/alerts` | GET | Same as `/stations/all` but filtered to `alert_level ∈ {MODERATE, SEVERE}` |

All routes exclude `_id` from responses; `datetime` fields are serialized to ISO-8601 strings.

Start with: `uvicorn serving.main:app --reload`

---

## Dashboard (`dashboard/app.py`)

### Current state

- **MTA dark theme:** `.streamlit/config.toml` sets `primaryColor=#0039A6`, `backgroundColor=#0E1117`, matching MTA brand guidelines.
- **MTA branded header:** Full-width dark-blue header bar with the SUBWAY DASH wordmark.
- **Station map:** 445 stations on a CARTO dark-matter basemap with `pitch=30` for a 3D tilt. Marker radius uses a linear curve `100 + score × 300` clipped to `[100, 400]`. Color encodes alert level: green = NORMAL, orange = MODERATE, red = SEVERE, grey = no data.
- **HTML tooltip:** Rich popup with MTA line badges (colored per official line palette), alert level, a rider-friendly crowding label (Quiet / Moderate / Busy / Very Busy), and delay formatted in minutes (e.g. "~2 min").
- **Station table:** Shows crowding label and delay in minutes instead of raw numeric values; sorted SEVERE-first.
- **KPI tiles:** Station count, SEVERE/MODERATE/NORMAL counts, last-updated age — styled with colored left-border accents.
- **Threshold reference lines:** Altair history chart shows MODERATE (0.20) and SEVERE (0.50) reference lines so the trend is readable against the scale.
- **Toast escalation:** `st.toast()` fires when any station transitions into SEVERE, tracked via `st.session_state["prev_alerts"]`.
- **System health strip:** Inline status indicators for MongoDB, Cassandra, and API — surfaced below the header so operators can diagnose connectivity without leaving the page.
- **Freshness indicator:** Sidebar shows last-fetch age; turns orange with a warning if data is >90 seconds stale.
- **30-second autorefresh:** via `streamlit-autorefresh`. Bridge parquet cached with `@st.cache_data`; API calls run every cycle.
- **Sidebar line filter:** `st.multiselect` over `daytime_routes`; filters map markers and table rows.
- **24h history tab:** Altair chart of `congestion_score` over the past 24 hours with threshold lines.
- **Next-hour forecast tab:** Calls `/api/v1/station/{id}/forecast` backed by Cassandra baseline.

### Why all DB reads go through FastAPI

The dashboard does not query databases directly. All speed-layer reads go through the `/stations/all`, `/station/{id}/congestion`, `/station/{id}/history`, and `/alerts` endpoints. Baseline reads use the `/station/{id}/baseline` endpoint (full 24-hour profile) and `/station/{id}/forecast` (next-hour point estimate). No direct MongoDB or Cassandra calls from the dashboard.

**Why PyDeck?** It renders WebGL maps inside Streamlit with a single function call and supports the `ScatterplotLayer` → `get_fill_color` pattern needed for colored markers. The CARTO dark-matter style is loaded via a direct style URL, keeping the map visually consistent with the MTA dark theme.

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
Kafka + GTFS producer
        │
        ▼
PySpark batch + speed layer
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

The Lambda merge (`lambda_merge.py`) is the only writer to MongoDB. Cassandra has two writers: batch jobs write the baseline tables, and the NWS poller writes `current_weather`. The dashboard is read-only at runtime; it never writes to either database.

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
