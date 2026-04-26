# Track C — Storage, Serving & Dashboard: Design Document

**Owner:** Yash Jain  
**Last updated:** Week 2

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

## Serving layer (`serving/db_clients.py`)

A small module that owns all database connections. Both the FastAPI app (Week 5) and any scripts that need DB access import from here — nothing else creates its own connections.

**Why module-level singletons?** Streamlit reruns the entire script on every user interaction. Without lazy-init singletons, every button click would open a new MongoDB and Cassandra connection. The module-level globals (`_mongo_client`, `_cassandra_cluster`) are created once on first call and reused. `close_connections()` is provided for clean shutdown in tests or scripts.

Connection strings are read from environment variables (`MONGO_URI`, `CASSANDRA_HOSTS`) so the same code works locally and in Docker without changes.

---

## Dashboard (`dashboard/app.py`)

Currently a scaffold: a PyDeck `ScatterplotLayer` over NYC with 5 hardcoded test stations. The hardcoded stations will be replaced with real coordinates from the bridge table parquet (blocked on Preyansh's Week 2 handoff — see C2.2 in the execution plan).

**Why PyDeck?** It renders WebGL maps inside Streamlit with a single function call and supports the `ScatterplotLayer` → `get_fill_color` pattern we need for green/yellow/red congestion markers. Folium was the alternative but requires HTML embedding and doesn't compose as cleanly with Streamlit's reactive model.

**Planned evolution of `app.py`:**
- Week 3: poll MongoDB every 30s via `streamlit-autorefresh`
- Week 5: all data reads go through FastAPI (no direct DB calls in dashboard)
- Week 6: color-coded markers driven by `alert_level`
- Week 7: time-series chart, subway line filter
- Week 8: forecast widget, alert banner, freshness indicator

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
            FastAPI (Week 5)
                  │
                  ▼
            Streamlit dashboard
```

The Lambda merge (Preyansh's `lambda_merge.py`) is the only writer to MongoDB. Cassandra has two writers: Preyansh's batch jobs write the baseline tables; Arjun's NWS poller writes `current_weather`. Track C is read-only at runtime — it never writes to either database.

---

## Setup

```bash
# Start databases
cd infra && docker compose up -d

# First-time only: create MongoDB TTL index
python infra/init_mongo.py

# Verify both connections
python infra/verify_connections.py

# Apply Cassandra schema (idempotent — safe to re-run)
docker exec -i infra-cassandra-1 cqlsh localhost < infra/cassandra_schema.cql

# Run dashboard
streamlit run dashboard/app.py
```
