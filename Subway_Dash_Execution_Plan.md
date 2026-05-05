# Subway Dash — Technical Execution Plan
**Project:** Real-Time MTA Congestion and Predictive Delay Pipeline  
**Team:** Arjun Bajpai (ab13092) · Preyansh Agrawal (pa2753) · Yash Jain (yj3335)  
**Architecture:** Lambda (Batch + Speed Layers)  
**Stack:** Apache Kafka · PySpark · MongoDB/Cassandra · Streamlit/Dash  

> **How to read this document:** Sections marked `[ASSUMPTION]` contain values or design decisions that are reasonable starting points but must be validated against real data during execution. Each one has a note on when and how to confirm it.  

---

## Team & Track Assignments

| Member | Track | Primary Responsibility |
|---|---|---|
| **Arjun Bajpai** | Track A | Data Ingestion & Infrastructure |
| **Preyansh Agrawal** | Track B | Processing, Joins & Prediction |
| **Yash Jain** | Track C | Storage, Serving Layer & Visualization |

---

## Weekly Execution Schedule

### Phase 1 — Foundation & Ingestion (Weeks 1–3)

| Week | Track A — Arjun (Ingestion/Infra) | Track B — Preyansh (Processing) | Track C — Yash (Storage/Viz) |
|---|---|---|---|
| **W1** | Provision local cluster via Docker Compose (Kafka + Zookeeper + Spark). Create the final-shape Kafka topics from day one: `gtfs-vehicle` (12 partitions), `gtfs-trips` (12 partitions), `gtfs-alerts`, `gtfs-dlq`, `weather-feed` (compacted). Verify broker health and produce a test message on each topic. | Download all static datasets: `stops.txt` (GTFS static), MTA Stations Dataset, MTA Hourly Ridership, NOAA historical weather CSVs. Profile schemas — document column names, dtypes, null rates, and row counts. | Stand up MongoDB (with 24-hour TTL index on `speed_layer.inserted_at`) and Cassandra locally. Design initial keyspace structure. Scaffold Streamlit app shell with an NYC subway basemap using PyDeck. |
| **W2** | Verify GTFS-Realtime feed access (no API key required since 2021). Write the Kafka Producer in Python. Poll MTA every 15s. Deserialize Protobuf using `gtfs-realtime-bindings`. Use `route_id` → 12-partition mapping (with `hash(route_id) % 12` fallback for unknown routes). | Build the **Station ID Bridge Table** (full methodology in Section 3.1). Write PySpark batch job to ingest and clean historical ridership CSVs — drop nulls, normalize timestamps to UTC, cast numeric columns. | Author Cassandra DDL for the 4 batch-side tables: `station_capacity_baseline`, `station_max_entries`, `current_weather`, `weather_coefficients`. Write first real Streamlit page: static station map with placeholder markers. |
| **W3** | Stress-test the Kafka pipeline. Confirm no message loss at sustained load. Implement DLQ message categorization. Set up the **NWS** real-time weather poller (15-min refresh) — upserts `current_weather` Cassandra row and publishes to `weather-feed`. NOAA CDO is used only for historical batch path. | Clean NOAA historical CSV (daily granularity). Write PySpark batch job that joins weather onto ridership on `date` only (NOAA is daily, not hourly). Persist output as `ridership_weather_baseline` Parquet. | Implement MongoDB document schema for speed-layer output (`station_complex_id`, `event_timestamp`, `avg_arrival_delay_secs`, `service_deficit`, `demand_intensity`, `congestion_score`, `predicted_delay_mins`, `alert_level`, `inserted_at`). Wire Streamlit to poll MongoDB every 30s using `streamlit-autorefresh`. |

**Phase 1 Exit Criteria**
- Kafka is streaming live GTFS data reliably with zero DLQ messages under normal load.
- Historical ridership and weather data are joined and persisted as Parquet.
- MongoDB and Cassandra are live with schemas confirmed.
- Streamlit shell is running and reading from MongoDB.

---

### Phase 2 — Joins, Analytics & Prediction (Weeks 4–7)

| Week | Track A — Arjun | Track B — Preyansh | Track C — Yash |
|---|---|---|---|
| **W4** | Build PySpark Structured Streaming consumer reading from `gtfs-vehicle`. Parse `VehiclePosition` Protobuf: extract `trip_id`, `stop_id`, `current_status`, `timestamp`. Apply the bridge broadcast join to attach `station_complex_id`. Sink to a `vehicle_positions` staging Parquet table. | **Batch Layer Core Job:** Execute the full Section 3.2 job. Outputs: `station_capacity_baseline` (avg/std/p95 entries per `(station, day, hour, weather)` cell) and `station_max_entries` (per-station historical peak — used to normalize `demand_intensity` in Week 6). | Integrate the Cassandra writer (`processing/cassandra_writer.py`) and round-trip a sample write/read against `station_capacity_baseline`. Render the real station map in Streamlit using bridge table coordinates. |
| **W5** | Add Kafka consumer for `gtfs-trips` (TripUpdate feed). Explode `stop_time_update` arrays and extract `arrival.delay` directly (not a manual `actual − scheduled` subtraction). Sink to a `trip_delays` staging table; validate Assumption A-16 (`arrival.delay` populated > 90% of rows). | **Speed Layer Core Job:** PySpark Structured Streaming watermark join on `(trip_id, stop_id)` within a 5-minute window combining `vehicle_positions` and `trip_delays`. Aggregate per `station_complex_id` with a 10-min sliding window / 30-second slide → `avg_arrival_delay_secs` stream. | Build the FastAPI serving layer exposing Cassandra (batch views) and MongoDB (speed-layer views). Implement `/api/v1/station/{id}/congestion`, `/api/v1/stations/all`, `/api/v1/alerts`, `/health`. Switch Streamlit to call FastAPI instead of querying databases directly. |
| **W6** | Audit the 12-partition layout (set in Week 1) under rush-hour load. Confirm `max_partition / median_partition` ≤ 3 and no LAG buildup. **No repartitioning event** — that would invalidate Spark checkpoints. | **Lambda Merge Job:** Implement `process_micro_batch` (Section 3.3) computing `service_deficit × demand_intensity` on the `[0, 1]` scale. Read `current_weather` via cached `get_current_weather_bucket()`. Write enriched docs to MongoDB `speed_layer`. | Wire Streamlit map to live FastAPI endpoints. Render color-coded markers: green (NORMAL) → yellow (MODERATE, score ≥ 0.20) → red (SEVERE, score ≥ 0.50). Add hover tooltip with `complex_name`, score, predicted delay, alert level. |
| **W7** | End-to-end pipeline smoke test. Inject a synthetic TripUpdate with `arrival.delay = 600s` for a Times Sq stop and confirm Streamlit reflects the change within 60 seconds. | **Threshold Back-Test:** Reconstruct inputs for ≥ 2 known historical congestion events, plug into Section 3.3 formulas, and verify `congestion_score` lands in the expected `[0.20, 0.50)` (MODERATE) and `≥ 0.50` (SEVERE) bands. Tighten or loosen thresholds; resolve Assumption A-06. | Add a time-series chart: 24-hour rolling `congestion_score` for a user-selected station. Add subway line filter via `st.multiselect` driven by `Daytime Routes`. All interactions update within 2 seconds. |

**Phase 2 Exit Criteria**
- Lambda merge is producing `congestion_score ∈ [0, 1]` values in near-real-time (< 60s latency).
- MODERATE / SEVERE thresholds are calibrated against ≥ 2 known historical events.
- Dashboard reflects live data end-to-end through the FastAPI serving layer.

---

### Phase 3 — Hardening, UI Polish & Delivery (Weeks 8–10)

| Week | Track A — Arjun | Track B — Preyansh | Track C — Yash |
|---|---|---|---|
| **W8** | Performance audit. Measure Kafka consumer lag and Spark streaming micro-batch duration. Target: micro-batch ≤ 25 s inside the 30 s trigger. Tune `spark.sql.shuffle.partitions` and executor memory. Measure skew ratio; apply salted join only if `max/median > 5×`. | Backfill batch jobs: reprocess 6–12 months of historical ridership to populate the full `station_capacity_baseline`. Validate against the raw NY Open Data hourly CSV for 5 benchmark stations (Times Sq, Grand Central, 34 St–Penn, Fulton, Atlantic Av). | Add next-hour forecast widget (queries baseline at `current_hour + 1`). Implement alert banner: SEVERE (score ≥ 0.50), MODERATE (≥ 0.20), all-clear. Add a "data freshness" timestamp tied to `inserted_at`. |
| **W9** | Write the final `docker-compose.yml` with health checks and persistent volumes — including `spark_checkpoints`, `spark_staging`, Cassandra, MongoDB, and Kafka data. Write `README.md` and `docs/kafka_config.md`. | Compute weather coefficients (`avg_entries_rain / avg_entries_clear`) for the historical analytics. Calibrate `service_deficit` endpoints (60s grace, 300s saturation) against empirical `arrival_delay_secs` percentiles — resolves Assumption A-07. Draft Analytics Report through Section 8. | Run a 2-hour user acceptance test. Log latency spikes, broken map renders, stale-data incidents. Fix all P0 bugs before Week 10. Test empty-state handling (producer down → dashboard recovers within 35 s of restart). |
| **W10** | Cold start test (target: full stack live within 8 minutes from `docker-compose up`). 24-hour unattended health check. Tag final release `v1.0.0`. | Complete Analytics Report (sections 9–10: performance results, limitations, lessons learned). Audit all 16 assumptions and document final status (CONFIRMED / REVISED / UNRESOLVED) as a report appendix. | Final Streamlit polish: MTA line branding in tooltips, loading spinners, empty-state handling. Record a 3-minute demo video showing cold start → normal state → synthetic SEVERE event → recovery. Assemble final submission package. |

**Phase 3 Exit Criteria**
- All three deliverables are complete: pipeline running, dashboard live, analytics report written.
- `docker-compose up` launches the full stack from a cold start.
- Demo video captures at least one live congestion event.

---

## Section 3 — Technical Roadmap: Core PySpark Jobs

### 3.1 The Spatial Join Problem — GTFS Stop ID → Historical Station ID

**The mismatch:** GTFS-Realtime feeds identify train positions using atomic `stop_id` values like `127N` (northbound platform) and `127S` (southbound platform). The MTA Hourly Ridership dataset aggregates tap counts under a `station_complex_id` (e.g., `613` = "Times Sq–42 St") that groups multiple platforms and lines into one logical station. A direct key join fails because these ID spaces are completely different.

**Resolution: 3-Tier Fallback**

> `[ASSUMPTION]` The coverage split across the three tiers is estimated as ~92% / ~6% / ~2%. These percentages are not empirically verified — they are informed estimates based on the MTA Stations Dataset structure. **Action (Week 2):** After Preyansh runs the initial Tier 1 join against the actual datasets, log the null rate. If it exceeds 15%, revisit the Levenshtein threshold in Tier 2 or expand the geospatial radius in Tier 3.

**Tier 1 — Direct Key Join (estimated ~92% coverage)**

The MTA Stations Dataset (`Stations.csv` from NY Open Data) contains a `GTFS Stop ID` column. Build this as a broadcast-joinable bridge table at pipeline startup.

```python
from pyspark.sql.functions import col, regexp_replace, broadcast

# Load the MTA Stations Dataset — contains the authoritative GTFS → Complex mapping
stations_df = spark.read.csv("mta_stations.csv", header=True, inferSchema=True)

bridge = stations_df.select(
    col("GTFS Stop ID").alias("gtfs_stop_id"),              # e.g., "127"
    col("Station Complex ID").alias("station_complex_id"),  # e.g., "613"
    col("Complex Name").alias("complex_name"),
    col("GTFS Latitude").cast("double").alias("lat"),
    col("GTFS Longitude").cast("double").alias("lon"),
    col("Borough").alias("borough")
)

# GTFS-Realtime stop_id includes direction suffix: "127N" or "127S"
# Strip the suffix before joining
gtfs_stream = gtfs_stream.withColumn(
    "stop_id_base",
    regexp_replace(col("stop_id"), "[NS]$", "")
)

# Tier 1: direct broadcast join — O(n) cost, bridge is ~500 rows
joined = gtfs_stream.join(
    broadcast(bridge),
    gtfs_stream.stop_id_base == bridge.gtfs_stop_id,
    how="left"
)
```

**Tier 2 — Normalized Name Match (estimated ~6% coverage)**

For rows where Tier 1 yields a null `station_complex_id`, apply fuzzy name matching. The bridge table is small (~500 rows), so a full cross-join against unmatched stops is cheap.

> `[ASSUMPTION]` The Levenshtein distance threshold of `<= 4` is a starting estimate. **Action (Week 2):** Manually inspect all Tier 2 matches. If there are false positives (wrong station matched), tighten to `<= 2`. If too many legitimate matches are missed, loosen to `<= 6`.

```python
import re
from pyspark.sql.functions import col, udf, levenshtein
from pyspark.sql.types import StringType

normalize = udf(
    lambda s: re.sub(r"[^a-z0-9 ]", "", s.lower().strip()) if s else "",
    StringType()
)

# `unmatched` comes from Tier 1's left join — only `stop_id` and `stop_name` are guaranteed
# populated. Bridge-side columns (including `borough`) are null here, so we can NOT filter
# on borough. Instead, cross-join against the full ~500-row bridge.
unmatched = joined.filter(col("station_complex_id").isNull()) \
    .select("stop_id", "stop_name", "stop_id_base")

unmatched_norm = unmatched.withColumn("norm_stop_name", normalize(col("stop_name")))
bridge_norm = bridge.withColumn("norm_complex_name", normalize(col("complex_name")))

# Full cross-join: cost is O(n_unmatched × 500), tolerable for a one-time bridge build
tier2 = unmatched_norm.crossJoin(bridge_norm) \
    .withColumn("name_dist", levenshtein(col("norm_stop_name"), col("norm_complex_name"))) \
    .filter(col("name_dist") <= 4) \
    .orderBy("stop_id", "name_dist") \
    .dropDuplicates(["stop_id"])
```

**Tier 3 — Geospatial Proximity (estimated ~2% coverage)**

For any stop still unresolved after Tier 2, compute Haversine distance against all stations and assign the nearest complex within 75 meters. This handles station renames and newly opened stops.

> `[ASSUMPTION]` The 75-meter proximity threshold is chosen conservatively to avoid false pairings in dense complexes (Fulton St, Union Sq, 14 St–Union Sq). **Action (Week 2):** After running Tier 3, manually verify every matched pair. Tighten to 50m if any two distinct complexes get linked.

The unmatched GTFS stops need lat/lon for this step. Those values come from the GTFS static `stops.txt` (joined back in before Tier 3), NOT from the bridge — the bridge columns were null for these rows.

```python
import math
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0  # Earth radius in meters
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

haversine_udf = udf(haversine, DoubleType())

# Residual stops that fell through both Tier 1 and Tier 2.
# Re-join with gtfs_stops_static (loaded from stops.txt) to recover lat/lon columns.
tier2_matched_ids = tier2.select("stop_id")
tier3_input = unmatched.join(tier2_matched_ids, on="stop_id", how="left_anti") \
    .join(gtfs_stops_static.select("stop_id", "stop_lat", "stop_lon"), on="stop_id", how="left")

# Cross-join against bridge (~500 rows) and keep nearest match under 75m
tier3 = tier3_input.crossJoin(
    bridge.select(
        col("station_complex_id").alias("b_complex_id"),
        col("complex_name").alias("b_complex_name"),
        col("lat").alias("b_lat"),
        col("lon").alias("b_lon")
    )
) \
    .withColumn("dist_m", haversine_udf(col("stop_lat"), col("stop_lon"), col("b_lat"), col("b_lon"))) \
    .filter(col("dist_m") < 75) \
    .orderBy("stop_id", "dist_m") \
    .dropDuplicates(["stop_id"])
```

> **Deliverable:** After all three tiers run, persist the fully resolved bridge table as a small Parquet file. Reload it on every driver restart and broadcast to all executors. Log any rows that fall through all three tiers to the DLQ for manual review.

---

### 3.2 Batch Layer Core Job — Building the Capacity Baseline

This job runs nightly at 2:00 AM. It aggregates 90 days of historical ridership and joins daily weather to produce per-station expected-entry baselines.

**Key correction vs. earlier drafts:** the MTA Hourly Ridership dataset is already keyed on `station_complex_id`. It does NOT need to be joined with the GTFS bridge table. The bridge is only used in the **speed layer** where GTFS-Realtime `stop_id` must be mapped to `station_complex_id`.

> `[ASSUMPTION]` The 90-day lookback window is a starting estimate for achieving a statistically stable baseline (enough samples per `day_of_week × hour_of_day × weather_bucket` cell). **Action (Week 4):** After the first baseline run, check the minimum sample count per cell. If any cell has fewer than 30 samples, extend the window to 180 days. If data volume causes memory pressure, reduce to 60 days and drop the `weather_bucket` dimension from low-traffic stations.

> `[ASSUMPTION]` The weather bucket thresholds (`SNOW > 0.1 inches` → "snow", `PRCP > 0.1 inches` → "rain") are reasonable starting cutoffs for NYC conditions but are not tuned. **Action (Week 9):** Validate thresholds against ridership records from known storm days (e.g., 2022 nor'easters). Adjust if ridership does not segment cleanly at 0.1 inches.

```python
from pyspark.sql.functions import col, hour, dayofweek, to_date, avg, stddev, max as spark_max, percentile_approx, when

# Inputs (local filesystem paths — switch to s3:// only if MinIO/S3 is provisioned)
ridership = spark.read.parquet("file:///data/ridership/clean/")
weather   = spark.read.parquet("file:///data/weather/clean/")

# ---- Weather preparation ----
# NOAA GHCND data is daily; propagate one bucket value across all 24 hours of that date.
weather_daily = weather.select(
    to_date(col("DATE")).alias("date"),
    col("PRCP").cast("double").alias("prcp_in"),
    col("SNOW").cast("double").alias("snow_in")
).withColumn(
    "weather_bucket",
    when(col("snow_in") > 0.1, "snow")
    .when(col("prcp_in") > 0.1, "rain")
    .otherwise("clear")
).select("date", "weather_bucket")

# ---- Ridership preparation ----
# MTA Hourly Ridership natively has `station_complex_id` — no bridge join needed.
ridership_enriched = ridership \
    .withColumn("date",        to_date(col("transit_timestamp"))) \
    .withColumn("hour_of_day", hour(col("transit_timestamp"))) \
    .withColumn("day_of_week", dayofweek(col("transit_timestamp")))

# ---- Join weather on `date` only (NOAA is daily, not hourly) ----
ridership_full = ridership_enriched.join(weather_daily, on="date", how="left") \
    .fillna({"weather_bucket": "clear"})  # missing weather day → assume clear

# ---- Baseline aggregation ----
baseline = ridership_full.groupBy(
    "station_complex_id", "day_of_week", "hour_of_day", "weather_bucket"
).agg(
    avg("entries").alias("avg_entries"),
    stddev("entries").alias("std_entries"),
    percentile_approx("entries", 0.95).alias("p95_entries")
)

# ---- Station-level max (used to normalize demand_intensity in the speed layer) ----
station_max = ridership_full.groupBy("station_complex_id") \
    .agg(spark_max("entries").alias("max_hourly_entries"))

# Write both tables to Cassandra
baseline.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "subway_dash") \
    .option("table", "station_capacity_baseline") \
    .mode("overwrite") \
    .save()

station_max.write \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "subway_dash") \
    .option("table", "station_max_entries") \
    .mode("overwrite") \
    .save()
```

---

### 3.3 Lambda Merge — Computing the Live Congestion Score

**Semantic design:** Congestion emerges when train service degrades **and** the station has high expected passenger demand. An on-time train at 3 AM at a quiet outer-borough stop is not congestion; a 5-minute delay at Times Sq at 8 AM is. The score must capture both factors.

**Two independent signals:**

1. **`service_deficit`** — from the **live** GTFS-Realtime `TripUpdate` stream (`arrival.delay` field in seconds, averaged over a 10-minute rolling window per station). Normalized to 0–1 over the range "1-minute grace" (0) to "5-minute delay" (1):
   ```
   service_deficit = clamp((avg_arrival_delay_secs - 60) / 240, 0, 1)
   ```

2. **`demand_intensity`** — from the **batch** baseline. The expected passenger load at this station, this hour, this weather bucket, normalized against the station's own all-time max to stay in 0–1:
   ```
   demand_intensity = avg_entries / max_hourly_entries
   ```

**Congestion score:**
```
congestion_score = service_deficit × demand_intensity      # range [0, 1]
```

This product is high only when **both** signals are high. A quiet station with severe delays scores low (service deficit high, demand low). A busy station with on-time trains scores low (demand high, deficit zero). A busy station with delays scores high — which is exactly what the proposal promises to detect.

**Predicted delay** (minutes the rider should expect to wait beyond schedule, scaled up when demand amplifies queuing):
```
predicted_delay_mins = (avg_arrival_delay_secs / 60) × (1 + demand_intensity)
```

> `[ASSUMPTION]` The micro-batch interval of 30 seconds is assumed to be achievable given cluster resources and GTFS data volume. **Action (Week 8):** Measure actual micro-batch duration. If > 25 seconds, raise trigger to 60s and update the dashboard poll interval to match.

> `[ASSUMPTION]` A single NYC-wide `weather_bucket` is applied uniformly to all stations. **Action (Week 3):** Inspect historical NOAA data for Central Park vs. JFK vs. LGA on the same dates — if readings diverge by more than one bucket on ≥10% of days, promote to a borough-level lookup.

```python
from pyspark.sql.functions import col, hour, dayofweek, lit, broadcast, when, greatest, least

# ---- One-time setup: load the batch views and broadcast ----
baseline_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "subway_dash") \
    .option("table", "station_capacity_baseline") \
    .load()

station_max_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "subway_dash") \
    .option("table", "station_max_entries") \
    .load()

baseline_b    = broadcast(baseline_df)
station_max_b = broadcast(station_max_df)

# ---- Weather lookup ----
# get_current_weather_bucket() reads the single-row `current_weather` Cassandra table
# that the NWS poller upserts every 15 minutes. Returns "clear", "rain", or "snow".
# A module-level cache with a 15-min TTL avoids hitting Cassandra per micro-batch.
from infra.weather_cache import get_current_weather_bucket

def process_micro_batch(batch_df, batch_id):
    """
    batch_df columns (produced by the upstream speed-layer join — see Section 9 Task B5.1):
      station_complex_id   STRING   (from bridge-enriched vehicle_positions)
      event_timestamp      TIMESTAMP
      avg_arrival_delay_secs  DOUBLE  (10-min rolling mean from TripUpdate.arrival.delay)
    """
    current_bucket = get_current_weather_bucket()

    enriched = batch_df \
        .withColumn("hour_of_day",   hour(col("event_timestamp"))) \
        .withColumn("day_of_week",   dayofweek(col("event_timestamp"))) \
        .withColumn("weather_bucket", lit(current_bucket)) \
        .join(baseline_b,
              on=["station_complex_id", "day_of_week", "hour_of_day", "weather_bucket"],
              how="left") \
        .join(station_max_b, on="station_complex_id", how="left")

    scored = enriched \
        .withColumn(
            # service_deficit: 0 when delays ≤ 60s, 1 when delays ≥ 300s, linear in between
            "service_deficit",
            least(lit(1.0), greatest(lit(0.0),
                (col("avg_arrival_delay_secs") - lit(60.0)) / lit(240.0)))
        ).withColumn(
            # demand_intensity: expected entries this hour / station's historical peak
            "demand_intensity",
            when(col("max_hourly_entries") > 0,
                 col("avg_entries") / col("max_hourly_entries"))
            .otherwise(lit(0.0))
        ).withColumn(
            "congestion_score",
            col("service_deficit") * col("demand_intensity")   # [0, 1]
        ).withColumn(
            "predicted_delay_mins",
            (col("avg_arrival_delay_secs") / lit(60.0)) * (lit(1.0) + col("demand_intensity"))
        ).withColumn(
            # [ASSUMPTION A-06] Thresholds 0.20 and 0.50 are starting estimates calibrated
            # for the 0–1 product range. Week 7 back-test re-tunes against known events.
            "alert_level",
            when(col("congestion_score") >= 0.50, "SEVERE")
            .when(col("congestion_score") >= 0.20, "MODERATE")
            .otherwise("NORMAL")
        )

    # Write to MongoDB (uses the official MongoDB Spark Connector v10 format string)
    scored.write \
        .format("mongodb") \
        .option("spark.mongodb.connection.uri", "mongodb://mongo:27017") \
        .option("spark.mongodb.database", "subway_dash") \
        .option("spark.mongodb.collection", "speed_layer") \
        .mode("append") \
        .save()

query = live_stream.writeStream \
    .foreachBatch(process_micro_batch) \
    .trigger(processingTime="30 seconds") \
    .option("checkpointLocation", "file:///checkpoints/congestion_score") \
    .start()
```

**Why this formulation is correct:**

- **No unit mismatch.** `service_deficit` and `demand_intensity` are both dimensionless ratios in `[0, 1]`. Their product is also in `[0, 1]`.
- **No semantic mismatch.** The live signal measures train service quality; the batch signal measures passenger demand. They are independent by construction, so multiplying them has physical meaning (both must be elevated for congestion to register).
- **No circular dependency.** `predicted_delay_mins` is a forward projection from observed delay and demand — the delay is the input, not an output that gets regressed against itself.

---

## Section 4 — Lambda Architecture Integration Points

The Lambda merge happens **once** — inside the speed-layer micro-batch, where the broadcast batch baseline is joined against the live stream. The dashboard reads the already-merged score directly; it does not perform a second blending step.

| Merge Point | Trigger | Mechanism | Output Destination |
|---|---|---|---|
| **Nightly Baseline Rebuild** | Cron: 2:00 AM daily | PySpark batch job aggregates last 90 days of ridership and writes `station_capacity_baseline` + `station_max_entries` | Cassandra `subway_dash` keyspace |
| **Live Weather Refresh** | Every 15 minutes | NWS poller fetches current conditions and upserts the single-row `current_weather` table | Cassandra `current_weather` table |
| **Historical Weather Join** | Once (Week 3), re-run yearly | Batch job joins ridership to NOAA daily weather and persists `ridership_weather_baseline` Parquet | Local Parquet (`file:///data/ridership_weather_baseline/`) |
| **Lambda Merge (Speed Layer)** | Every 30 seconds (micro-batch) | Structured Streaming job reads `gtfs-vehicle` + `gtfs-trips`, broadcast-joins batch baseline + station_max, computes `congestion_score = service_deficit × demand_intensity`, writes enriched documents | MongoDB `speed_layer` collection |
| **Dashboard Read** | Every 30 seconds (Streamlit poll) | Streamlit calls FastAPI, which reads the latest MongoDB document per station. **No re-blending at read time** — the score is already the final Lambda output. | Streamlit frontend |

---

## Section 5 — Risk Register & Mitigations

| Risk | Severity | Root Cause | Mitigation |
|---|---|---|---|
| **Protobuf deserialization overhead** | High | `gtfs-realtime-bindings` Python parser is single-threaded; calling it inside a `map()` creates one parser instance per row | Use `mapPartitions` instead of `map` — amortizes parser initialization cost across the entire partition. Alternatively, pre-decode Protobuf to JSON inside the Kafka Producer so Spark only handles clean JSON. |
| **Spark join skew on high-traffic stations** | High | Times Sq, Grand Central, and Atlantic Ave process 10× average traffic — their `station_complex_id` keys dominate single partitions, creating straggler tasks | Apply a **salted join**: append a random suffix `0–9` to `station_complex_id` on the large side, replicate baseline rows with all 10 salt values, join on the salted composite key, then strip the salt. `[ASSUMPTION]` The 10× skew factor and salt width of 10 are estimates. **Action (Week 8):** Run `df.groupBy("station_complex_id").count().orderBy(desc("count"))` to measure actual skew before applying salt. If max/median ratio is < 5×, skip salting — it adds overhead for marginal gain. |
| **Kafka consumer lag during rush hour** | Medium | GTFS feed burst rate exceeds Spark's micro-batch throughput; lag accumulates and produces delayed alerts | Pre-partition Kafka topics by subway line group (12 groups → 12 partitions). Set `maxOffsetsPerTrigger = 5000` in the Spark consumer config to enforce backpressure and prevent OOM. `[ASSUMPTION]` The value of 5,000 is an estimate; tune during Week 8 performance audit based on measured records-per-second from the live feed. |
| **NOAA API rate limits** | Medium | NOAA CDO API is capped at 5 req/s and 10,000 req/day; multiple Spark workers polling it independently will be throttled | Centralize all NOAA calls inside a single poller process that owns the API quota. Publish results to the `weather-feed` Kafka topic. All Spark jobs read from Kafka only — never the API directly. |
| **Unresolved GTFS → station_complex_id mappings** | Medium | MTA station renames, newly opened stations, or data entry errors in source files cause bridge table misses | Implement the 3-tier fallback (Section 3.1). Log all Tier 3 matches and any null residuals to the DLQ Kafka topic. Review and manually patch during Week 3. |
| **Cassandra write hotspot** | Low | All speed-layer micro-batches write simultaneously to the same partition key during a system-wide event | Use a composite partition key: `(station_complex_id, shard_id)` where `shard_id = hash(event_timestamp_seconds) % 4`. Distributes writes across 4 virtual shards per station. |

---

## Section 6 — Handoff Contracts & Cross-Track Dependencies

```
Week 2 End:
  Preyansh → delivers bridge table (Parquet) to Arjun and Yash
  Yash     → delivers Cassandra DDL scripts to Arjun and Preyansh

Week 4 End:
  Arjun    → delivers gtfs-vehicle staging Parquet spec to Preyansh
  Preyansh → delivers station_capacity_baseline Parquet schema to Yash

Week 5 End:
  Yash     → delivers FastAPI endpoint contracts (OpenAPI spec) to Preyansh
  Arjun    → delivers final Kafka topic schemas (JSON field names + types) to Preyansh

Week 6 End:
  Preyansh → delivers final congestion_score formula and threshold values to Yash

Week 9 End:
  All tracks → sign off on 2-hour integration test results
  Arjun    → confirms `docker-compose up` launches the full stack from cold start
```

---

## Section 7 — Assumptions Registry

All assumptions in this plan are listed here in one place for easy tracking. Each must be resolved during execution and the plan updated accordingly.

| ID | Assumption | Confidence | Must Be Resolved By | Owner |
|---|---|---|---|---|
| A-01 | Tier 1 spatial join covers ~92% of stops; Tier 2 ~6%; Tier 3 ~2% | Medium | End of Week 2 | Preyansh |
| A-02 | Levenshtein threshold of `<= 4` correctly resolves renamed stations without false positives | Medium | End of Week 2 | Preyansh |
| A-03 | Haversine proximity threshold of 100m correctly isolates unique stations in dense areas | Medium | End of Week 2 | Preyansh |
| A-04 | 90-day lookback window provides sufficient sample density per `(station, hour, day, weather)` cell | Medium | End of Week 4 | Preyansh |
| A-05 | Weather bucket thresholds (`SNOW > 0.1`, `PRCP > 0.1` inches) cleanly segment ridership behavior | Low | End of Week 9 | Preyansh |
| A-06 | Alert thresholds `congestion_score ≥ 0.20` (MODERATE) and `≥ 0.50` (SEVERE) correctly classify real events on the `[0,1]` product scale | Low | End of Week 7 — back-test against ≥ 2 known events | Preyansh |
| A-07 | `service_deficit` normalization range — 60s grace to 300s saturation — matches how riders actually perceive late trains | Low | End of Week 7 — back-test; adjust the 60/300 endpoints if needed | Preyansh |
| A-08 | `demand_intensity = avg_entries / max_hourly_entries` is a sound 0–1 normalization (per-station self-max, not network-wide) | Medium | End of Week 4 — confirm after baseline backfill: no division-by-zero, distribution is well-spread | Preyansh |
| A-09 | A single NYC-wide weather bucket is sufficient granularity; borough-level is not needed | Medium | End of Week 3 — compare Central Park vs. JFK vs. LGA historical data | Arjun |
| A-10 | 30-second micro-batch interval is achievable given cluster resources and GTFS data volume | Medium | End of Week 8 performance audit | Arjun |
| A-11 | `maxOffsetsPerTrigger = 5000` is the right Kafka backpressure value for this feed's burst rate | Medium | End of Week 8 performance audit | Arjun |
| A-12 | Spark join skew ratio for top stations warrants salted joins (threshold: max/median > 5×) | Medium | End of Week 8 — measure before applying salt | Arjun |
| A-13 | MTA Stations Dataset contains ~470 unique station complexes | Low | End of Week 1 — verify row count after download | Preyansh |
| A-14 | Alert banner threshold `congestion_score ≥ 0.50` correctly flags SEVERE without spamming | Low | End of Week 7 — tune after back-testing | Yash |
| A-15 | NOAA daily weather data (one bucket per day) is adequate; hourly weather granularity is not needed | Medium | End of Week 3 — check rain/clear transitions within single days in history | Preyansh |
| A-16 | `arrival.delay` from GTFS-Realtime TripUpdates is populated reliably (not null > 90% of messages) | High | End of Week 5 — validate during the `gtfs_trips_consumer` build (Task A5.2) | Arjun |

---

## Section 8 — Definition of Done

| Deliverable | Acceptance Criteria |
|---|---|
| **Distributed Data Pipeline** | Kafka is ingesting live GTFS data. PySpark batch jobs run on cron schedule. Speed layer produces `congestion_score` within 60 seconds of a real-world event. |
| **Live Dashboard** | Streamlit map shows all ~470 NYC subway stations `[ASSUMPTION A-13 — exact count verified in Week 1]`. Markers update within 30 seconds. Predicted delay values are non-zero during peak hours. Alert banner fires when `congestion_score ≥ 0.50` (SEVERE, matching the `[0, 1]` product-scale thresholds from Assumption A-06; tuned in Week 7). |
| **Analytics Report** | Covers: system architecture diagram, 3-tier spatial join methodology, the `service_deficit × demand_intensity` congestion formulation with its derivation, batch weather model, Lambda merge strategy, final calibrated thresholds, and lessons learned. Reproducible by a reader unfamiliar with the codebase. |

---

## Section 9 — Detailed Task Breakdown Per Member

> This section is the ground-level execution guide. Each task is atomic — it produces exactly one named output (a file, a running service, a validated dataset, or a documented result). Tasks marked `[BLOCKED BY]` cannot start until the listed dependency is complete.

---

### Phase 1 — Foundation & Ingestion (Weeks 1–3)

---

#### Week 1

---

##### Arjun — Infrastructure Setup

**Task A1.1 — Write `docker-compose.yml`**
- Services: `zookeeper` (image: `confluentinc/cp-zookeeper:7.5`), `kafka` (image: `confluentinc/cp-kafka:7.5`), `spark-master`, `spark-worker`
- Expose Kafka on `localhost:9092`, Spark UI on `localhost:8080`
- Output: `infra/docker-compose.yml`

**Task A1.2 — Create Kafka Topics**
- Run after `docker-compose up`
- Partition counts are set to their **final** values now to avoid a costly recreation mid-project (one partition per NYC subway line group for `gtfs-vehicle` and `gtfs-trips`)
- Create: `gtfs-vehicle` (12 partitions), `gtfs-trips` (12 partitions), `gtfs-alerts` (2 partitions), `gtfs-dlq` (2 partitions), `weather-feed` (1 partition, compacted)
- All topics use replication-factor 1 (single-broker dev cluster — acceptable limitation for a student project)
- Commands:
  ```bash
  kafka-topics.sh --create --topic gtfs-vehicle --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092
  kafka-topics.sh --create --topic gtfs-trips   --partitions 12 --replication-factor 1 --bootstrap-server localhost:9092
  kafka-topics.sh --create --topic gtfs-alerts  --partitions 2  --replication-factor 1 --bootstrap-server localhost:9092
  kafka-topics.sh --create --topic gtfs-dlq     --partitions 2  --replication-factor 1 --bootstrap-server localhost:9092
  kafka-topics.sh --create --topic weather-feed --partitions 1  --replication-factor 1 --bootstrap-server localhost:9092 \
      --config cleanup.policy=compact
  ```
- Output: all 5 topics visible in `kafka-topics.sh --list`

**Task A1.3 — Verify Kafka + Spark Connectivity**
- Write `infra/test_kafka_spark.py`: a 10-line PySpark Structured Streaming job that reads from `gtfs-vehicle` and prints the count of received messages every 10 seconds
- Run it for 2 minutes against manually produced test messages
- Output: console shows non-zero message counts

**Task A1.4 — Set Up Project Repository**
- Create the following top-level directory structure:
  ```
  subway-dash/
  ├── infra/           # docker-compose, Kafka scripts
  ├── ingestion/       # Kafka producers and pollers
  ├── processing/      # PySpark batch and streaming jobs
  ├── serving/         # FastAPI app
  ├── dashboard/       # Streamlit app
  ├── data/            # Local data files and bridge table
  └── docs/            # Analytics report
  ```
- Output: repo pushed to shared Git remote

---

##### Preyansh — Data Acquisition & Profiling

**Task B1.1 — Download All Source Datasets**
- `stops.txt`: download from MTA GTFS static feed ZIP at `http://web.mta.info/developers/data/nyct/subway/google_transit.zip`
- `MTA_Stations.csv`: download from NY Open Data (MTA Subway Stations dataset)
- `MTA_Hourly_Ridership.csv`: download from NY State Open Data (MTA Subway Hourly Ridership, 2020–present)
- NOAA weather: pull via NOAA CDO API for station `GHCND:USW00094728` (Central Park) and `GHCND:USW00094789` (JFK), years 2022–2024
- Save all to `data/raw/`
- Output: 4 files present in `data/raw/`, sizes logged

**Task B1.2 — Write Data Profiling Notebook**
- File: `processing/notebooks/01_data_profiling.ipynb`
- For each dataset, record: total row count, column names and dtypes, null count per column, min/max values for key numeric columns, 5 sample rows
- Specifically verify: does `MTA_Stations.csv` have a column named exactly `GTFS Stop ID`? Does it have `Station Complex ID`? What is the exact spelling? Does `MTA_Hourly_Ridership.csv` have a `station_complex_id` column or something similar?
- Output: notebook with all four datasets profiled, column names confirmed

**Task B1.3 — Document Join Keys**
- After profiling, write `docs/join_key_map.md` listing:
  - The exact column name in each dataset that will serve as the join key
  - Any format mismatches (e.g., `stop_id` is string `"127"` vs integer `127`)
  - The exact timestamp column name and format in the ridership CSV
- Output: `docs/join_key_map.md` reviewed and signed off with Arjun

---

##### Yash — Storage & Frontend Shell

**Task C1.1 — Stand Up MongoDB and Cassandra**
- Add MongoDB (`mongo:6`) and Cassandra (`cassandra:4.1`) to `infra/docker-compose.yml`
- Mount persistent volumes for both so data survives container restarts
- Confirm both start cleanly alongside Kafka
- Create a 24-hour TTL index on the `speed_layer` collection so documents auto-expire (prevents unbounded growth):
  ```python
  from pymongo import MongoClient, ASCENDING
  client = MongoClient("mongodb://localhost:27017")
  db = client["subway_dash"]
  db.speed_layer.create_index(
      [("inserted_at", ASCENDING)],
      expireAfterSeconds=86400  # 24 hours
  )
  ```
- Verify connectivity:
  ```python
  # MongoDB
  print(client.server_info())  # must not throw

  # Cassandra
  from cassandra.cluster import Cluster
  cluster = Cluster(["localhost"])
  session = cluster.connect()
  print(session.execute("SELECT release_version FROM system.local"))
  ```
- Output: both clients connect without errors; TTL index confirmed via `db.speed_layer.index_information()`

**Task C1.2 — Scaffold Streamlit App**
- File: `dashboard/app.py`
- Page 1: NYC subway basemap using `pydeck` with a `ScatterplotLayer` rendering 5 hardcoded test stations as red dots
- Confirm the map renders at `http://localhost:8501`
- Output: `dashboard/app.py` running with visible map

**Task C1.3 — Write Database Client Helpers**
- File: `serving/db_clients.py`
- Functions: `get_mongo_collection(name)`, `get_cassandra_session()`, `close_connections()`
- Both functions must read connection strings from environment variables, not hardcoded strings
- Output: `serving/db_clients.py` importable without errors

---

#### Week 2

---

##### Arjun — GTFS Kafka Producer

**Task A2.1 — Verify MTA GTFS-Realtime Feed Access**
- MTA removed the API-key requirement for GTFS-Realtime feeds around 2021. The endpoints are publicly accessible over HTTP GET.
- Confirm access by running:
  ```bash
  curl -s https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs -o /tmp/vp.pb
  ls -l /tmp/vp.pb   # must be non-empty Protobuf payload
  ```
- Document the exact URLs for all three feeds (VehiclePositions, TripUpdates, Alerts) in `docs/mta_feeds.md`
- Output: all 3 feed URLs confirmed reachable and returning non-empty Protobuf

**Task A2.2 — Write `ingestion/gtfs_producer.py`**
- Every 15 seconds, call all 3 MTA GTFS-Realtime endpoints: VehiclePositions, TripUpdates, Alerts
- For each feed:
  1. Fetch raw Protobuf bytes
  2. Deserialize using `google.transit.gtfs_realtime_pb2.FeedMessage`
  3. For each entity in the feed, serialize to a flat JSON dict (include `route_id` so the partitioner can route by subway line group)
  4. Partition key: map `route_id` to one of 12 partitions using a fixed dict (A/C/E→0, B/D/F/M→1, G→2, J/Z→3, L→4, N/Q/R/W→5, 1/2/3→6, 4/5/6→7, 7→8, S→9, SIR→10). For unknown route_ids, use `hash(route_id) % 12` rather than a single overflow bucket (prevents hotspotting).
  5. Publish to the corresponding Kafka topic
- Wrap deserialization in `try/except`; on failure, publish the raw bytes + error message to `gtfs-dlq`
- Output: `ingestion/gtfs_producer.py` running continuously, producing to all 3 topics across all 12 partitions

**Task A2.3 — Write `ingestion/gtfs_monitor.py`**
- A simple Kafka consumer that prints a one-line status every 60 seconds:
  ```
  [10:32:00] gtfs-vehicle: 142 msgs/min | gtfs-trips: 87 msgs/min | gtfs-alerts: 3 msgs/min | DLQ: 0 msgs
  ```
- Output: monitor confirms messages are flowing with zero DLQ entries

---

##### Preyansh — Bridge Table & Ridership Cleaning

**Task B2.1 — Write `processing/build_bridge_table.py`**
- Load `MTA_Stations.csv` into PySpark
- Build `bridge` DataFrame (columns: `gtfs_stop_id`, `station_complex_id`, `complex_name`, `lat`, `lon`, `borough`) using exact column names confirmed in Task B1.2
- Run Tier 1 join against `stops.txt`; log null rate
- If null rate > 8%: run Tier 2 fuzzy name match (Levenshtein ≤ 4)
- If residual > 2%: run Tier 3 Haversine crossJoin (< 100m)
- Log: total stops processed, Tier 1 match count, Tier 2 match count, Tier 3 match count, unresolved count
- Persist to `data/bridge/station_bridge.parquet`
- Output: `station_bridge.parquet` written; null rate logged and compared against Assumption A-01

**Task B2.2 — Write `processing/clean_ridership.py`**
- Load `MTA_Hourly_Ridership.csv` into PySpark
- Transformations:
  1. Cast timestamp column to UTC using the exact format found in profiling
  2. Extract `date` (yyyy-MM-dd) and `hour_of_day` (0–23) columns
  3. Drop rows where `entries` is null or negative
  4. Cast `entries` to integer
  5. Join on `station_complex_id` using the bridge table to verify all IDs resolve
- Write cleaned output to `s3://subway-dash/ridership/clean/` as Parquet, partitioned by `date`
- Output: Parquet written, row count before/after cleaning logged

**Task B2.3 — Deliver Bridge Table to Arjun and Yash**
- Share `data/bridge/station_bridge.parquet` via Git
- Share `docs/join_key_map.md` with confirmed column names
- Handoff: confirm Arjun can read the bridge table in his Spark consumer, confirm Yash can read it for the Streamlit map markers

---

##### Yash — Cassandra Schema & First Real Map

**Task C2.1 — Write Cassandra DDL**
- File: `infra/cassandra_schema.cql`
- Create keyspace:
  ```sql
  CREATE KEYSPACE IF NOT EXISTS subway_dash
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  ```
- Create `station_capacity_baseline` table (batch view — no TTL, rebuilt nightly):
  ```sql
  CREATE TABLE subway_dash.station_capacity_baseline (
      station_complex_id  TEXT,
      day_of_week         INT,
      hour_of_day         INT,
      weather_bucket      TEXT,
      avg_entries         DOUBLE,
      std_entries         DOUBLE,
      p95_entries         DOUBLE,
      PRIMARY KEY ((station_complex_id, weather_bucket), day_of_week, hour_of_day)
  );
  ```
- Create `station_max_entries` table (normalizer for `demand_intensity`):
  ```sql
  CREATE TABLE subway_dash.station_max_entries (
      station_complex_id  TEXT PRIMARY KEY,
      max_hourly_entries  DOUBLE
  );
  ```
- Create `current_weather` table (single-row table updated by the NWS poller every 15 min; read by the speed-layer broadcast):
  ```sql
  CREATE TABLE subway_dash.current_weather (
      scope           TEXT PRIMARY KEY,   -- always the literal "nyc"
      weather_bucket  TEXT,               -- "clear" | "rain" | "snow"
      prcp_in         DOUBLE,
      snow_in         DOUBLE,
      tmax_f          DOUBLE,
      updated_at      TIMESTAMP
  );
  ```
- Create `weather_coefficients` table (Week 9 historical uplift model — separate from `current_weather`):
  ```sql
  CREATE TABLE subway_dash.weather_coefficients (
      station_complex_id  TEXT,
      weather_bucket      TEXT,
      multiplier          DOUBLE,
      PRIMARY KEY (station_complex_id, weather_bucket)
  );
  ```
- Note: the earlier draft's `station_metrics` table is dropped. The speed-layer output lives in MongoDB (`speed_layer` collection with a 24-hour TTL); Cassandra holds only the permanent batch views. Keeping a third "mixed" table would duplicate the MongoDB output and create consistency drift.
- Run DDL against live Cassandra; confirm all tables exist
- Output: `infra/cassandra_schema.cql` committed; 4 tables verified (`station_capacity_baseline`, `station_max_entries`, `current_weather`, `weather_coefficients`)

**Task C2.2 — Render Real Station Map**
- Load `data/bridge/station_bridge.parquet` (from Preyansh's handoff) using Pandas
- Pass all station lat/lon coordinates to Streamlit `pydeck` ScatterplotLayer
- Confirm all stations render with correct geographic positions
- Output: map shows correct station locations with no missing dots

**Task C2.3 — Write MongoDB Schema Spec**
- File: `docs/mongodb_schema.md`
- Document the exact JSON structure for the `speed_layer` collection:
  ```json
  {
    "station_complex_id": "613",
    "complex_name": "Times Sq-42 St",
    "event_timestamp": "2024-04-21T08:30:00Z",
    "avg_arrival_delay_secs": 185.0,
    "service_deficit": 0.52,
    "demand_intensity": 0.68,
    "congestion_score": 0.35,
    "predicted_delay_mins": 5.2,
    "alert_level": "MODERATE",
    "weather_bucket": "clear",
    "inserted_at": "2024-04-21T08:30:05Z"
  }
  ```
- Share with Preyansh so the `process_micro_batch` output matches exactly what the API expects
- Output: `docs/mongodb_schema.md` committed

---

#### Week 3

---

##### Arjun — Pipeline Hardening & Weather Poller

**Task A3.1 — Sustained Load Test**
- Run `gtfs_producer.py` continuously for 2 hours
- Every 15 minutes, run:
  ```bash
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spark-consumer
  ```
- Log `LAG` column for each partition
- Target: LAG = 0 for all partitions at steady state
- Output: load test log with LAG readings at each 15-minute interval

**Task A3.2 — DLQ Monitoring**
- After the 2-hour load test, consume all messages from `gtfs-dlq` and categorize errors
- Fix any recurring deserialization errors in `gtfs_producer.py`
- Output: DLQ empty or all errors are known and documented as non-blocking edge cases

**Task A3.3 — Write `ingestion/weather_poller.py` (NWS, not NOAA CDO)**
- NOAA CDO serves *historical* climate data with 1–3 day latency — unusable for real-time. The speed layer needs current conditions, which come from the National Weather Service (NWS) public API at `https://api.weather.gov`. No API key required; the only requirement is a descriptive `User-Agent` header identifying the project + contact email.
- Every 15 minutes, call `GET https://api.weather.gov/stations/KNYC/observations/latest` (KNYC = Central Park ASOS station). Extract the fields `properties.precipitationLastHour.value` (mm), `properties.temperature.value` (°C), `properties.textDescription`.
  - Convert precipitation mm → inches: `prcp_in = mm / 25.4`.
  - NWS does not publish a dedicated live snowfall field reliably; infer snow from `textDescription` containing any of `"Snow" | "Sleet" | "Ice"` AND `temperature < 0 °C`.
- Classify: `weather_bucket = "snow"` if snow inferred, else `"rain"` if `prcp_in > 0.1`, else `"clear"` — same thresholds as the historical batch path so live and batch buckets stay aligned.
- Two writes every cycle (dual-write is intentional — the speed layer reads from Cassandra; downstream debugging/replay reads from Kafka):
  1. Upsert the single-row `subway_dash.current_weather` Cassandra table (partition key `scope = 'nyc'`)
  2. Publish a JSON message to `weather-feed` Kafka topic: `{ "timestamp": "...", "weather_bucket": "rain", "prcp_in": 0.15, "tmax_f": 45 }`
- Handle HTTP 5xx or timeouts with exponential backoff (max 3 retries). On total failure, do NOT overwrite — leave the last-known-good row in `current_weather` intact.
- NOAA CDO is still used (unchanged) for the *historical* weather ingestion in Task B3.1 — only the real-time path moves to NWS.
- Output: `ingestion/weather_poller.py` running; `current_weather` Cassandra row refreshed every 15 minutes with a fresh `updated_at`; `weather-feed` Kafka topic receiving a message on the same cadence

---

##### Preyansh — NOAA Cleaning & Weather-Ridership Join

**Task B3.1 — Write `processing/clean_weather.py`**
- Load NOAA historical CSV into PySpark
- Transformations:
  1. Parse `DATE` column to `date` (yyyy-MM-dd) and `hour` (0–23) using the exact format in the NOAA file
  2. Cast `PRCP`, `SNOW`, `TMAX`, `TMIN` to double
  3. Forward-fill missing hourly values using `Window` function (missing weather readings are common in NOAA data)
  4. Add `weather_bucket` column using the same thresholds as `weather_poller.py` (Assumption A-05) — `SNOW > 0.1 in` → "snow", `PRCP > 0.1 in` → "rain", else "clear"
- Output: `s3://subway-dash/weather/clean/` Parquet

**Task B3.2 — Write `processing/join_ridership_weather.py`**
- Read cleaned ridership Parquet and cleaned weather Parquet
- Join on `(date, hour_of_day)` — left join (ridership is the primary table; missing weather = null)
- Null-fill `weather_bucket` as `"clear"` for any unmatched rows
- Write to `s3://subway-dash/ridership_weather_baseline/` partitioned by `(date)`
- Validate: count rows where `weather_bucket` is null — must be < 1% of total rows
- Output: `ridership_weather_baseline` Parquet written and validated

**Task B3.3 — Phase 1 Data Validation Report**
- Write a short `docs/phase1_validation.md`:
  - Bridge table: total stations, null rate after all 3 tiers
  - Ridership: row count before/after cleaning, date range covered
  - Weather join: null rate, date range, weather bucket distribution (% clear / rain / snow)
- Output: `docs/phase1_validation.md` committed

---

##### Yash — MongoDB Wiring & Cassandra Baseline Schema

**Task C3.1 — Wire Streamlit to MongoDB**
- In `dashboard/app.py`, add a 30-second refresh loop using `streamlit-autorefresh` (or `st.rerun()` gated by `time.time()` — do NOT use `st.experimental_rerun()`, which was removed in Streamlit 1.27):
  ```python
  from streamlit_autorefresh import st_autorefresh
  st_autorefresh(interval=30_000, key="live_refresh")
  ```
- Query MongoDB `speed_layer` collection: fetch the 10 most recently inserted documents sorted by `inserted_at`
- Display raw JSON in a `st.dataframe()` below the map
- Test: manually insert a test document using `db_clients.py`, confirm it appears in Streamlit within 35 seconds
- Output: Streamlit refreshes live and shows MongoDB documents

**Task C3.2 — Write `processing/cassandra_writer.py`**
- PySpark → Cassandra sink function:
  ```python
  def write_to_cassandra(df, table_name, mode="append"):
      df.write \
          .format("org.apache.spark.sql.cassandra") \
          .option("keyspace", "subway_dash") \
          .option("table", table_name) \
          .mode(mode) \
          .save()
  ```
- Test: write 5 hardcoded rows to `station_capacity_baseline`, then read them back with a `SELECT` query
- Output: round-trip confirmed

**Task C3.3 — Schema Handoff to Preyansh**
- The `station_capacity_baseline`, `station_max_entries`, `current_weather`, and `weather_coefficients` tables were all created in Task C2.1 with their final shape — there is nothing to recreate here.
- Confirm with Preyansh that his Week 4 baseline writer (Task B4.1) will produce exactly the columns declared in the DDL: `(station_complex_id, day_of_week, hour_of_day, weather_bucket, avg_entries, std_entries, p95_entries)` for `station_capacity_baseline`, and `(station_complex_id, max_hourly_entries)` for `station_max_entries`. Any column-name mismatch will silently drop data on write.
- Cross-check Spark's `dayofweek` convention (Sunday = 1) against Preyansh's expectation; document the convention in `docs/join_key_map.md`.
- Output: explicit written confirmation from Preyansh that his job's output schema matches the DDL byte-for-byte

---

### Phase 2 — Joins, Analytics & Prediction (Weeks 4–7)

---

#### Week 4

---

##### Arjun — Vehicle Position Streaming Consumer

**Task A4.1 — Write `processing/gtfs_vehicle_consumer.py`**
- PySpark Structured Streaming job reading from `gtfs-vehicle` Kafka topic
- Schema for the JSON value:
  ```
  trip_id, route_id, vehicle_id, stop_id, current_status, current_stop_sequence, timestamp
  ```
- Apply bridge table broadcast join: add `station_complex_id` from `station_bridge.parquet`
- Strip direction suffix from `stop_id` before joining (regex: `[NS]$`)
- Write to `s3://subway-dash/staging/vehicle_positions/` using `trigger(processingTime="30 seconds")`
- Checkpoint at `/checkpoints/vehicle_positions/`
- Output: staging files appearing every 30 seconds in S3/HDFS

**Task A4.2 — Validate Vehicle Position Output**
- Let the consumer run for 1 hour
- Count: how many unique `station_complex_id` values appear? How many `null` station IDs?
- If null rate > 5%: escalate to Preyansh to re-examine the bridge table
- Output: validation report logged to console, null rate documented

---

##### Preyansh — Batch Layer Core Job

**Task B4.1 — Write `processing/build_baseline.py`**
- Input: cleaned ridership Parquet from Week 2 (`file:///data/ridership/clean/`) and cleaned NOAA daily weather Parquet from Week 3 (`file:///data/weather/clean/`)
- Logic: full batch job exactly as specified in Section 3.2 of this plan. Critical points to preserve (do not reintroduce the old draft errors):
  1. MTA Hourly Ridership natively carries `station_complex_id` — do **not** join with the bridge table in this job. The bridge is only needed by the speed layer.
  2. NOAA GHCND is daily — join weather on `date` only, not `(date, hour)`. The same `weather_bucket` applies to all 24 hours of a given day.
  3. Produce two outputs: `station_capacity_baseline` (the grouped aggregation) AND `station_max_entries` (per-station historical peak, used later by the speed layer for `demand_intensity` normalization).
- Grouping keys for `station_capacity_baseline`: `(station_complex_id, day_of_week, hour_of_day, weather_bucket)`
- Aggregations: `avg(entries)`, `stddev(entries)`, `percentile_approx(entries, 0.95)`
- Write both tables to Cassandra with `mode("overwrite")`
- Output: `station_capacity_baseline` and `station_max_entries` tables populated in Cassandra

**Task B4.2 — Validate Baseline Accuracy**
- Query Cassandra: fetch `avg_entries` for station `611` (Times Sq–42 St complex — confirm exact `station_complex_id` from the MTA Stations Dataset during Task B1.2 and correct here if different), `day_of_week=4` (Wednesday, Spark convention: Sunday=1), `hour_of_day=8`, `weather_bucket="clear"`
- Expected range: roughly **15,000–30,000 entries/hour** for Times Sq–42 St at the 8 AM peak. `[ASSUMPTION]` The earlier draft cited 30k–70k/hour; that figure is the MTA's published *daily* entries in the mid-hundred-thousands divided across the day, not the peak hourly count. Published NY Open Data hourly ridership for Times Sq on weekday mornings lands in the 15k–30k band — use that as the sanity bracket.
- If the value is outside this range: check for unit errors (MTA "entries" = tap-ins, one per fare-paying rider; half-days, aggregation windows, or double-counting turnstile sides are common pitfalls). Re-examine the ridership CSV column used.
- If the value is within ±30% of the expected range: pass. Tighter validation happens in Task B8.3 with more stations.
- Output: baseline accuracy spot-check result documented in `docs/phase1_validation.md`

---

##### Yash — Cassandra Writer Integration

**Task C4.1 — Integrate PySpark → Cassandra Writer**
- Update `processing/build_baseline.py` to call `write_to_cassandra(baseline_df, "station_capacity_baseline", mode="overwrite")`
- Run end-to-end: `build_baseline.py` writes → Cassandra stores → read back with Python client
- Print 5 rows from Cassandra to confirm write succeeded
- Output: Cassandra `station_capacity_baseline` table has data

**Task C4.2 — Streamlit Round-Trip Test**
- Query `station_capacity_baseline` from Cassandra in Streamlit
- Display a table of `avg_entries` by `hour_of_day` for a single station (hardcode Times Sq for now)
- Output: Streamlit shows a table of baseline values for Times Sq, hours 0–23

---

#### Week 5

---

##### Arjun — Trip Update Streaming Consumer

**Task A5.1 — Write `processing/gtfs_trips_consumer.py`**
- PySpark Structured Streaming reading from `gtfs-trips` Kafka topic
- For each `TripUpdate` entity, explode the `stop_time_update` array to one row per stop
- Per-row schema extracted directly from the Protobuf fields (do **not** compute deltas manually — GTFS-Realtime publishes the delay explicitly in `StopTimeUpdate.arrival.delay` and `StopTimeUpdate.departure.delay`, both in seconds):
  ```
  trip_id              STRING
  stop_id              STRING
  arrival_delay_secs   INT   (= stop_time_update.arrival.delay; negative means early)
  departure_delay_secs INT   (= stop_time_update.departure.delay)
  arrival_time_epoch   LONG  (= stop_time_update.arrival.time, raw for debugging)
  timestamp            TIMESTAMP  (feed header timestamp)
  ```
- **Do not compute** `dwell_time = arrival_time_actual - arrival_time_scheduled`. The earlier draft did this, but (a) the GTFS feed does not carry a separate "scheduled" timestamp per update, and (b) the delay field is already what we want. Using `arrival.delay` directly eliminates a whole class of timestamp-math bugs.
- Filter out null `arrival_delay_secs` rows (validates Assumption A-16 online)
- Write to `file:///data/staging/trip_delays/` with 30-second trigger
- Checkpoint at `file:///checkpoints/trip_delays/`
- Output: `trip_delays` staging files appearing every 30 seconds, each row carrying a populated `arrival_delay_secs`

**Task A5.2 — Validate Arrival Delays**
- Sample 100 rows from the `trip_delays` staging table
- Verify: most values are in the range **-120s to +600s** (trains are commonly 0–2 min early or 1–10 min late). Extreme outliers > 1800s indicate stale trip updates and should be flagged.
- Also record the null rate on `arrival_delay_secs` — this directly resolves Assumption A-16. Target: null rate < 10%. If higher, investigate whether the MTA feed uses `departure.delay` for this route family instead and add a coalesce.
- Output: validation result logged; null rate and outlier counts documented

---

##### Preyansh — Speed Layer Watermark Join

**Task B5.1 — Write `processing/speed_layer_join.py`**
- PySpark Structured Streaming that produces the `avg_arrival_delay_secs` signal consumed by `lambda_merge.py` in Week 6
- Read from both staging streams:
  - `file:///data/staging/vehicle_positions/` (from Arjun's Task A4.1 — already bridge-enriched with `station_complex_id`)
  - `file:///data/staging/trip_delays/` (from Arjun's Task A5.1 — carries `arrival_delay_secs` per stop)
- Apply a 5-minute watermark on `timestamp` in both streams
- Stream-to-stream join on `(trip_id, stop_id)` within the watermark window to attach `arrival_delay_secs` to each `station_complex_id` event
- Aggregate per station with a **10-minute sliding window, 30-second slide**, using `avg("arrival_delay_secs")`:
  ```python
  enriched = joined \
      .withWatermark("timestamp", "5 minutes") \
      .groupBy(
          window(col("timestamp"), "10 minutes", "30 seconds"),
          col("station_complex_id")
      ) \
      .agg(avg("arrival_delay_secs").alias("avg_arrival_delay_secs"))
  ```
- Output columns exactly matching the contract consumed by Section 3.3 `process_micro_batch`:
  ```
  station_complex_id      STRING
  event_timestamp         TIMESTAMP   (= window.end)
  avg_arrival_delay_secs  DOUBLE
  ```
- **Do NOT** compute a `live_entry_rate_per_min` here. The earlier draft used a rolling count of VehiclePosition events as a turnstile proxy — that is a train-frequency metric, not passenger demand, and mixing it into the congestion score broke semantics. Passenger demand comes from the batch baseline (`demand_intensity`) and is applied in `lambda_merge.py`; the speed layer's single job is to surface `avg_arrival_delay_secs`.
- Sink to `file:///data/staging/speed_layer_delays/` with a 30-second trigger and checkpoint at `file:///checkpoints/speed_layer_delays/`
- Output: enriched stream with one row per `(station_complex_id, 30-second tick)` carrying `avg_arrival_delay_secs`

**Task B5.2 — Run 30-Minute Live Test**
- Let `speed_layer_join.py` run for 30 minutes
- Check: are rows appearing in the output? Are `station_complex_id` values populated (not null)? Are `avg_arrival_delay_secs` values plausible (typically -60 to +300)?
- Sample 10 rows and manually verify they represent real stations
- Output: test result documented; any null-join or window-boundary issues escalated

---

##### Yash — FastAPI Serving Layer

**Task C5.1 — Write `serving/main.py`**
- FastAPI app with the following routes:
  - `GET /api/v1/station/{station_id}/congestion` → returns latest MongoDB document for that station
  - `GET /api/v1/stations/all` → returns all stations with their latest `congestion_score` and `alert_level`
  - `GET /api/v1/alerts` → returns all stations where `alert_level` is MODERATE or SEVERE, sorted by `congestion_score` desc
  - `GET /health` → returns `{"status": "ok"}`
- All routes must respond in < 200ms on a cold query
- Output: `serving/main.py` running at `http://localhost:8000`, all routes return valid JSON

**Task C5.2 — Update Streamlit to Use FastAPI**
- Replace all direct MongoDB and Cassandra queries in `dashboard/app.py` with calls to the FastAPI endpoints
- Use `requests.get()` with a 5-second timeout
- Handle API errors gracefully: if the API is unreachable, show a warning banner rather than crashing
- Output: Streamlit reads all data through FastAPI with no direct database connections

---

#### Week 6

---

##### Arjun — Partition Distribution Audit & Consumer Tuning

> The 12-partition layout and the `ROUTE_PARTITION_MAP` were configured in Week 1 (Task A1.2) and Week 2 (Task A2.2). There is no repartitioning event here — doing it mid-project would force a topic delete/recreate and invalidate all Spark checkpoints. Week 6 only audits that the early decision is holding up.

**Task A6.1 — Partition Distribution Audit**
- For 30 minutes during a weekday rush hour (07:30–09:00 or 17:00–19:00), record message throughput per partition for both `gtfs-vehicle` and `gtfs-trips`:
  ```bash
  kafka-run-class.sh kafka.tools.GetOffsetShell \
      --broker-list localhost:9092 --topic gtfs-vehicle --time -1
  ```
  Take two snapshots 30 minutes apart; the delta per partition is the message count during that window.
- Compute `max_partition_count / median_partition_count`. Expected: ratio ≤ 3 (the partition map is intentionally biased — 1/2/3 and 4/5/6 partitions handle more traffic than the S/SIR partitions).
- If a single partition exceeds 40% of total traffic: re-examine the `ROUTE_PARTITION_MAP` and split the dominant group (e.g., separate `N/Q/R/W` into two partitions). If all ratios are within tolerance: pass.
- Output: partition distribution report saved to `docs/kafka_partition_audit.md`

**Task A6.2 — Consumer Lag Verification**
- Run `gtfs_monitor.py` for 30 minutes during the audit window above
- Confirm LAG ≤ 1,000 messages on every partition at each 60-second check
- If LAG climbs monotonically on any partition: increase that Spark consumer's `maxOffsetsPerTrigger` and re-check. Do not delete or recreate the topic.
- Output: 30-minute LAG log; all partitions within tolerance

---

##### Preyansh — Lambda Merge Job

**Task B6.1 — Write `get_current_weather_bucket()` (in `infra/weather_cache.py`)**
- Queries the `subway_dash.current_weather` Cassandra table — specifically the single row with `scope = 'nyc'` that Arjun's `weather_poller.py` (Task A3.3) upserts every 15 minutes. The `weather_coefficients` table is a different artifact (Week 9 historical uplift model) and must not be read here.
- Returns `"clear"`, `"rain"`, or `"snow"` as a Python string; defaults to `"clear"` if the row is missing or older than 60 minutes (stale reading).
- Module-level in-memory cache with a 15-minute TTL, so only 1 Cassandra read per 15 min — not once per micro-batch:
  ```python
  import time
  from serving.db_clients import get_cassandra_session

  _cache = {"bucket": "clear", "fetched_at": 0.0}
  _TTL_SECS = 15 * 60

  def get_current_weather_bucket() -> str:
      now = time.time()
      if now - _cache["fetched_at"] < _TTL_SECS:
          return _cache["bucket"]
      session = get_cassandra_session()
      row = session.execute(
          "SELECT weather_bucket, updated_at FROM subway_dash.current_weather WHERE scope = 'nyc'"
      ).one()
      if row and (now - row.updated_at.timestamp()) < 3600:
          _cache["bucket"] = row.weather_bucket
      else:
          _cache["bucket"] = "clear"   # stale or missing → fail safe
      _cache["fetched_at"] = now
      return _cache["bucket"]
  ```
- Output: function tested in isolation — returns correct bucket for a manually inserted test row; cache hit avoids Cassandra round-trip on second call

**Task B6.2 — Write `processing/lambda_merge.py` (full `process_micro_batch`)**
- Full implementation **exactly matching Section 3.3** of this plan — do not reintroduce the removed z-score, placeholder multipliers, or regression formulation. The canonical pipeline is:
  1. Read from the `speed_layer_delays` staging stream (Task B5.1 output): one row per `(station_complex_id, event_timestamp, avg_arrival_delay_secs)`
  2. Broadcast-load `station_capacity_baseline` and `station_max_entries` from Cassandra once at driver startup
  3. Inside `process_micro_batch(batch_df, batch_id)`:
     - Add `hour_of_day`, `day_of_week`, `weather_bucket` (driver-side scalar from `get_current_weather_bucket()`)
     - Left-join baseline on `(station_complex_id, day_of_week, hour_of_day, weather_bucket)`
     - Left-join `station_max_entries` on `station_complex_id`
     - Compute `service_deficit = clamp((avg_arrival_delay_secs - 60) / 240, 0, 1)`
     - Compute `demand_intensity = avg_entries / max_hourly_entries` (0 when max is null/zero)
     - Compute `congestion_score = service_deficit × demand_intensity` (range `[0, 1]`)
     - Compute `predicted_delay_mins = (avg_arrival_delay_secs / 60) × (1 + demand_intensity)`
     - Assign `alert_level`: `SEVERE` if score ≥ 0.50, `MODERATE` if ≥ 0.20, else `NORMAL` (Assumption A-06 — tune in Week 7)
  4. Write to MongoDB `speed_layer` collection using the schema from `docs/mongodb_schema.md` with an `inserted_at` field set to `current_timestamp()` so the 24-hour TTL index fires correctly
- Checkpoint at `file:///checkpoints/lambda_merge/`; trigger `processingTime="30 seconds"`
- Run for 1 hour; check MongoDB for inserted documents
- Output: MongoDB contains documents with `congestion_score ∈ [0, 1]` and plausible `predicted_delay_mins` values

**Task B6.3 — Validate Congestion Scores**
- Query MongoDB: find the station with the highest `congestion_score` in the last hour
- Manually check: is it a plausible station at this time of day? (e.g., Times Sq at 8 AM rush should score higher than a Brooklyn local stop at 2 AM)
- Output: sanity check result documented in `docs/phase2_validation.md`

---

##### Yash — Live Color-Coded Map

**Task C6.1 — Implement Color-Coded Markers**
- Update `dashboard/app.py`:
  - Call `GET /api/v1/stations/all` every 30 seconds
  - Map `alert_level` to RGB color: `NORMAL=(0,200,0)`, `MODERATE=(255,165,0)`, `SEVERE=(220,0,0)`
  - Pass colors to `pydeck` ScatterplotLayer `get_fill_color` field
- Output: map shows green/yellow/red dots updating live

**Task C6.2 — Add Hover Tooltip**
- Configure `pydeck` tooltip:
  ```python
  tooltip={"text": "{complex_name}\nScore: {congestion_score}\nDelay: {predicted_delay_mins} min\nStatus: {alert_level}"}
  ```
- Output: hovering over any station shows the tooltip with real values

**Task C6.3 — Manual Color Transition Test**
- Insert a synthetic document into MongoDB for Times Sq with `congestion_score=0.30` and `alert_level="MODERATE"`
- Confirm the Times Sq marker turns yellow within 35 seconds
- Insert a second document with `congestion_score=0.70` and `alert_level="SEVERE"`
- Confirm the marker turns red within 35 seconds
- Output: color transitions verified manually

---

#### Week 7

---

##### Arjun — End-to-End Smoke Test

**Task A7.1 — Write `ingestion/inject_synthetic_delay.py`**
- A standalone Python script that publishes one fake TripUpdate message to `gtfs-trips` with:
  - `stop_id`: `127N` (Times Sq northbound)
  - `arrival_delay_secs`: `600` (10-minute delay)
  - `timestamp`: current UTC time
- Output: script runs in < 5 seconds and Kafka confirms message received

**Task A7.2 — Run Full Pipeline Smoke Test**
- Step 1: Run `inject_synthetic_delay.py`
- Step 2: Start a stopwatch
- Step 3: Watch Streamlit map
- Target: Times Sq marker should turn yellow or red within 60 seconds
- Log: actual observed end-to-end latency
- If latency > 60s: identify which stage is the bottleneck (Kafka lag? Spark micro-batch duration? FastAPI response time? Streamlit poll interval?) and log the root cause
- Output: smoke test result documented; latency recorded

---

##### Preyansh — Anomaly Detection Back-Test

**Task B7.1 — Find 2 Known Historical Congestion Events**
- Search MTA service alerts archive (available at `https://api.mta.info`) or news archives for dates with documented major delays
- Suggested candidates: any A/C/E line signal failure, any large event at Madison Sq Garden, any snowstorm day
- Record: date, approximate time, affected stations
- Output: 2 events documented in `docs/phase2_validation.md`

**Task B7.2 — Back-Test Congestion Thresholds**
- Reconstruct the inputs for each of the 2 historical events:
  1. Look up `avg_arrival_delay_secs` for the affected station during the event window from archived GTFS feeds (if unavailable, estimate from service alert text — e.g., "20-minute delays" → 1200s)
  2. Look up `avg_entries` and `max_hourly_entries` from the batch baseline for that `(station, day_of_week, hour_of_day, weather_bucket)`
  3. Plug into the Section 3.3 formulas to compute `service_deficit`, `demand_intensity`, and `congestion_score`
- Verify: known MODERATE events should score in roughly `[0.20, 0.50)`; known SEVERE events should score `≥ 0.50`. Verify one "quiet baseline" hour (e.g., 3 AM Tuesday at an outer-borough stop) scores near `0.00`.
- If a known MODERATE event scores below 0.20: lower the MODERATE threshold (try 0.15) and re-test. If a known NORMAL period scores above 0.20: raise the MODERATE threshold (try 0.25). Adjust SEVERE similarly.
- Document the final validated thresholds — these replace the starting values in Assumption A-06. Do **not** attempt to tune `service_deficit`'s 60/300s endpoints in this task (Assumption A-07 is a Week 9 activity).
- Output: final validated thresholds written to `docs/phase2_validation.md` and patched into `lambda_merge.py` (single-source constants at the top of the file so they are easy to find)

---

##### Yash — Time-Series Chart & Sidebar Filter

**Task C7.1 — Add Time-Series Chart**
- Add a `st.selectbox` letting the user pick any station from the full station list
- On station selection, query `GET /api/v1/station/{id}/congestion` with a `?hours=24` parameter
- Render a `st.line_chart` of `congestion_score` over the last 24 hours
- Output: time-series chart renders for any selected station with data

**Task C7.2 — Add Subway Line Filter**
- Add a `st.multiselect` sidebar widget with all NYC subway line names (A, C, E, 1, 2, 3, etc.)
- Filter the map markers to only show stations served by selected lines
- Use the `MTA_Stations.csv` `Daytime Routes` column as the source of truth for which lines serve each station
- Output: selecting/deselecting lines updates the map immediately

**Task C7.3 — Interaction Performance Test**
- For each interactive element (station dropdown, line filter, map hover), measure response time
- Target: all interactions respond in < 2 seconds
- If any interaction is slow: check whether it is issuing redundant API calls and add `st.cache_data(ttl=30)` where appropriate
- Output: all interactions confirmed < 2 seconds

---

### Phase 3 — Hardening, Polish & Delivery (Weeks 8–10)

---

#### Week 8

---

##### Arjun — Performance Audit & Tuning

**Task A8.1 — Kafka Consumer Lag Audit**
- For 4 consecutive hours, run every 30 minutes:
  ```bash
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spark-vehicle-consumer
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group spark-trips-consumer
  ```
- Log LAG per partition at each interval
- If LAG is growing over time (not recovering): reduce Spark trigger interval or increase `maxOffsetsPerTrigger` — resolve Assumption A-11
- Output: LAG audit log; final `maxOffsetsPerTrigger` value confirmed and documented

**Task A8.2 — Spark Micro-Batch Duration Measurement**
- Open Spark UI at `localhost:4040` during a live `lambda_merge.py` run
- Record the "Duration" column for 10 consecutive micro-batches
- Target: all < 25 seconds (to fit inside the 30-second trigger)
- If any batch exceeds 25 seconds: tune `spark.sql.shuffle.partitions` — try values: 12, 24, 48 — pick the one that minimizes batch duration
- Output: final `spark.sql.shuffle.partitions` value documented

**Task A8.3 — Skew Analysis**
- Run in PySpark:
  ```python
  from pyspark.sql.functions import desc
  vehicle_positions_df.groupBy("station_complex_id").count().orderBy(desc("count")).show(20)
  ```
- Compute `max_count / median_count` ratio — resolve Assumption A-12
- If ratio > 5: implement salted join in `lambda_merge.py`
- If ratio ≤ 5: skip salting, remove the salted join from the plan, update Assumption A-12 status
- Output: skew ratio documented; salted join implemented or explicitly skipped

---

##### Preyansh — Historical Backfill

**Task B8.1 — Plan Backfill Scope**
- First, run a 1-month test backfill to measure runtime
- Estimate total runtime for 6 months and 12 months
- Choose the lookback window that fits within the available compute budget and time — resolve Assumption A-04
- Output: chosen backfill window documented

**Task B8.2 — Run Full Historical Backfill**
- Submit `build_baseline.py` for each month in the chosen window as separate Spark batch jobs
- Monitor job progress in Spark UI
- After completion: count distinct `(station_complex_id, day_of_week, hour_of_day, weather_bucket)` cells in Cassandra
- Query minimum cell sample count: any cell with < 30 samples is flagged as low-confidence (dashboard should show these as grey markers)
- Output: `station_capacity_baseline` fully populated; low-confidence cells flagged

**Task B8.3 — Baseline Accuracy Validation**
- For 5 well-known stations (Times Sq–42 St, Grand Central–42 St, Atlantic Av–Barclays Ctr, 34 St–Penn Station, Fulton St), query `avg_entries` from `station_capacity_baseline` for Wednesday 8 AM clear weather
- Compare against the actual hourly totals visible in the raw `MTA_Hourly_Ridership.csv` for the same `(station, hour, day-of-week)` cells averaged across the lookback window. The NY Open Data CSV is the source of truth; "published MTA ridership reports" in the earlier draft were ambiguous (daily totals vs. hourly).
- Expected order of magnitude (all at weekday 8 AM peak, clear weather):
  - Times Sq–42 St: ~15,000–30,000 entries/hr
  - Grand Central–42 St: ~15,000–25,000 entries/hr
  - 34 St–Penn Station: ~10,000–20,000 entries/hr
  - Fulton St: ~8,000–15,000 entries/hr
  - Atlantic Av–Barclays Ctr: ~6,000–12,000 entries/hr
- Tolerance: within ±20% of the raw CSV hourly average. Anything outside tolerance signals a unit or grouping bug in `build_baseline.py`.
- Output: accuracy validation documented in `docs/phase2_validation.md` with one row per station: `(expected_range, observed_avg_entries, pass/fail)`

---

##### Yash — Predictive UI Layer

**Task C8.1 — Add Next-Hour Forecast Widget**
- In FastAPI, add `GET /api/v1/station/{id}/forecast` — returns the batch baseline values for `(current_day_of_week, current_hour_of_day + 1, current_weather_bucket)` as the next-hour prediction
- In Streamlit, display next to the current score: "Next Hour Forecast: score X.X (MODERATE)"
- Output: forecast widget showing plausible values for the next hour

**Task C8.2 — Implement Alert Banner**
- Call `GET /api/v1/alerts` on every Streamlit refresh
- If any SEVERE station exists: `st.error(f"⚠ SEVERE congestion at {n} stations — {station_names}")`
- If any MODERATE station exists and no SEVERE: `st.warning(f"Moderate congestion at {n} stations")`
- If all clear: `st.success("No active congestion alerts")`
- Output: alert banner displays correctly for all three states (verified with synthetic MongoDB inserts)

**Task C8.3 — Data Freshness Indicator**
- In the sidebar, display: `"Data as of: {X} seconds ago"` — compute from `inserted_at` field of the most recent MongoDB document
- If `inserted_at` is more than 90 seconds old: show indicator in red ("⚠ Data may be stale")
- Output: freshness indicator visible and accurate

---

#### Week 9

---

##### Arjun — Containerization & Documentation

**Task A9.1 — Write Final `docker-compose.yml`**
- All services in one file: Zookeeper, Kafka (12 partitions pre-configured), Spark master, Spark worker, MongoDB, Cassandra, FastAPI, Streamlit, `weather_poller` (NWS), `gtfs_producer`
- Add health checks for Kafka, Cassandra, and MongoDB so dependent services wait for them to be ready (use `depends_on` with `condition: service_healthy`)
- Volume mounts required for persistence and correctness:
  - Cassandra data (`cassandra_data:/var/lib/cassandra`)
  - MongoDB data (`mongo_data:/data/db`)
  - Kafka logs (`kafka_data:/var/lib/kafka/data`)
  - **Spark streaming checkpoints** (`spark_checkpoints:/checkpoints`) — critical. Without this, every `docker-compose up` re-runs streams from the earliest Kafka offset and produces stale documents. Every Structured Streaming job in this project writes to `/checkpoints/<job_name>/` and the path must survive container restarts.
  - **Staging data** (`spark_staging:/data/staging`) — vehicle_positions, trip_delays, and speed_layer_delays all write here; downstream stages read from here
  - Mount the project's `data/bridge/` and `data/raw/` directories read-only into the Spark worker container so bridge table and source CSVs are reachable
- Output: `docker-compose up` starts all services; all report healthy within 3 minutes; a subsequent `docker-compose down && docker-compose up` resumes streaming jobs from saved checkpoints (verify: no re-processing of already-consumed offsets)

**Task A9.2 — Write `README.md`**
- Sections: Prerequisites, Setup (one-command start), Configuration (env variables), Architecture diagram (ASCII or Mermaid), Troubleshooting (top 5 common errors with fixes)
- Output: `README.md` reviewed by Yash and Preyansh; both can start the stack from scratch using only the README

**Task A9.3 — Write Kafka Topic Config Documentation**
- File: `docs/kafka_config.md`
- Document: topic names, partition counts, retention settings, producer partition key logic, consumer group names
- Output: `docs/kafka_config.md` committed

---

##### Preyansh — Weather Model Finalization & Analytics Report

**Task B9.1 — Compute Weather Multipliers**
- From `ridership_weather_baseline` Parquet, group by `(station_complex_id, weather_bucket)` and compute `avg(entries)` for each bucket
- Compute multiplier: `multiplier = avg_entries_rain / avg_entries_clear` (and `avg_entries_snow / avg_entries_clear`)
- Example expected output: rain multiplier ≈ 0.88 (ridership drops ~12% in rain); snow multiplier ≈ 0.70
- Write multipliers to Cassandra `weather_coefficients` table
- Output: `weather_coefficients` table populated

**Task B9.2 — Calibrate `service_deficit` Endpoints (Assumption A-07)**
- The Section 3.3 formula hard-codes a 60-second grace and 300-second saturation for the `service_deficit` ramp. Week 9 task is to validate or revise those two endpoints against observed data — NOT to introduce a new regression, and NOT to re-tune the alert thresholds (that's Task B7.2).
- From the `trip_delays` staging data collected across Weeks 5–8, compute the empirical distribution of `arrival_delay_secs` per station during *normal* operating windows (no service alerts active):
  - Let `p50` and `p95` be the network-wide percentiles of `arrival_delay_secs`
  - Propose new endpoints: `grace = max(30, p50)` and `saturation = max(240, p95)` — ensures the ramp doesn't fire on "normal" delay noise and does saturate at delays riders recognize as severe
- Cross-check the new endpoints against the 2 historical events from Task B7.1: MODERATE events should still land `service_deficit > 0.4`; SEVERE events `service_deficit > 0.8`.
- If the empirical data shifts endpoints by < 15% from the defaults (60/300): leave the defaults in place and mark Assumption A-07 as CONFIRMED with no change.
- If shifts are larger: update the endpoints in `lambda_merge.py` (single-source constants) and document the change.
- Output: endpoint calibration result documented in `docs/phase3_validation.md`; `lambda_merge.py` reflects final values

**Task B9.3 — Write Analytics Report Methodology Section**
- File: `docs/analytics_report.md`
- Sections to complete this week:
  1. System architecture (with diagram — show batch path, speed path, merge point inside `process_micro_batch`, and the single write to MongoDB)
  2. Data sources and ingestion (MTA GTFS-Realtime via public HTTP, MTA Hourly Ridership, MTA Stations Dataset, NOAA CDO for historical weather, NWS for real-time weather)
  3. Spatial join methodology (3-tier approach, actual coverage percentages measured in Week 2)
  4. Batch layer: baseline computation, daily NOAA weather join, `station_max_entries` derivation
  5. Speed layer: watermark stream-to-stream join, direct use of GTFS `arrival.delay`, 10-minute sliding average
  6. Lambda merge: the dimensionless product formulation (`service_deficit × demand_intensity`), 60/300s calibration results from B9.2, final MODERATE/SEVERE thresholds from B7.2
  7. Weather coefficient model (from Task B9.1 — rain/snow ridership uplift for historical reporting, distinct from the live `current_weather` lookup)
  8. Assumptions Registry: final status of all 16 entries — CONFIRMED, REVISED (with new values), or UNRESOLVED (with why)
- Output: draft report complete through Section 8

---

##### Yash — User Acceptance Testing

**Task C9.1 — Run 2-Hour UAT Session**
- Start full stack with `docker-compose up`
- For 2 hours, monitor the live dashboard and log every issue observed:
  - Latency spikes (marker > 60s stale)
  - Broken map tiles
  - API 500 errors
  - Stale data indicator firing incorrectly
  - Alert banner showing wrong state
- Output: `docs/uat_log.md` with timestamp, issue description, and severity (P0/P1/P2) for each issue

**Task C9.2 — Fix All P0 Bugs**
- P0 definition: anything that makes the dashboard non-functional or shows completely wrong data
- Fix each P0 bug before Week 10; do not proceed to polish until P0s are resolved
- Output: P0 bug count = 0 after fixes

**Task C9.3 — Test Empty State Handling**
- Stop the GTFS producer for 5 minutes while the dashboard is running
- Verify: dashboard shows the "Data may be stale" warning, does not crash, and shows the last known state
- Restart the producer; verify the dashboard recovers automatically within 35 seconds
- Output: empty state and recovery both confirmed working

---

#### Week 10

---

##### Arjun — Final Integration Test

**Task A10.1 — Cold Start Test**
- Stop all services: `docker-compose down -v` (this clears all data volumes)
- Start fresh: `docker-compose up`
- Clock how long until the dashboard shows live station data
- Target: fully operational within **8 minutes**. The startup path is: Zookeeper → Kafka (30–60s) → Cassandra schema initialization (30s) → MongoDB TTL index creation (5s) → Spark master + worker (30s) → GTFS producer first poll (15s) → weather poller first fetch (up to 15 min) → Spark streaming jobs first micro-batch (30s trigger + initial query planning, typically 1–2 min) → FastAPI ready → Streamlit first refresh (up to 30s). 5 minutes is aggressive for a cold cluster; 8 minutes is the realistic end-to-end target.
- The Streamlit dashboard will legitimately show an empty-state banner for the first few minutes — this is not a failure. Failure means something crashed or a container never passed its health check.
- Output: cold start time documented; any failures during startup fixed. If repeatedly > 10 minutes, profile which container is the bottleneck.

**Task A10.2 — 24-Hour Pipeline Health Check**
- Let the full stack run for 24 hours unattended
- Check every 6 hours: DLQ message count, Kafka LAG, Spark job status, MongoDB document count
- Output: 24-hour health report — any recurring failures documented and fixed

**Task A10.3 — Final Submission Package Assembly**
- Verify all code is committed to the shared Git repo
- Tag the final release: `git tag v1.0.0`
- Output: release tag pushed; repo link shared with team

---

##### Preyansh — Analytics Report Completion

**Task B10.1 — Complete Analytics Report**
- Add remaining sections to `docs/analytics_report.md`:
  8. Performance results (micro-batch latency, Kafka LAG audit results, cold start time)
  9. Limitations and future work (real-time turnstile integration, borough-level weather, GTFS subway extension)
  10. Lessons learned
- Output: complete `docs/analytics_report.md`, minimum 8 pages

**Task B10.2 — Final Assumptions Registry Audit**
- Go through all 14 assumptions in Section 7 of this plan
- For each: mark as CONFIRMED (with final value), REVISED (with new value), or UNRESOLVED (with explanation)
- Add the final status to the Analytics Report as an appendix
- Output: all 14 assumptions have a documented final status

---

##### Yash — Final Polish & Demo

**Task C10.1 — Final Streamlit Polish**
- Add MTA subway line color circles next to each station name in the tooltip (use official MTA brand colors)
- Add a loading spinner (`st.spinner`) that shows during the 30-second data refresh
- Add empty-state message: "No congestion data available yet — pipeline may be starting up" if the API returns no stations
- Ensure all text is readable on both light and dark backgrounds
- Output: polished dashboard reviewed by Arjun and Preyansh

**Task C10.2 — Record Demo Video**
- Record a 3-minute screen capture covering:
  1. Cold start: `docker-compose up` → dashboard comes online (speed through this)
  2. Normal state: green markers on the map
  3. Inject a synthetic SEVERE congestion event using `inject_synthetic_delay.py`
  4. Show the marker turning red, the alert banner appearing, and the time-series chart updating
  5. Show the hover tooltip with congestion score and predicted delay
- Output: demo video file saved to `docs/demo.mp4`

**Task C10.3 — Assemble Final Submission**
- Compile final submission package:
  ```
  subway-dash-final/
  ├── README.md                  (Arjun)
  ├── docker-compose.yml         (Arjun)
  ├── ingestion/                 (Arjun)
  ├── processing/                (Preyansh)
  ├── serving/                   (Yash)
  ├── dashboard/                 (Yash)
  ├── infra/                     (Arjun)
  ├── data/bridge/               (Preyansh)
  ├── docs/
  │   ├── analytics_report.md   (Preyansh)
  │   ├── join_key_map.md        (Preyansh)
  │   ├── mongodb_schema.md      (Yash)
  │   ├── kafka_config.md        (Arjun)
  │   ├── uat_log.md             (Yash)
  │   └── demo.mp4               (Yash)
  ```
- Output: final package submitted
