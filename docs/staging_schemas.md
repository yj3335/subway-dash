# Spark Staging Parquet Schemas

**End-of-Week-4 / End-of-Week-5 handoff** from Track A (Arjun) → Track B (Preyansh).

Track A's PySpark Structured Streaming consumers read from Kafka, parse the
JSON envelopes produced by `ingestion.gtfs_producer`, and sink Parquet to a
shared staging directory. Track B's `processing/speed_layer_join.py` reads
from these paths — so the column names, types, and semantics below are a
contract. Any change here must be coordinated.

Default base directory: `data/staging/` (override with `SUBWAY_DASH_DATA_DIR`).

---

## `data/staging/vehicle_positions/`

Producer: [`processing/gtfs_vehicle_consumer.py`](../processing/gtfs_vehicle_consumer.py)

- Source Kafka topic: `gtfs-vehicle`
- Trigger: `processingTime="30 seconds"`
- Output mode: `append`
- Partitioned by: `event_date`
- Checkpoint: `checkpoints/vehicle_positions/`

| Column | Type | Nullable | Source | Notes |
|---|---|---|---|---|
| `trip_id` | string | yes | `trip.trip_id` from VehiclePosition Protobuf | matches `trip_id` in `trip_delays` for stream-stream join |
| `route_id` | string | yes | `route_id` (top-level), falling back to `trip.route_id` | one of the standard NYC line codes (A, 1, L, SIR, etc.) |
| `vehicle_id` | string | yes | `vehicle.id` | MTA-internal vehicle handle |
| `stop_id` | string | yes | `stop_id` (raw, with direction suffix) | e.g., `127N`, `127S` |
| `stop_id_base` | string | yes | `regexp_replace(stop_id, '[NS]$', '')` | the directionless GTFS stop id used for the bridge join |
| `current_status` | string | yes | `current_status` enum from VehiclePosition | values: `INCOMING_AT`, `STOPPED_AT`, `IN_TRANSIT_TO` |
| `current_stop_sequence` | long | yes | `current_stop_sequence` | 1-indexed position within the trip |
| `event_timestamp` | timestamp (UTC) | no | `from_unixtime(timestamp)` if present, else Kafka receive ts | use this for windowing — NEVER `kafka_ts` directly |
| `event_date` | date | no | `to_date(event_timestamp)` | partition column |
| `station_complex_id` | string | yes | broadcast bridge join on `stop_id_base = bridge.gtfs_stop_id` | **may be null** — Tier-1 join misses (target < 5%) |
| `complex_name` | string | yes | from bridge | e.g., "Times Sq-42 St" |
| `lat` | double | yes | from bridge | station-complex centroid (not vehicle position) |
| `lon` | double | yes | from bridge | station-complex centroid (not vehicle position) |

**Validated end-to-end (this PR):** 2,780 rows / 2.6% null `station_complex_id`
on a ~2-minute live run. Below the 5% escalation threshold in Task A4.2.

**Important nuance:** `lat`/`lon` come from the **bridge table** (the station
complex location), not from VehiclePosition. The Protobuf does carry a live
vehicle lat/lon under `position.{latitude,longitude}` but it is not currently
extracted because the speed-layer score is keyed on `station_complex_id`, not
on continuous position. If/when a "trains in motion" view is built, add
`vehicle_lat` / `vehicle_lon` columns rather than overloading these.

---

## `data/staging/trip_delays/`

Producer: [`processing/gtfs_trips_consumer.py`](../processing/gtfs_trips_consumer.py)

- Source Kafka topic: `gtfs-trips`
- Trigger: `processingTime="30 seconds"`
- Output mode: `append`
- Partitioned by: `event_date`
- Checkpoint: `checkpoints/trip_delays/`

The consumer explodes the `stop_time_update` array — **one row per stop per
trip update**, not one row per TripUpdate entity.

| Column | Type | Nullable | Source | Notes |
|---|---|---|---|---|
| `trip_id` | string | yes | `trip.trip_id` from TripUpdate Protobuf | join key with `vehicle_positions` |
| `route_id` | string | yes | `route_id` (top-level), falling back to `trip.route_id` | |
| `stop_id` | string | **no** | `stop_time_update.stop_id` | filtered: rows with null `stop_id` are dropped |
| `stop_sequence` | int | yes | `stop_time_update.stop_sequence` | |
| `arrival_delay_secs` | int | **no** | `stop_time_update.arrival.delay` | filtered: rows with null delay are dropped (validates A-16); negative = early; 0 = on time; positive = late |
| `departure_delay_secs` | int | yes | `stop_time_update.departure.delay` | retained for coalesce fallback if `arrival.delay` proves unreliable |
| `arrival_time_epoch` | long | yes | `stop_time_update.arrival.time` | raw, kept for debugging timestamp math |
| `event_unix_ts` | long | yes | TripUpdate `timestamp` (epoch seconds) | |
| `event_timestamp` | timestamp (UTC) | no | `from_unixtime(event_unix_ts)` if present, else Kafka receive ts | use for watermark / windowing |
| `event_date` | date | no | `to_date(event_timestamp)` | partition column |
| `kafka_ts` | timestamp (UTC) | no | Kafka record receive timestamp | escape hatch only — do **not** join on this |

**Critical:** the consumer extracts `arrival.delay` **directly**. Do not
re-derive `dwell_time = arrival_time_actual − arrival_time_scheduled` —
GTFS-Realtime does not carry a scheduled timestamp per update, and the delay
field is already what we want in seconds. This decision is locked in by
Section 3.3 of the plan.

**Expected distribution** (per Task A5.2 validation): most `arrival_delay_secs`
values fall in `[-120, +600]`. Outliers > 1800 indicate stale TripUpdates and
should be flagged but not dropped (the speed-layer averaging absorbs them).

---

## `data/staging/speed_layer_delays/`

Producer: `processing/speed_layer_join.py` (Track B — Preyansh, Task B5.1)

Listed here because it consumes from the two staging paths above and the
contract closes at this output. **This is not Track A's writer.**

| Column | Type | Source |
|---|---|---|
| `station_complex_id` | string | from `vehicle_positions` (post-bridge-join) |
| `event_timestamp` | timestamp | window end (10-min sliding, 30s slide) |
| `avg_arrival_delay_secs` | double | `avg(arrival_delay_secs)` over the window |

Track B's `lambda_merge.py` then consumes from
`data/staging/speed_layer_delays/` and the Cassandra batch baseline to
produce the final MongoDB `speed_layer` documents.

---

## Operational notes

- **Partitioning by `event_date`** keeps the directory layout compatible with
  Hive-style discovery, so Spark and Pandas both read it without extra
  configuration. Old partitions can be deleted by removing the date directory.
- **Schema evolution:** any added column should default-null on read (we use
  `mergeSchema=true` in downstream readers). Removing or renaming a column is
  a breaking change — coordinate with Track B and rebuild checkpoints.
- **Restarting a consumer** picks up from its checkpoint. **Do not delete the
  checkpoint directory** unless you intend a full backfill from
  `--starting-offsets earliest`.
