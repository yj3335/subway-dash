# Phase 3 / Week 8 — Track A Live Staging Audit

**Run window:** 2026-05-06 01:15:23 UTC → 01:49:26 UTC (~34 minutes)
**Operator:** Arjun
**Goal:** Generate a longer staging sample so Track B can run speed-layer
join + Lambda merge end-to-end, and capture the metrics needed for the
Phase 3 performance / skew / calibration decisions.

Raw metrics: [`logs/audit_metrics.json`](../logs/audit_metrics.json).
Process logs: `logs/run_{producer,weather,vehicle,trips}.log`.

---

## What ran

| Process | Command | Uptime |
|---|---|---|
| MTA GTFS producer | `python -m ingestion.gtfs_producer` | 34:03 |
| NWS weather poller | `python -m ingestion.weather_poller` | 34:03 |
| Vehicle Spark consumer | `python -m processing.gtfs_vehicle_consumer --starting-offsets earliest` | 34:03 |
| Trips Spark consumer | `python -m processing.gtfs_trips_consumer --starting-offsets earliest` | 34:03 |

Stack: Docker Compose (Zookeeper, Kafka 7.5, Spark 3.5 master+worker,
Mongo 6, Cassandra 4.1) — all containers stayed healthy.

**Errors / exceptions in any of the 4 process logs: 0.**

---

## Top-line numbers

| Metric | Value |
|---|---|
| Producer cycles | 132 |
| Vehicles published to Kafka | 65,424 (producer log) / **73,286** (Kafka delta) |
| Trips published to Kafka | 76,477 (producer log) / **85,676** (Kafka delta) |
| Alerts published | 150 |
| **DLQ messages** | **0** |
| Weather observations | 3 (1 startup + 2 cycles at 15-min cadence) |

The Kafka-delta numbers run higher than the producer-log totals because a
few warm-up messages were already in the topics from the earlier session.
The producer-log totals are the cleanest "this run" figure.

Throughput: **≈36 vehicles/sec, ≈42 trips/sec** sustained. Each producer
cycle averaged 1.7–2.3 seconds wall time inside the 15-second tick.

---

## Staging output (for Track B)

### `data/staging/vehicle_positions/`

| Metric | Value | Plan target | Status |
|---|---|---|---|
| Rows written | **77,150** | — | ✅ |
| Distinct `station_complex_id` | 445 (of 496 in bridge = 89.7%) | — | ✅ |
| Distinct `route_id` | 30 | — | ✅ |
| Null `station_complex_id` | 2,357 (**3.06%**) | < 5% (A4.2) | ✅ PASS |
| Earliest `event_timestamp` | 2026-05-05 19:21:50 UTC | — | (early backfill) |
| Latest `event_timestamp` | 2026-05-06 01:53:54 UTC | — | |

### `data/staging/trip_delays/`

| Metric | Value | Plan target | Status |
|---|---|---|---|
| Rows written | **72,909** | — | ✅ |
| Distinct `trip_id` | 130 | — | |
| Distinct `stop_id` | 47 | — | (small because trips consumer also has the 30-min replay; full station coverage will widen with longer runs) |
| Null `arrival_delay_secs` | **0 (0.0%)** | < 10% (A5.2 / A-16) | ✅ **A-16 CONFIRMED** |
| `delay_min` | -141s (early) | — | within plausible band |
| `delay_p50` | **0s** | — | trains run on schedule median |
| `delay_p95` | **107s** | — | see calibration discussion below |
| `delay_p99` | 139s | — | |
| `delay_max` | 600s | — | (the synthetic Times Sq fixture; real-feed max ≈ 139s for this window) |

---

## Kafka partition distribution (A6.1)

Per-partition message counts at end of run (offsets, not deltas):

### `gtfs-vehicle`

| Partition | Routes | Count |
|---|---|---|
| 0 | A, C, E | 12,341 |
| 1 | B, D, F, M | **14,126** ← max |
| 2 | G | 3,284 |
| 3 | J, Z | 3,362 |
| 4 | L | 2,542 |
| 5 | N, Q, R, W | 12,978 |
| 6 | 1, 2, 3 | 8,535 |
| 7 | 4, 5, 6 | 8,974 |
| 8 | 7 | 2,408 |
| 9 | S, FS, GS, H (shuttles) | 7,495 |
| 10 | SIR, SI | 1,100 |
| 11 | overflow / null route_id | 5 |

- **`max / median = 14126 / 5428.5 = 2.60×`** — within plan target ≤ 3.0 ✅
- Partition 11 is essentially empty (5 records) — confirms the hash-fallback
  decision pays off: unknown routes distribute uniformly across 0–11
  rather than piling onto an overflow bucket.
- The 12-partition layout decided on Day 1 is holding under sustained load.
  **No repartitioning event needed.**

### `gtfs-trips`

| Partition | Count |
|---|---|
| 0 | 12,344 |
| 1 | **14,134** ← max |
| 2 | 3,291 |
| 3 | 3,364 |
| 4 | 4,214 |
| 5 | 12,981 |
| 6 | 12,671 |
| 7 | 12,410 |
| 8 | 4,114 |
| 9 | 9,588 |
| 10 | 1,110 |
| 11 | 10 |

- **`max / median = 14134 / 6901 = 2.05×`** — well inside ≤ 3.0 ✅

**Resolution: Assumption A-12 (partition skew)** — confirmed acceptable.
No partition split needed.

---

## Station-level skew (A8.3)

Counted vehicle-position events per `station_complex_id` (post-bridge join).
This is the metric that drives the salted-join decision in Track B's
`lambda_merge.py`.

| Metric | Value |
|---|---|
| Max events at one station | 3,314 (station 58 = Coney Island–Stillwell Av) |
| Median events per station | 77 |
| **Skew ratio (max / median)** | **43.04×** |
| Plan threshold (Section 5 / A-12) | 5× |

### Top 10 busiest stations (vehicle events)

| ID | Count | Station | Routes |
|---|---|---|---|
| 58 | 3,314 | Coney Island–Stillwell Av | D F N Q |
| 627 | 3,297 | Franklin Av (Brooklyn shuttle terminus) | C |
| 278 | 1,819 | Jamaica Center–Parsons/Archer | E J Z |
| 624 | 1,468 | Park Place (Brooklyn) | 2 3 |
| 261 | 1,296 | Forest Hills–71 Av | E F M R |
| 42 | 1,149 | Prospect Park | B Q S |
| 606 | 1,126 | Court Sq | 7 |
| 254 | 1,071 | Jamaica–179 St | F |
| 625 | 1,059 | Delancey St–Essex St | F |
| 143 | 999 | Inwood–207 St | A |

> **Important nuance — these are line *terminals*, not high-ridership
> stations.** GTFS-Realtime emits more VehiclePosition events per
> trip-end at terminals (multiple `current_status` transitions, longer
> dwell times). The actual *ridership* leaders (Times Sq–42 St
> [`station_complex_id` 611], Grand Central, Atlantic Av–Barclays)
> appear lower because they have shorter dwells. Times Sq is at #11
> with 969 events.
>
> **For salted-join decisions in `lambda_merge.py`:** the keys that
> matter are the ones with highest *demand_intensity × event count*,
> which means baseline-busy stations like Times Sq, not terminals.
> But since terminals dominate the raw stream too, salting helps both.

### Recommendation for Preyansh (Track B)

The 43× skew is well above the 5× threshold from the plan. **Implement
a salted join in `lambda_merge.py`** — the broadcast-baseline join
itself isn't the issue (baseline is ~500 rows broadcast to all executors),
but the upstream `groupBy("station_complex_id")` aggregation in
`speed_layer_join.py` will straggle on these terminals.

Two cheap-fixes worth trying first before salting:
1. **Increase `spark.sql.shuffle.partitions`** beyond 12 so straggler
   stations can shard across more reducers. Try 24, 48.
2. **AQE (`spark.sql.adaptive.enabled=true`,
   `spark.sql.adaptive.skewJoin.enabled=true`)** — Spark 3.5 handles
   skew automatically when this is on.

If both fail, then implement the 10-way salt described in Section 5
of the plan. **Resolves Assumption A-12 as REVISED: skew exists at
43×, but salting deferred pending AQE / shuffle-partitions tuning.**

---

## `arrival_delay_secs` calibration (Assumption A-07)

The plan hard-codes `service_deficit` as ramping from 60s grace to 300s
saturation. This run gives the first real distribution:

| Percentile | Delay (s) |
|---|---|
| min | -141 (early) |
| p50 | 0 |
| p95 | 107 |
| p99 | 139 |
| max (excl. synthetic) | 139 |

### What this means

- **The 60s "grace" endpoint looks correct.** p50 = 0, so half of
  observations are at-or-ahead of schedule. Most riders won't experience
  the grace zone trigger.
- **The 300s "saturation" endpoint is too aggressive for normal operation.**
  Even at p99, real delays don't exceed 139s. With the current formula,
  `service_deficit ≥ 0.8` would only fire on outliers — the score range
  used in production is effectively `[0, 0.33]` not `[0, 1]`.
- **Suggested revision (Preyansh, Task B9.2):**
  - `grace = 60s` (keep; matches p50 clear-sky baseline)
  - `saturation = 240s` (lower from 300) — would map p99 → ~0.44, leaving
    headroom for genuinely anomalous events while making the score range
    actually exercised
  - Re-run on a day with active service alerts before locking in.

Important caveat: this 34-minute window did not include any active
service-disruption alerts (alerts topic = 150 messages but those were
mostly elevator outages and weekend-service notices, not delay alerts).
The p95/p99 numbers above represent **steady-state** behavior. The
calibration that matters is the distribution during a real event —
schedule another live run during a known disruption before finalizing
the endpoints.

---

## Weather poller

3 cycles in 34 minutes (1 startup + 2 at the 15-min interval). All wrote:

- `weather_bucket = clear`
- `prcp_in = 0.0`
- `tmax_f = None` (NWS observation didn't include a temperature reading
  for this poll cycle — fallback path; the field is nullable)

Cassandra `current_weather` row updated 3 times. Kafka `weather-feed`
topic at 4 messages total (1 left over from earlier session + 3 new).

The `tmax_f = None` case is worth noting — sometimes NWS returns a null
`temperature.value`. The poller correctly stores null rather than a
sentinel. The `current_weather` row's `tmax_f` is unused by the speed
layer (it only reads `weather_bucket`), so this is informational only.

---

## Spark Structured Streaming behavior

| Metric | Value |
|---|---|
| Checkpoint commits — vehicle | 69 |
| Checkpoint commits — trips | 69 |
| Mid-run errors | 0 |
| Memory (RSS) — vehicle consumer | 7.2 MB driver (worker memory inside JVM, not measured here) |
| Memory (RSS) — trips consumer | 7.2 MB driver |

69 commits across ~34 minutes = a commit every ~30 seconds, exactly the
trigger interval. **Both consumers tracked the producer in real time** —
no Kafka LAG buildup, no missed batches, both restartable from
checkpoint.

The vehicle consumer wrote ~199 part-files; trips wrote ~19 part-files.
The asymmetry is expected: trips' `explode(stop_time_update)` produces
more rows per Kafka record (one row per stop visit per trip) but fewer
microbatch parquet files because Spark coalesces small partitions on
write.

---

## Status of assumptions touched by this run

| Assumption | Owner | Resolution |
|---|---|---|
| **A-12** (Spark skew warrants salting) | Arjun | **REVISED** — partition skew 2.6× (well within 3× target), but station-level skew 43× → salted join recommended for `lambda_merge.py` after first trying `spark.sql.adaptive.skewJoin.enabled=true`. |
| **A-16** (`arrival.delay` populated > 90%) | Arjun | **CONFIRMED** — 0% null over 72,909 rows. |
| **A-07** (60s/300s `service_deficit` endpoints) | Preyansh | **REVISION CANDIDATE** — `saturation=240s` better matches steady-state p99 of 139s; needs validation against an active-disruption run. |
| **A-11** (`maxOffsetsPerTrigger=5000`) | Arjun | **CONFIRMED ADEQUATE** — Kafka throughput ~80 msg/s on each topic, well below 5000-per-trigger limit. No backpressure observed. |
| **A-10** (30s micro-batch achievable) | Arjun | **CONFIRMED** — 69 commits in 34 minutes ≈ 29.5s avg trigger, no missed slots. |
| **A-13** (~470 station complexes) | Preyansh | Bridge has 496 entries; we observed 445 distinct stations in the live stream (89.7% coverage in 34 minutes). Long-tail stations would appear with longer runtime. |

---

## Hand-off

Track B and Track C can now run end-to-end against:

- `data/staging/vehicle_positions/event_date=2026-05-05` and `event_date=2026-05-06` — 77,150 rows
- `data/staging/trip_delays/event_date=2026-05-05` and `event_date=2026-05-06` — 72,909 rows
- `data/bridge/station_bridge.parquet/` — 496-row bridge (unchanged)

Both staging dirs are committed; `_spark_metadata/` is gitignored
(see previous PR), so the absolute-path issue Preyansh hit earlier
will not recur.

The `data/staging/checkpoints/` directories are **NOT** committed
(intentional — they reference local volume mounts). Track B should
remove its own local checkpoints before reading from the staging
Parquet files as a batch source.
