# Subway Dash Analytics Report

## 1. System Architecture

Subway Dash uses a Lambda architecture. Batch jobs build expected station demand from historical ridership and daily NOAA weather. Speed-layer jobs aggregate live GTFS-Realtime arrival delays. The Lambda merge happens once in `processing/lambda_merge.py`, which writes final congestion documents to MongoDB for the dashboard.

## 2. Data Sources

- MTA GTFS static `stops.txt` maps stop IDs and coordinates.
- MTA Subway Stations provides GTFS stop IDs, station complex IDs, names, routes, boroughs, and coordinates.
- MTA Subway Hourly Ridership provides historical station-complex ridership by hour.
- NOAA CDO provides historical daily precipitation and snow for baseline weather buckets.
- NWS real-time observations populate Cassandra `current_weather` through the weather poller.

## 3. Spatial Join Methodology

The bridge table resolves GTFS stop IDs to MTA station complex IDs using three tiers:

1. Direct normalized GTFS stop ID match.
2. Levenshtein name match for direct misses.
3. Haversine nearest-station match within a configurable radius.

The current bridge build resolved all `496` normalized GTFS stops through the
direct stop-ID tier:

| Match tier | Stops | Share |
|---|---:|---:|
| Direct GTFS stop ID | 496 | 100.00% |
| Levenshtein name match | 0 | 0.00% |
| Geospatial nearest match | 0 | 0.00% |
| Unresolved | 0 | 0.00% |

The resulting unresolved rate is `0.00%`, so the bridge table is ready for
batch baseline validation, staging enrichment, and dashboard station metadata.

## 4. Batch Layer

The baseline job keeps `station_complex_id` from the ridership dataset. It does not join ridership through the bridge table. Daily NOAA weather joins on `date`, then the job groups by station, Spark day-of-week, hour, and weather bucket to produce `avg_entries`, `std_entries`, and `p95_entries`. A second table stores each station's historical `max_hourly_entries` for demand normalization.

## 5. Speed Layer

`processing/speed_layer_join.py` joins bridge-enriched vehicle positions to trip-delay rows on `trip_id`, `stop_id`, and a five-minute event-time window. It aggregates a 10-minute sliding average of `arrival_delay_secs` per station.

## 6. Lambda Merge

`processing/lambda_merge.py` computes:

```text
service_deficit = clamp((avg_arrival_delay_secs - 60) / 240, 0, 1)
demand_intensity = avg_entries / max_hourly_entries
congestion_score = service_deficit * demand_intensity
predicted_delay_mins = (avg_arrival_delay_secs / 60) * (1 + demand_intensity)
```

Current operational thresholds are MODERATE at `0.20` and SEVERE at `0.50`.
Phase 2 validation confirmed that Lambda output stays in `[0, 1]`, produces
plausible predicted delays, and supports the dashboard alert color path. The
larger Phase 3 run produced all three alert classes in MongoDB:

| Alert level | Documents |
|---|---:|
| NORMAL | 2,875 |
| MODERATE | 40 |
| SEVERE | 1 |

## 7. Weather Coefficients

Weather coefficients are an analytics artifact computed from `ridership_weather_baseline`. They are not used for the real-time weather lookup in Lambda merge.

## 8. Assumptions Registry

Generate the audit template with:

```bash
python -m processing.audit_assumptions
```

Validation status:

| ID | Assumption | Status | Evidence |
|---|---|---|---|
| A-01 | Bridge-table coverage | CONFIRMED | `496/496` normalized GTFS stops resolved; unresolved rate `0.00%`. |
| A-04 | Baseline sample density | CONFIRMED | `station_capacity_baseline` contains `213,268` rows across clear/rain/snow buckets. |
| A-06 | Alert thresholds | CONFIRMED FOR DEMO | Phase 2 and Phase 3 runs produced valid score ranges and visible dashboard alert states at the configured thresholds. |
| A-07 | Service deficit endpoints | CONFIRMED FOR STEADY STATE | Larger run p50=`58.00s`, p95=`141.53s`, p99=`281.00s`; keep `60s` grace and `300s` saturation. |
| A-08 | Demand intensity normalization | CONFIRMED | `station_max_entries` contains `424` station maxima and no validation failures. |
| A-10 | 30-second micro-batch cadence | CONFIRMED | Streaming outputs aligned with the dashboard refresh cadence during validation. |
| A-12 | Skew handling | REVISED | Station-level input skew measured at `43x`; Spark AQE is enabled and salting is deferred unless runtime stragglers appear. |
| A-13 | Station count | REVISED | Bridge has `496` normalized stops; validation observed `445` distinct live station complexes in the staging run. |
| A-16 | Arrival-delay availability | REVISED | Native delay fields are not uniform across routes; the trips consumer computes delay from static schedule lookup when needed. |

## 9. Performance Results

### Batch Validation

| Output | Result |
|---|---:|
| Cleaned ridership rows | 4,749,454 |
| Cleaned weather rows | 477 |
| Ridership-weather joined rows | 4,749,454 |
| `station_capacity_baseline` rows | 213,268 |
| `station_max_entries` rows | 424 |
| Cassandra baseline rows written | 213,268 capacity rows, 424 max-entry rows |

### Streaming Validation

| Check | Result |
|---|---:|
| Vehicle staging rows | 77,150 |
| Distinct live station complexes observed | 445 |
| Trip delay staging rows in smoke test | 2,393 |
| Finite speed-layer smoke-test rows | 377 |
| Larger speed-layer rows | 2,538 |
| Null station IDs in larger speed-layer output | 0 |
| Highest station-window output count | 137 |

### Delay Distribution

| Metric | Seconds |
|---|---:|
| p50 | 58.00 |
| p95 | 141.53 |
| p99 | 281.00 |
| max | 347.00 |

### Serving And Dashboard

| Check | Result |
|---|---|
| FastAPI `/health` | PASS, MongoDB and Cassandra reachable |
| FastAPI `/api/v1/stations/all` | PASS, returned latest station documents |
| FastAPI `/api/v1/alerts` | PASS, returned active alert documents |
| Dashboard render | PASS, Streamlit displayed MongoDB-backed station state |
| Synthetic severe alert | PASS, Times Sq-42 St rendered as SEVERE |

## 10. Limitations and Lessons Learned

The system produces a working station-level congestion signal, but it is still
bounded by the quality and shape of the public transit feeds. GTFS-Realtime
fields are not uniform across routes, so the trips consumer must combine native
delay fields with static-schedule-derived delay estimates. This makes the delay
signal useful for station-level aggregation, but not a replacement for exact
train operations data.

Station-level stream skew is measurable: validation saw a `43x` max-to-median
input skew ratio, driven largely by terminal stations with longer dwell behavior.
Spark AQE and bounded file-source processing kept the validated run stable, so
salted aggregation remains an optimization to apply only if Spark UI/runtime logs
show straggling micro-batches.

The current service-deficit and alert thresholds are calibrated for steady-state
and synthetic severe validation. A longer run during a real disruption would be
the strongest next validation step, especially for deciding whether the `300s`
saturation point should stay fixed or move upward.

The storage choices matched their access patterns. Cassandra worked well for
keyed station/hour/weather baseline reads, while MongoDB fit the latest-status
document model with TTL expiry. Keeping the dashboard behind FastAPI simplified
Streamlit reruns and kept database connection logic centralized.
