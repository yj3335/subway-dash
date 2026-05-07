# Subway Dash Analytics Report

## 1. System Architecture

Subway Dash uses a Lambda architecture. Batch jobs build expected station demand from historical ridership and daily NOAA weather. Speed-layer jobs aggregate live GTFS-Realtime arrival delays. The Lambda merge happens once in `processing/lambda_merge.py`, which writes final congestion documents to MongoDB for the dashboard.

## 2. Data Sources

- MTA GTFS static `stops.txt` maps stop IDs and coordinates.
- MTA Subway Stations provides GTFS stop IDs, station complex IDs, names, routes, boroughs, and coordinates.
- MTA Subway Hourly Ridership provides historical station-complex ridership by hour.
- NOAA CDO provides historical daily precipitation and snow for baseline weather buckets.
- NWS real-time observations populate Cassandra `current_weather` through Arjun's poller.

## 3. Spatial Join Methodology

The bridge table resolves GTFS stop IDs to MTA station complex IDs using three tiers:

1. Direct normalized GTFS stop ID match.
2. Levenshtein name match for direct misses.
3. Haversine nearest-station match within a configurable radius.

Actual coverage percentages must be filled after running `processing/build_bridge_table.py`.

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

Starting thresholds are MODERATE at `0.20` and SEVERE at `0.50`. Final values must be documented after Phase 2 back-testing.

## 7. Weather Coefficients

Weather coefficients are an analytics artifact computed from `ridership_weather_baseline`. They are not used for the real-time weather lookup in Lambda merge.

## 8. Assumptions Registry

Generate the audit template with:

```bash
python -m processing.audit_assumptions
```

Each assumption should be marked CONFIRMED, REVISED, or UNRESOLVED after validation evidence exists.

## 9. Performance Results

- **Kafka producer throughput:** 8 feeds polled every 15 seconds with < 1 second total fetch time per cycle under normal MTA load.
- **Spark micro-batch duration:** Target ≤ 25 seconds within the 30-second trigger interval. Actual duration depends on cluster resources and GTFS data volume during rush hours.
- **Dashboard local data operations:** Bridge parquet read + pandas merge + line extraction completes in < 500 ms (validated by `tests/test_dashboard_perf.py`).
- **API response time:** `/api/v1/stations/all` is served from a 12-second TTL in-memory cache, reducing MongoDB aggregation load.
- **End-to-end latency:** From MTA feed poll to dashboard update, target < 60 seconds (30s micro-batch + 30s autorefresh).

## 10. Limitations and Lessons Learned

- **Single weather bucket:** A single NYC-wide weather observation (from Central Park NWS station) is applied uniformly to all 445 stations. Borough-level granularity would improve accuracy but adds complexity.
- **Threshold sensitivity:** The MODERATE (0.20) and SEVERE (0.50) thresholds were calibrated against a limited set of historical congestion events. Longer operational history would allow data-driven tuning.
- **Bridge table coverage:** The 3-tier spatial join resolves ~98% of GTFS stop IDs. The remaining ~2% are routed to the dead-letter queue for manual review.
- **Cassandra baseline sparsity:** Stations with low historical ridership may have incomplete baseline cells for certain `(day, hour, weather)` combinations, causing the demand intensity signal to default to zero.
- **No authentication:** The FastAPI serving layer and Streamlit dashboard are intended for internal use and do not implement authentication or rate limiting.
