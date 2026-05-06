# Phase 2 Validation

Run after Arjun's staging outputs and the batch baseline are available.

## Speed-Layer Join Checks

| Check | Result |
|---|---|
| Rows in `data/staging/vehicle_positions/` | `2,780` |
| Rows in `data/staging/trip_delays/` | `2,393` |
| Rows in finite smoke-test speed-layer output | `377` |
| `station_complex_id` null rate | `0%` in speed-layer output |
| Typical `avg_arrival_delay_secs` range | includes early/on-time/late values, sample range `-46.0` to `99.0` seconds |

Status: PASS for a finite local smoke test using the staged parquet inputs from Track A. The production command should use `data/staging/speed_layer_delays/` as output with a persistent checkpoint.

## Lambda Merge Checks

| Check | Result |
|---|---|
| Debug parquet rows in `data/debug/lambda_merge/` | `377` |
| `congestion_score` in `[0, 1]` | PASS |
| `predicted_delay_mins` plausible | PASS; highest sample `2.72` minutes |
| Highest-scoring station sanity check | station `134` / Sutter Av, `congestion_score = 0.084` |

Status: PASS for local parquet sink validation. The local run defaulted `weather_bucket` to `clear` because serving dependencies were not installed in the active venv; this is expected fail-closed behavior for local testing. MongoDB sink validation still requires the serving dependencies and MongoDB runtime.

## Threshold Back-Test

Use:

```bash
python -m processing.calibrate_thresholds --events-csv data/validation/historical_events.csv
```

The CSV should include `event_name`, `station_complex_id`, `avg_arrival_delay_secs`, `avg_entries`, `max_hourly_entries`, and optional `expected_level`.
