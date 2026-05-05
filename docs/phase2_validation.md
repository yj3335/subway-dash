# Phase 2 Validation

Run after Arjun's staging outputs and the batch baseline are available.

## Speed-Layer Join Checks

| Check | Result |
|---|---|
| Rows in `data/staging/speed_layer_delays/` | TBD |
| `station_complex_id` null rate | TBD |
| Typical `avg_arrival_delay_secs` range | TBD |

## Lambda Merge Checks

| Check | Result |
|---|---|
| MongoDB `speed_layer` documents inserted | TBD |
| `congestion_score` in `[0, 1]` | TBD |
| `predicted_delay_mins` plausible | TBD |
| Highest-scoring station sanity check | TBD |

## Threshold Back-Test

Use:

```bash
python -m processing.calibrate_thresholds --events-csv data/validation/historical_events.csv
```

The CSV should include `event_name`, `station_complex_id`, `avg_arrival_delay_secs`, `avg_entries`, `max_hourly_entries`, and optional `expected_level`.

