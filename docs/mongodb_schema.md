# MongoDB Schema — `speed_layer` Collection

Database: `subway_dash`  
Collection: `speed_layer`  
TTL index: `inserted_at` — documents expire after 24 hours

## Document Structure

```json
{
  "station_complex_id":    "613",
  "complex_name":          "Times Sq-42 St",
  "event_timestamp":       "2024-04-21T08:30:00Z",
  "avg_arrival_delay_secs": 185.0,
  "service_deficit":       0.52,
  "demand_intensity":      0.68,
  "congestion_score":      0.35,
  "predicted_delay_mins":  5.2,
  "alert_level":           "MODERATE",
  "weather_bucket":        "clear",
  "inserted_at":           "2024-04-21T08:30:05Z"
}
```

## Field Reference

| Field | Type | Description |
|---|---|---|
| `station_complex_id` | string | MTA station complex ID (e.g. `"613"`) |
| `complex_name` | string | Human-readable station name |
| `event_timestamp` | ISO 8601 UTC | End of the 10-minute window this score covers |
| `avg_arrival_delay_secs` | float | Rolling 10-min mean of `arrival.delay` from GTFS TripUpdates |
| `service_deficit` | float [0, 1] | `clamp((avg_arrival_delay_secs - 60) / 240, 0, 1)` |
| `demand_intensity` | float [0, 1] | `avg_entries / max_hourly_entries` from batch baseline |
| `congestion_score` | float [0, 1] | `service_deficit × demand_intensity` |
| `predicted_delay_mins` | float | `(avg_arrival_delay_secs / 60) × (1 + demand_intensity)` |
| `alert_level` | string | `"NORMAL"` / `"MODERATE"` (≥ 0.20) / `"SEVERE"` (≥ 0.50) |
| `weather_bucket` | string | `"clear"` / `"rain"` / `"snow"` at time of computation |
| `inserted_at` | ISO 8601 UTC | Write timestamp — used by the 24-hour TTL index |

## Notes

- One document per `(station_complex_id, event_timestamp)`. The speed-layer micro-batch writes at 30-second intervals, so multiple documents per station accumulate throughout the day.
- The FastAPI `/api/v1/station/{id}/congestion` endpoint fetches the single latest document per station (sort by `inserted_at` descending, limit 1).
- `inserted_at` must always be set at write time — missing it silently disables TTL expiry for that document.
