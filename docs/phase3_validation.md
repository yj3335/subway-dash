# Phase 3 Validation

Run after several days or weeks of staged `trip_delays` are available.

## Service Deficit Calibration

Use:

```bash
python -m processing.calibrate_service_deficit
```

The script computes observed p50/p95 arrival delay seconds and recommends whether to keep or revise the default 60-second grace and 300-second saturation endpoints.

## Weather Coefficients

Use:

```bash
python -m processing.compute_weather_coefficients --sink parquet,cassandra
```

This populates rain/snow multipliers for the analytics report. It is separate from the real-time `current_weather` lookup used by `lambda_merge.py`.

