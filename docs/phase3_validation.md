# Phase 3 Validation

Status: complete for the larger local staging run.

## Larger Staging Speed-Layer Run

| Check | Result |
|---|---:|
| Speed-layer rows in `data/staging/speed_layer_delays/` | `2,538` |
| Null `station_complex_id` rows | `0` |
| Arrival delay p50 seconds | `58.00` |
| Arrival delay p95 seconds | `141.53` |
| Arrival delay p99 seconds | `281.00` |
| Arrival delay max seconds | `347.00` |
| Highest station-window output count | `137` |

Status: PASS. The larger staging run produced clean station-window delay output with no null station IDs.

## Lambda Merge MongoDB Output

| Check | Result |
|---|---:|
| MongoDB `subway_dash.speed_layer` documents | `2,916` |
| Latest `inserted_at` | `2026-05-06T04:25:32.108Z` |
| `NORMAL` documents | `2,875` |
| `MODERATE` documents | `40` |
| `SEVERE` documents | `1` |

Status: PASS. Lambda merge wrote scored congestion documents to MongoDB and produced all three alert classes during the larger run.

## Skew Audit

Station-level input skew was measured at `43x`, which is above the `5x` tuning threshold. Spark AQE skew handling was enabled for the larger run:

```bash
export SPARK_CONF_spark__sql__adaptive__enabled=true
export SPARK_CONF_spark__sql__adaptive__skewJoin__enabled=true
```

The raw skew ratio is expected to remain `43x` because AQE changes Spark execution strategy, not the input data distribution. The aggregated speed-layer output was balanced enough for this run: the highest station-window output count was `137`.

Decision: defer salted aggregation unless Spark UI or runtime logs show micro-batch stragglers as a measured bottleneck. Assumption A-12 is revised from "salting may be needed based on input skew alone" to "salting is needed only if input skew produces unacceptable runtime stragglers after AQE."

## Service Deficit Calibration

Use:

```bash
python -m processing.calibrate_service_deficit
```

The script computes observed p50/p95 arrival delay seconds and recommends whether to keep or revise the default 60-second grace and 300-second saturation endpoints.

For this larger steady-state run, the observed speed-layer delay distribution was p50=`58.00s`, p95=`141.53s`, p99=`281.00s`, and max=`347.00s`.

Decision: keep the current `SERVICE_GRACE_SECS = 60.0` and `SERVICE_SATURATION_SECS = 300.0` for now. A lower saturation value such as `240s` remains a candidate, but should not be locked in until a disruption-heavy run or synthetic severe validation confirms that the current thresholds under-alert.

## Weather Coefficients

Use:

```bash
python -m processing.compute_weather_coefficients --sink parquet,cassandra
```

This populates rain/snow multipliers for the analytics report. It is separate from the real-time `current_weather` lookup used by `lambda_merge.py`.
