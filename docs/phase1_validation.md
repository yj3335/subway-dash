# Phase 1 Validation

Last updated: 2026-05-05

## Bridge Table

- Total normalized GTFS stops: `496`
- Tier 1 direct matches: `496`
- Tier 2 name matches: `0`
- Tier 3 geospatial matches: `0`
- Unresolved stops: `0`
- Unresolved rate: `0.00%`

Status: PASS. Assumption A-01 is confirmed for the current MTA static + stations datasets because every normalized GTFS stop resolved through Tier 1.

Output:

```text
data/bridge/station_bridge.parquet/
```

## Ridership Cleaning

Input:

```text
data/raw/MTA_Hourly_Ridership_Beginning_2025.csv
```

Raw CSV validation:

- File size: `5.7 GB`
- Raw rows: `37,097,502`
- Date range: `2025-01-01 00:00:00` to `2026-04-22 23:00:00`
- Bad timestamps: `0`
- Required columns present: `transit_timestamp`, `station_complex_id`, `station_complex`, `borough`, `ridership`, `latitude`, `longitude`

Cleaning output:

- Cleaned rows: `4,749,454`
- Bridge validation rows: `4,749,454 true`
- Bridge validation failures: `0`

Status: PASS. The row reduction is expected because the raw feed has multiple payment/fare rows per station-hour; the cleaning job aggregates those into one station-hour `entries` row.

Output:

```text
data/ridership/clean/
```

## Weather Cleaning

Input:

```text
data/raw/noaa_weather.csv
```

Raw file validation:

- Raw rows including header: `478`
- Date range requested: `2025-01-01` to `2026-04-22`

Output:

```text
data/weather/clean/
```

Status: PASS if Spark validation shows non-null `weather_bucket` values for the cleaned weather rows.

Spark validation:

- Clean weather rows: `477`
- Date range: `2025-01-01` to `2026-04-22`
- `clear`: `371`
- `rain`: `83`
- `snow`: `23`

Status: PASS.

## Ridership + Weather Join

Output exists:

```text
data/ridership_weather_baseline/
```

Filesystem size:

- `data/ridership_weather_baseline/`: `115 MB`

Status: PASS if Spark validation shows:

- Joined row count matches cleaned ridership row count: expected `4,749,454`
- `weather_bucket` is never null
- Joined date range covers the cleaned ridership date range

Spark validation:

- Joined rows: `4,749,454`
- Date range: `2025-01-01` to `2026-04-23`
- `clear`: `3,694,715`
- `rain`: `826,509`
- `snow`: `228,230`

Status: PASS. The joined date range includes `2026-04-23`, one day beyond the NOAA file's `2026-04-22` max date. This is expected for the available ridership extract; missing weather rows default to `clear` by design.

## Local Batch Baseline

Command:

```bash
python -m processing.build_baseline \
  --ridership-weather-input data/ridership_weather_baseline \
  --lookback-days 0 \
  --sink parquet
```

Outputs:

```text
data/batch/station_capacity_baseline/
data/batch/station_max_entries/
```

Spark validation:

- `station_capacity_baseline` rows: `213,268`
- `station_max_entries` rows: `424`
- Baseline weather bucket rows:
  - `clear`: `71,232`
  - `rain`: `71,228`
  - `snow`: `70,808`
- Top station peaks:
  - station `610`: `23,692`
  - station `611`: `18,852`
  - station `448`: `17,193`
  - station `628`: `15,344`
  - station `604`: `14,621`
- Sample cell `station_complex_id = 611`, Spark `day_of_week = 4`, `hour_of_day = 8`, `weather_bucket = clear`:
  - `avg_entries`: `328.3269230769231`
  - `std_entries`: `213.7142788547956`
  - `p95_entries`: `415.0`

Schemas:

```text
station_capacity_baseline(
  station_complex_id string,
  day_of_week int,
  hour_of_day int,
  weather_bucket string,
  avg_entries double,
  std_entries double,
  p95_entries double
)

station_max_entries(
  station_complex_id string,
  max_hourly_entries double
)
```

Status: PASS. The parquet baseline is ready for local speed-layer testing.

Cassandra write validation:

- Target table `subway_dash.station_capacity_baseline`: `213,268` rows
- Target table `subway_dash.station_max_entries`: `424` rows
- Cassandra baseline weather bucket rows:
  - `clear`: `71,232`
  - `rain`: `71,228`
  - `snow`: `70,808`

Status: PASS. Cassandra baseline tables were written and read back successfully.

Run:

```bash
python - <<'PY'
from processing.spark_utils import get_spark

s = get_spark("phase1-validation")

for label, path in {
    "ridership_clean": "data/ridership/clean",
    "weather_clean": "data/weather/clean",
    "ridership_weather": "data/ridership_weather_baseline",
}.items():
    df = s.read.parquet(path)
    print(f"\n== {label} ==")
    print("rows:", df.count())
    if "weather_bucket" in df.columns:
        df.groupBy("weather_bucket").count().show()
    if "date" in df.columns:
        df.selectExpr("min(date) as min_date", "max(date) as max_date").show()
        if "weather_bucket" in df.columns:
            print("null weather_bucket:", df.filter("weather_bucket IS NULL").count())
PY
```

## Phase 1 Outputs

Ready to hand off:

- `data/bridge/station_bridge.parquet/` to Arjun and Yash
- `data/ridership/clean/` for any teammate reruns of batch baseline work
- `data/weather/clean/` for any teammate reruns of weather bucket baseline work
- `data/ridership_weather_baseline/` for Phase 2 baseline and analytics work
- `data/batch/station_capacity_baseline/` for local speed-layer baseline lookup testing
- `data/batch/station_max_entries/` for local demand intensity normalization testing
- `docs/join_key_map.md`
- `docs/phase1_validation.md`

Cassandra target tables:

- `subway_dash.station_capacity_baseline`
- `subway_dash.station_max_entries`
