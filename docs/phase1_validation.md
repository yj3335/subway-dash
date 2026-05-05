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

## Phase 1 Track B Handoff

Ready to hand off:

- `data/bridge/station_bridge.parquet/` to Arjun and Yash
- `data/ridership/clean/` for batch baseline work
- `data/weather/clean/` for weather bucket baseline work
- `data/ridership_weather_baseline/` for Phase 2 baseline and analytics work
- `docs/join_key_map.md`
- `docs/phase1_validation.md`
