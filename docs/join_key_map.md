# Join Key Map

## Confirmed Columns

| Dataset | Purpose | Column |
|---|---|---|
| MTA Stations | GTFS stop key | `GTFS Stop ID` |
| MTA Stations | Station complex key | `Complex ID` |
| MTA Stations | Route labels | `Daytime Routes` |
| MTA Hourly Ridership | Station complex key | `station_complex_id` |
| MTA Hourly Ridership | Timestamp | `transit_timestamp` |
| MTA Hourly Ridership | Entry/ridership count | `ridership` |
| NOAA Daily Weather | Daily join key | `DATE` |

## Conventions

- GTFS-Realtime stop IDs such as `127N` and `127S` are normalized to base stop ID `127`.
- Spark `dayofweek()` is used in batch outputs: Sunday = 1, Wednesday = 4, Saturday = 7.
- NOAA historical weather is daily. Ridership joins to weather on `date` only, and the same bucket is applied to all 24 hours.
- Local defaults are under `data/`; every CLI accepts path overrides for Docker or S3 later.
