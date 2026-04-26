CREATE KEYSPACE IF NOT EXISTS subway_dash
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Batch baseline: avg/std/p95 entries per (station, day, hour, weather) cell.
-- Rebuilt nightly; no TTL.
CREATE TABLE IF NOT EXISTS subway_dash.station_capacity_baseline (
    station_complex_id  TEXT,
    day_of_week         INT,
    hour_of_day         INT,
    weather_bucket      TEXT,
    avg_entries         DOUBLE,
    std_entries         DOUBLE,
    p95_entries         DOUBLE,
    PRIMARY KEY ((station_complex_id, weather_bucket), day_of_week, hour_of_day)
);

-- Per-station historical peak entry count. Used to normalize demand_intensity
-- in the speed layer: demand_intensity = avg_entries / max_hourly_entries.
CREATE TABLE IF NOT EXISTS subway_dash.station_max_entries (
    station_complex_id  TEXT PRIMARY KEY,
    max_hourly_entries  DOUBLE
);

-- Single-row table. The NWS poller upserts this every 15 minutes.
-- Speed layer reads it via get_current_weather_bucket() with a 15-min cache.
CREATE TABLE IF NOT EXISTS subway_dash.current_weather (
    scope           TEXT PRIMARY KEY,
    weather_bucket  TEXT,
    prcp_in         DOUBLE,
    snow_in         DOUBLE,
    tmax_f          DOUBLE,
    updated_at      TIMESTAMP
);

-- Per-station rain/snow ridership multipliers computed in Week 9.
-- Separate from current_weather — this is a historical analytics artifact.
CREATE TABLE IF NOT EXISTS subway_dash.weather_coefficients (
    station_complex_id  TEXT,
    weather_bucket      TEXT,
    multiplier          DOUBLE,
    PRIMARY KEY (station_complex_id, weather_bucket)
);
