from __future__ import annotations

import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = Path(os.environ.get("SUBWAY_DASH_DATA_DIR", REPO_ROOT / "data"))

RAW_DIR = DEFAULT_DATA_DIR / "raw"
BRIDGE_DIR = DEFAULT_DATA_DIR / "bridge"
RIDERSHIP_CLEAN_DIR = DEFAULT_DATA_DIR / "ridership" / "clean"
WEATHER_CLEAN_DIR = DEFAULT_DATA_DIR / "weather" / "clean"
RIDERSHIP_WEATHER_DIR = DEFAULT_DATA_DIR / "ridership_weather_baseline"
STAGING_DIR = DEFAULT_DATA_DIR / "staging"
CHECKPOINT_DIR = Path(os.environ.get("SUBWAY_DASH_CHECKPOINT_DIR", REPO_ROOT / "checkpoints"))

RAW_STOPS = RAW_DIR / "stops.txt"
RAW_STATIONS = RAW_DIR / "MTA_Stations.csv"
RAW_RIDERSHIP = RAW_DIR / "MTA_Hourly_Ridership.csv"
RAW_RIDERSHIP_2020_2024 = RAW_DIR / "MTA_Hourly_Ridership_2020_2024.csv"
RAW_RIDERSHIP_2025 = RAW_DIR / "MTA_Hourly_Ridership_Beginning_2025.csv"
RAW_WEATHER = RAW_DIR / "noaa_weather.csv"
BRIDGE_PARQUET = BRIDGE_DIR / "station_bridge.parquet"

MTA_STATIC_ZIP_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"
MTA_STATIONS_CSV_URL = "https://data.ny.gov/api/views/39hk-dx4f/rows.csv?accessType=DOWNLOAD"
MTA_HOURLY_RIDERSHIP_2020_2024_CSV_URL = "https://data.ny.gov/api/views/wujg-7c2s/rows.csv?accessType=DOWNLOAD"
MTA_HOURLY_RIDERSHIP_2025_CSV_URL = "https://data.ny.gov/api/views/5wq4-mkjj/rows.csv?accessType=DOWNLOAD"
MTA_HOURLY_RIDERSHIP_2020_2024_RESOURCE_ID = "wujg-7c2s"
MTA_HOURLY_RIDERSHIP_2025_RESOURCE_ID = "5wq4-mkjj"
NOAA_CDO_DATA_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/data"


def ensure_parent(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def ensure_dir(path: str | Path) -> Path:
    resolved = Path(path)
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def path_str(path: str | Path) -> str:
    return str(Path(path).expanduser())


def default_path(path: Path) -> str:
    return str(path)
