from __future__ import annotations

import math
import re
from datetime import datetime, timezone
from typing import Iterable, Sequence


MODERATE_THRESHOLD = 0.20
SEVERE_THRESHOLD = 0.50
SERVICE_GRACE_SECS = 60.0
SERVICE_SATURATION_SECS = 300.0
RAIN_THRESHOLD_IN = 0.1
SNOW_THRESHOLD_IN = 0.1


def normalize_stop_id(value: object) -> str:
    """Normalize GTFS stop IDs by stripping whitespace and N/S direction suffixes."""
    if value is None:
        return ""
    text = str(value).strip()
    return re.sub(r"[NS]$", "", text)


def normalize_station_name(value: object) -> str:
    if value is None:
        return ""
    text = str(value).lower().strip()
    text = re.sub(r"&", " and ", text)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_m = 6_371_000.0
    phi1 = math.radians(float(lat1))
    phi2 = math.radians(float(lat2))
    dphi = math.radians(float(lat2) - float(lat1))
    dlambda = math.radians(float(lon2) - float(lon1))
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return radius_m * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_weather(prcp_in: object, snow_in: object) -> str:
    prcp = float(prcp_in or 0.0)
    snow = float(snow_in or 0.0)
    if snow > SNOW_THRESHOLD_IN:
        return "snow"
    if prcp > RAIN_THRESHOLD_IN:
        return "rain"
    return "clear"


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def compute_service_deficit(
    avg_arrival_delay_secs: object,
    grace_secs: float = SERVICE_GRACE_SECS,
    saturation_secs: float = SERVICE_SATURATION_SECS,
) -> float:
    delay = float(avg_arrival_delay_secs or 0.0)
    if saturation_secs <= grace_secs:
        raise ValueError("saturation_secs must be greater than grace_secs")
    return clamp((delay - grace_secs) / (saturation_secs - grace_secs))


def compute_demand_intensity(avg_entries: object, max_hourly_entries: object) -> float:
    max_entries = float(max_hourly_entries or 0.0)
    if max_entries <= 0:
        return 0.0
    return clamp(float(avg_entries or 0.0) / max_entries)


def compute_congestion_score(service_deficit: object, demand_intensity: object) -> float:
    return clamp(float(service_deficit or 0.0)) * clamp(float(demand_intensity or 0.0))


def compute_predicted_delay_mins(avg_arrival_delay_secs: object, demand_intensity: object) -> float:
    return (float(avg_arrival_delay_secs or 0.0) / 60.0) * (1.0 + clamp(float(demand_intensity or 0.0)))


def classify_alert_level(
    congestion_score: object,
    moderate_threshold: float = MODERATE_THRESHOLD,
    severe_threshold: float = SEVERE_THRESHOLD,
) -> str:
    score = float(congestion_score or 0.0)
    if score >= severe_threshold:
        return "SEVERE"
    if score >= moderate_threshold:
        return "MODERATE"
    return "NORMAL"


def find_column(columns: Sequence[str], candidates: Iterable[str], *, required: bool = True) -> str | None:
    by_normalized = {normalize_column_name(column): column for column in columns}
    for candidate in candidates:
        found = by_normalized.get(normalize_column_name(candidate))
        if found:
            return found
    if required:
        raise ValueError(f"Could not find any of {list(candidates)} in columns {list(columns)}")
    return None


def normalize_column_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def parse_iso_timestamp(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

