from __future__ import annotations

import argparse
import csv
import json
import os
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from datetime import date
from pathlib import Path

from processing.config import (
    MTA_HOURLY_RIDERSHIP_2020_2024_CSV_URL,
    MTA_HOURLY_RIDERSHIP_2020_2024_RESOURCE_ID,
    MTA_HOURLY_RIDERSHIP_2025_CSV_URL,
    MTA_HOURLY_RIDERSHIP_2025_RESOURCE_ID,
    MTA_STATIC_ZIP_URL,
    MTA_STATIONS_CSV_URL,
    NOAA_CDO_DATA_URL,
    RAW_RIDERSHIP_2020_2024,
    RAW_RIDERSHIP_2025,
    RAW_STATIONS,
    RAW_STOPS,
    RAW_WEATHER,
    ensure_dir,
    ensure_parent,
)


DEFAULT_NOAA_STATIONS = ["GHCND:USW00094728", "GHCND:USW00094789"]
NOAA_DATATYPES = ["PRCP", "SNOW", "TMAX", "TMIN"]
RIDERSHIP_SELECT_COLUMNS = [
    "transit_timestamp",
    "station_complex_id",
    "station_complex",
    "borough",
    "payment_method",
    "ridership",
    "transfers",
    "latitude",
    "longitude",
]


def download_url(url: str, destination: Path, *, skip_existing: bool = True) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return
    ensure_parent(destination)
    print(f"downloading: {url} -> {destination}")
    part_path = destination.with_suffix(destination.suffix + ".part")
    bytes_written = 0
    try:
        with urllib.request.urlopen(url, timeout=120) as response, part_path.open("wb") as handle:
            while True:
                chunk = response.read(4 * 1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                bytes_written += len(chunk)
                print(f"  {bytes_written / (1024 * 1024):,.1f} MiB", end="\r", flush=True)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        if part_path.exists():
            part_path.unlink()
        raise RuntimeError(f"HTTP {exc.code} while downloading {url}\n{body[:2000]}") from exc
    part_path.replace(destination)
    print()
    print(f"wrote {destination.stat().st_size:,} bytes: {destination}")


def with_socrata_limit(url: str, limit: int | None) -> str:
    if not limit:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}$limit={int(limit)}"


def ridership_resource_url(resource_id: str, *, since: str | None = None, limit: int | None = None) -> str:
    params: dict[str, object] = {
        "$select": ",".join(RIDERSHIP_SELECT_COLUMNS),
        "$order": "transit_timestamp",
    }
    if since:
        params["$where"] = f"transit_timestamp >= '{since}T00:00:00'"
    if limit:
        params["$limit"] = int(limit)
    return f"https://data.ny.gov/resource/{resource_id}.csv?{urllib.parse.urlencode(params)}"


def download_gtfs_static(destination: Path, *, skip_existing: bool = True) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return
    zip_path = destination.with_suffix(".zip")
    download_url(MTA_STATIC_ZIP_URL, zip_path, skip_existing=False)
    with zipfile.ZipFile(zip_path) as archive:
        with archive.open("stops.txt") as source:
            ensure_parent(destination)
            destination.write_bytes(source.read())
    print(f"extracted stops.txt -> {destination}")


def _noaa_get(token: str, params: dict[str, object]) -> dict:
    query = urllib.parse.urlencode(params, doseq=True)
    request = urllib.request.Request(
        f"{NOAA_CDO_DATA_URL}?{query}",
        headers={"token": token, "User-Agent": "subway-dash/1.0"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def download_noaa_weather(
    destination: Path,
    *,
    token: str,
    start_date: str,
    end_date: str,
    stations: list[str],
    skip_existing: bool = True,
) -> None:
    if skip_existing and destination.exists() and destination.stat().st_size > 0:
        print(f"exists: {destination}")
        return

    # CDO paginates responses. Store per date, then aggregate Central Park/JFK
    # into one NYC-wide daily row because the runtime model uses a city bucket.
    by_date: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for station in stations:
        offset = 1
        while True:
            payload = _noaa_get(
                token,
                {
                    "datasetid": "GHCND",
                    "stationid": station,
                    "datatypeid": NOAA_DATATYPES,
                    "startdate": start_date,
                    "enddate": end_date,
                    "units": "standard",
                    "limit": 1000,
                    "offset": offset,
                },
            )
            results = payload.get("results", [])
            for row in results:
                day = row["date"][:10]
                by_date[day][row["datatype"]].append(float(row["value"]))
            if len(results) < 1000:
                break
            offset += 1000
            time.sleep(0.25)

    ensure_parent(destination)
    with destination.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["DATE", "PRCP", "SNOW", "TMAX", "TMIN"])
        writer.writeheader()
        for day in sorted(by_date):
            values = by_date[day]
            writer.writerow(
                {
                    "DATE": day,
                    "PRCP": max(values.get("PRCP", [0.0])),
                    "SNOW": max(values.get("SNOW", [0.0])),
                    "TMAX": _mean_or_blank(values.get("TMAX", [])),
                    "TMIN": _mean_or_blank(values.get("TMIN", [])),
                }
            )
    print(f"wrote NOAA daily weather rows: {destination}")


def _mean_or_blank(values: list[float]) -> float | str:
    return sum(values) / len(values) if values else ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Subway Dash source datasets into data/raw.")
    parser.add_argument("--raw-dir", default=str(RAW_STOPS.parent))
    parser.add_argument("--start-date", default=f"{date.today().year - 2}-01-01")
    parser.add_argument("--end-date", default=str(date.today()))
    parser.add_argument("--noaa-station", action="append", dest="noaa_stations", default=[])
    parser.add_argument(
        "--ridership-limit",
        type=int,
        default=None,
        help="Optional Socrata row limit for quick smoke-test downloads. Omit for the full CSV.",
    )
    parser.add_argument(
        "--ridership-years",
        choices=["all", "2020-2024", "2025"],
        default="all",
        help="Which MTA hourly ridership dataset to download.",
    )
    parser.add_argument(
        "--ridership-since",
        default=None,
        help="Optional YYYY-MM-DD filter. Uses faster Socrata resource endpoint with projected columns.",
    )
    parser.add_argument("--skip-ridership", action="store_true", help="Download only static GTFS/stations/weather inputs.")
    parser.add_argument("--skip-existing", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--skip-noaa", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_dir = ensure_dir(args.raw_dir)
    download_gtfs_static(raw_dir / RAW_STOPS.name, skip_existing=args.skip_existing)
    download_url(MTA_STATIONS_CSV_URL, raw_dir / RAW_STATIONS.name, skip_existing=args.skip_existing)
    if args.skip_ridership:
        print("skipping ridership download by request")
    else:
        if args.ridership_years in {"all", "2020-2024"}:
            url = (
                ridership_resource_url(
                    MTA_HOURLY_RIDERSHIP_2020_2024_RESOURCE_ID,
                    since=args.ridership_since,
                    limit=args.ridership_limit,
                )
                if args.ridership_since
                else with_socrata_limit(MTA_HOURLY_RIDERSHIP_2020_2024_CSV_URL, args.ridership_limit)
            )
            download_url(
                url,
                raw_dir / RAW_RIDERSHIP_2020_2024.name,
                skip_existing=args.skip_existing,
            )
        if args.ridership_years in {"all", "2025"}:
            url = (
                ridership_resource_url(
                    MTA_HOURLY_RIDERSHIP_2025_RESOURCE_ID,
                    since=args.ridership_since,
                    limit=args.ridership_limit,
                )
                if args.ridership_since
                else with_socrata_limit(MTA_HOURLY_RIDERSHIP_2025_CSV_URL, args.ridership_limit)
            )
            download_url(
                url,
                raw_dir / RAW_RIDERSHIP_2025.name,
                skip_existing=args.skip_existing,
            )

    if args.skip_noaa:
        print("skipping NOAA download by request")
        return

    token = os.environ.get("NOAA_TOKEN")
    if not token:
        print("NOAA_TOKEN is not set; leave data/raw/noaa_weather.csv in place or rerun with a token.")
        return

    download_noaa_weather(
        raw_dir / RAW_WEATHER.name,
        token=token,
        start_date=args.start_date,
        end_date=args.end_date,
        stations=args.noaa_stations or DEFAULT_NOAA_STATIONS,
        skip_existing=args.skip_existing,
    )


if __name__ == "__main__":
    main()
