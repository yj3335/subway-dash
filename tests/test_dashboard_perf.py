"""Dashboard data-loading performance test.

Measures the local data operations that run on every Streamlit rerun:
  1. Bridge parquet read + dedup        budget: 300 ms
  2. Pandas merge (bridge + statuses)   budget: 100 ms
  3. Line extraction                    budget:  50 ms

These are the parts of the 2-second end-to-end budget that are
testable without running servers. Network latency (FastAPI call) and
PyDeck rendering (browser-side) are excluded.
"""
import time
import unittest

import pandas as pd

from processing.config import BRIDGE_PARQUET


def _load_stations() -> pd.DataFrame:
    df = pd.read_parquet(
        BRIDGE_PARQUET,
        columns=["station_complex_id", "complex_name", "lat", "lon", "daytime_routes"],
    )
    df = df.drop_duplicates("station_complex_id")
    return df.rename(columns={"complex_name": "name"})


def _get_all_lines(df: pd.DataFrame) -> list:
    lines: set = set()
    for val in df["daytime_routes"].dropna():
        lines.update(str(val).split())
    return sorted(lines)


def _build_map_data(stations_df: pd.DataFrame, statuses_df: pd.DataFrame) -> pd.DataFrame:
    df = stations_df.merge(
        statuses_df[["station_complex_id", "alert_level", "congestion_score", "avg_arrival_delay_secs"]],
        on="station_complex_id",
        how="left",
    )
    df["alert_level"] = df["alert_level"].fillna("N/A")
    return df


class DashboardPerfTests(unittest.TestCase):
    def test_station_load_under_300ms(self):
        t0 = time.perf_counter()
        stations = _load_stations()
        elapsed = time.perf_counter() - t0
        self.assertLessEqual(len(stations), 445)
        self.assertGreater(len(stations), 0)
        self.assertLess(elapsed, 0.3, f"Bridge parquet load took {elapsed:.3f}s (budget 300ms)")

    def test_map_merge_under_100ms(self):
        stations = _load_stations()
        mock_statuses = pd.DataFrame({
            "station_complex_id": stations["station_complex_id"].tolist(),
            "alert_level": ["NORMAL"] * len(stations),
            "congestion_score": [0.1] * len(stations),
            "avg_arrival_delay_secs": [30.0] * len(stations),
        })
        t0 = time.perf_counter()
        result = _build_map_data(stations, mock_statuses)
        elapsed = time.perf_counter() - t0
        self.assertEqual(len(result), len(stations))
        self.assertLess(elapsed, 0.1, f"Map merge took {elapsed:.3f}s (budget 100ms)")

    def test_line_extraction_under_50ms(self):
        stations = _load_stations()
        t0 = time.perf_counter()
        lines = _get_all_lines(stations)
        elapsed = time.perf_counter() - t0
        self.assertGreater(len(lines), 0)
        self.assertLess(elapsed, 0.05, f"Line extraction took {elapsed:.3f}s (budget 50ms)")

    def test_total_local_ops_under_500ms(self):
        t0 = time.perf_counter()
        stations = _load_stations()
        mock_statuses = pd.DataFrame({
            "station_complex_id": stations["station_complex_id"].tolist(),
            "alert_level": ["NORMAL"] * len(stations),
            "congestion_score": [0.1] * len(stations),
            "avg_arrival_delay_secs": [30.0] * len(stations),
        })
        _build_map_data(stations, mock_statuses)
        _get_all_lines(stations)
        elapsed = time.perf_counter() - t0
        self.assertLess(elapsed, 0.5, f"Total local data ops took {elapsed:.3f}s (budget 500ms)")


if __name__ == "__main__":
    unittest.main()
