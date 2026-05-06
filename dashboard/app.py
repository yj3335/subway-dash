import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Streamlit adds the script directory (dashboard/) to sys.path, not the project
# root. Insert the root explicitly so processing.* and serving.* are importable.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import pandas as pd
import pydeck
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET
from serving.db_clients import get_cassandra_session

API_URL = os.environ.get("SUBWAY_DASH_API_URL", "http://localhost:8000")

_ALERT_COLORS = {
    "NORMAL":   [0, 200, 0, 180],
    "MODERATE": [255, 200, 0, 210],
    "SEVERE":   [220, 0, 0, 220],
}
_NO_DATA_COLOR = [128, 128, 128, 120]

st.set_page_config(page_title="Subway Dash", layout="wide")
st_autorefresh(interval=30_000, key="autorefresh")


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------

@st.cache_data
def load_stations() -> pd.DataFrame:
    df = pd.read_parquet(
        BRIDGE_PARQUET,
        columns=["station_complex_id", "complex_name", "lat", "lon", "daytime_routes"],
    )
    df = df.drop_duplicates("station_complex_id")
    return df.rename(columns={"complex_name": "name"})


@st.cache_data
def get_all_lines(stations_df: pd.DataFrame) -> list[str]:
    lines: set[str] = set()
    for val in stations_df["daytime_routes"].dropna():
        lines.update(str(val).split())
    return sorted(lines)


@st.cache_data(ttl=3600)
def load_station_baseline(station_id: str, weather_bucket: str = "clear") -> pd.DataFrame:
    """Return avg_entries grouped by hour_of_day for one station (cached 1 h)."""
    try:
        session = get_cassandra_session()
        rows = session.execute(
            "SELECT hour_of_day, avg_entries "
            "FROM subway_dash.station_capacity_baseline "
            "WHERE station_complex_id = %s AND weather_bucket = %s",
            (station_id, weather_bucket),
        )
        df = pd.DataFrame(list(rows), columns=["hour_of_day", "avg_entries"])
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return df
    return df.groupby("hour_of_day", as_index=False)["avg_entries"].mean().sort_values("hour_of_day")


# ---------------------------------------------------------------------------
# Live data (runs every 30 s refresh)
# ---------------------------------------------------------------------------

def load_station_statuses() -> pd.DataFrame:
    """Latest congestion doc per station from the API, or empty DataFrame on failure."""
    try:
        resp = requests.get(f"{API_URL}/api/v1/stations/all", timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        st.warning(f"API unavailable: {exc}")
        return pd.DataFrame()
    docs = resp.json()
    return pd.DataFrame(docs) if docs else pd.DataFrame()


# ---------------------------------------------------------------------------
# Data wrangling helpers
# ---------------------------------------------------------------------------

def build_map_data(stations_df: pd.DataFrame, statuses_df: pd.DataFrame) -> pd.DataFrame:
    if statuses_df.empty:
        df = stations_df.copy()
        df["alert_level"] = "N/A"
        df["congestion_score"] = 0.0
        df["avg_arrival_delay_secs"] = None
    else:
        df = stations_df.merge(
            statuses_df[["station_complex_id", "alert_level", "congestion_score", "avg_arrival_delay_secs"]],
            on="station_complex_id",
            how="left",
        )
        df["alert_level"] = df["alert_level"].fillna("N/A")
        df["congestion_score"] = df["congestion_score"].fillna(0.0)
    df["color"] = df["alert_level"].map(lambda lvl: _ALERT_COLORS.get(lvl, _NO_DATA_COLOR))
    # Scale radius: 100 m baseline + up to 300 m extra at score = 1.0
    df["radius"] = (100 + df["congestion_score"].fillna(0.0) * 300).clip(100, 400)
    return df


def filter_by_lines(map_df: pd.DataFrame, selected_lines: list[str]) -> pd.DataFrame:
    if not selected_lines:
        return map_df
    selected_set = set(selected_lines)
    mask = map_df["daytime_routes"].apply(
        lambda r: bool(set(str(r).split()) & selected_set) if pd.notna(r) else False
    )
    return map_df[mask]


def _freshness_str(statuses_df: pd.DataFrame) -> str:
    if statuses_df.empty or "inserted_at" not in statuses_df.columns:
        return "—"
    latest = pd.to_datetime(statuses_df["inserted_at"], utc=True).max()
    if pd.isna(latest):
        return "—"
    delta = int((datetime.now(timezone.utc) - latest).total_seconds())
    if delta < 60:
        return f"{delta}s ago"
    return f"{delta // 60}m {delta % 60}s ago"


# ---------------------------------------------------------------------------
# Fetch live data
# ---------------------------------------------------------------------------

stations = load_stations()
statuses = load_station_statuses()
map_df = build_map_data(stations, statuses)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Subway Dash")
st.sidebar.markdown("**Legend**")
st.sidebar.markdown(
    "<span style='color:#dc0000'>⬤</span> SEVERE &nbsp;"
    "<span style='color:#ffc800'>⬤</span> MODERATE &nbsp;"
    "<span style='color:#00c800'>⬤</span> NORMAL &nbsp;"
    "<span style='color:#808080'>⬤</span> No data",
    unsafe_allow_html=True,
)
st.sidebar.divider()
all_lines = get_all_lines(stations)
selected_lines = st.sidebar.multiselect("Filter by subway line", all_lines)
filtered_map_df = filter_by_lines(map_df, selected_lines)

# ---------------------------------------------------------------------------
# Title + alert banner
# ---------------------------------------------------------------------------

st.title("Subway Dash — Live Congestion")

if not statuses.empty:
    severe_n = int((statuses["alert_level"] == "SEVERE").sum())
    moderate_n = int((statuses["alert_level"] == "MODERATE").sum())
    if severe_n:
        st.error(f"🔴 **SEVERE** congestion at **{severe_n}** station(s) — see red markers on map.")
    elif moderate_n:
        st.warning(f"🟡 **Moderate** congestion at **{moderate_n}** station(s).")
    else:
        st.success("🟢 All monitored stations operating normally.")

# ---------------------------------------------------------------------------
# KPI tiles
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)
if statuses.empty:
    for col, label in zip([c1, c2, c3, c4, c5],
                          ["Stations Monitored", "🔴 Severe", "🟡 Moderate", "🟢 Normal", "Last Updated"]):
        col.metric(label, "—")
else:
    c1.metric("Stations Monitored", len(statuses))
    c2.metric("🔴 Severe",   int((statuses["alert_level"] == "SEVERE").sum()))
    c3.metric("🟡 Moderate", int((statuses["alert_level"] == "MODERATE").sum()))
    c4.metric("🟢 Normal",   int((statuses["alert_level"] == "NORMAL").sum()))
    c5.metric("Last Updated", _freshness_str(statuses))

# ---------------------------------------------------------------------------
# Map
# ---------------------------------------------------------------------------

layer = pydeck.Layer(
    "ScatterplotLayer",
    data=filtered_map_df,
    get_position="[lon, lat]",
    get_fill_color="color",
    get_radius="radius",
    radius_min_pixels=4,
    radius_max_pixels=20,
    pickable=True,
)

view = pydeck.ViewState(latitude=40.73, longitude=-73.98, zoom=11)

st.pydeck_chart(
    pydeck.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={"text": "{name}\nStatus: {alert_level}\nScore: {congestion_score}\nDelay: {avg_arrival_delay_secs}s"},
    ),
    use_container_width=True,
)

# ---------------------------------------------------------------------------
# Station congestion table (human-readable columns + progress bar for score)
# ---------------------------------------------------------------------------

st.subheader("Station Congestion Status")
if statuses.empty:
    st.info("No congestion data yet. Start the API server and run processing/lambda_merge.py.")
else:
    display = statuses[
        ["complex_name", "alert_level", "congestion_score", "avg_arrival_delay_secs", "event_timestamp", "weather_bucket"]
    ].copy()
    display.columns = ["Station", "Status", "Score", "Avg Delay (s)", "Updated", "Weather"]
    _badge = {"SEVERE": "🔴 SEVERE", "MODERATE": "🟡 MODERATE", "NORMAL": "🟢 NORMAL"}
    display["Status"] = display["Status"].map(lambda s: _badge.get(s, s))
    st.dataframe(
        display,
        column_config={
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=1, format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# W8: Next-hour forecast widget
# ---------------------------------------------------------------------------

with st.expander("Next-Hour Forecast"):
    now_utc = datetime.now(timezone.utc)
    next_hour = (now_utc.hour + 1) % 24

    st.caption(
        f"Historical baseline (clear weather) for the selected station. "
        f"Current hour: **{now_utc.hour:02d}:00** → Next hour: **{next_hour:02d}:00**."
    )

    forecast_name = st.selectbox(
        "Station", stations["name"].sort_values().tolist(), key="forecast_station"
    )
    forecast_id = stations.loc[stations["name"] == forecast_name, "station_complex_id"].values[0]

    baseline_df = load_station_baseline(forecast_id)

    if baseline_df.empty:
        st.info("No baseline data for this station. Run processing/build_baseline.py --sink cassandra.")
    else:
        cur_row  = baseline_df[baseline_df["hour_of_day"] == now_utc.hour]["avg_entries"].values
        next_row = baseline_df[baseline_df["hour_of_day"] == next_hour]["avg_entries"].values
        cur_val  = int(cur_row[0])  if len(cur_row)  else None
        next_val = int(next_row[0]) if len(next_row) else None

        fc1, fc2, fc3 = st.columns([1, 1, 2])
        fc1.metric(
            f"Now ({now_utc.hour:02d}:00)",
            f"{cur_val:,}" if cur_val is not None else "—",
        )
        fc2.metric(
            f"Next ({next_hour:02d}:00)",
            f"{next_val:,}" if next_val is not None else "—",
            delta=f"{next_val - cur_val:+,}" if cur_val is not None and next_val is not None else None,
        )
        with fc3:
            st.caption("24-hour baseline")
            st.line_chart(baseline_df.set_index("hour_of_day")["avg_entries"], height=120)

# ---------------------------------------------------------------------------
# 24-hour time-series chart (drill-down per station)
# ---------------------------------------------------------------------------

with st.expander("Station 24-Hour History"):
    station_names = stations["name"].sort_values().tolist()
    selected_name = st.selectbox("Station", station_names, key="history_station")
    selected_id = stations.loc[stations["name"] == selected_name, "station_complex_id"].values[0]

    try:
        hist_resp = requests.get(f"{API_URL}/api/v1/station/{selected_id}/history", timeout=5)
        hist_resp.raise_for_status()
        hist_docs = hist_resp.json()
    except Exception as exc:
        hist_docs = []
        st.warning(f"Could not load history: {exc}")

    if hist_docs:
        hist_df = pd.DataFrame(hist_docs)
        hist_df["event_timestamp"] = pd.to_datetime(hist_df["event_timestamp"])
        hist_df = hist_df.set_index("event_timestamp").sort_index()
        st.line_chart(hist_df["congestion_score"])
    else:
        st.info("No history yet for this station in the past 24 hours.")

# ---------------------------------------------------------------------------
# Cassandra round-trip: Times Sq hourly capacity baseline
# ---------------------------------------------------------------------------

st.subheader("Times Sq-42 St (ID 611) — Hourly Capacity Baseline (clear weather)")
hourly = load_station_baseline("611")
if hourly.empty:
    st.info("No baseline data yet. Run processing/build_baseline.py --sink cassandra to populate.")
else:
    st.line_chart(hourly.set_index("hour_of_day")["avg_entries"])
