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
_ALERT_SORT  = {"SEVERE": 0, "MODERATE": 1, "NORMAL": 2}
_BADGE       = {"SEVERE": "🔴 SEVERE", "MODERATE": "🟡 MODERATE", "NORMAL": "🟢 NORMAL"}

st.set_page_config(page_title="Subway Dash", layout="wide")
st_autorefresh(interval=30_000, key="autorefresh")


# ---------------------------------------------------------------------------
# Cached loaders
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
# Live data
# ---------------------------------------------------------------------------

def load_station_statuses() -> pd.DataFrame:
    try:
        resp = requests.get(f"{API_URL}/api/v1/stations/all", timeout=5)
        resp.raise_for_status()
    except Exception:
        st.warning("Live data unavailable — check that the API server is running.")
        return pd.DataFrame()
    docs = resp.json()
    return pd.DataFrame(docs) if docs else pd.DataFrame()


# ---------------------------------------------------------------------------
# Helpers
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


def _relative_time(ts_str) -> str:
    if pd.isna(ts_str):
        return "—"
    try:
        ts = pd.to_datetime(ts_str, utc=True)
        delta = int((datetime.now(timezone.utc) - ts).total_seconds())
        if delta < 60:
            return f"{delta}s ago"
        if delta < 3600:
            return f"{delta // 60}m ago"
        return f"{delta // 3600}h ago"
    except Exception:
        return "—"


# ---------------------------------------------------------------------------
# Fetch live data
# ---------------------------------------------------------------------------

stations = load_stations()
statuses = load_station_statuses()
map_df   = build_map_data(stations, statuses)

# ---------------------------------------------------------------------------
# Sidebar — line filter only
# ---------------------------------------------------------------------------

st.sidebar.markdown("### Filter by line")
all_lines = get_all_lines(stations)
selected_lines = st.sidebar.multiselect("Subway line", all_lines, label_visibility="collapsed")
filtered_map_df = filter_by_lines(map_df, selected_lines)

# ---------------------------------------------------------------------------
# Title + SEVERE-only alert banner
# ---------------------------------------------------------------------------

st.title("Subway Dash — Live Congestion")

if not statuses.empty:
    severe_n = int((statuses["alert_level"] == "SEVERE").sum())
    if severe_n:
        st.error(f"🔴 **SEVERE** congestion at **{severe_n}** station(s) — see red markers on map.")

# ---------------------------------------------------------------------------
# KPI tiles
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)
if statuses.empty:
    for col, label in zip(
        [c1, c2, c3, c4, c5],
        ["Stations Monitored", "🔴 Severe", "🟡 Moderate", "🟢 Normal", "Last Updated"],
    ):
        col.metric(label, "—")
else:
    c1.metric("Stations Monitored", len(statuses))
    c2.metric("🔴 Severe",   int((statuses["alert_level"] == "SEVERE").sum()))
    c3.metric("🟡 Moderate", int((statuses["alert_level"] == "MODERATE").sum()))
    c4.metric("🟢 Normal",   int((statuses["alert_level"] == "NORMAL").sum()))
    c5.metric("Last Updated", _freshness_str(statuses))

# ---------------------------------------------------------------------------
# Map + legend caption
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

st.pydeck_chart(
    pydeck.Deck(
        layers=[layer],
        initial_view_state=pydeck.ViewState(latitude=40.73, longitude=-73.98, zoom=11),
        tooltip={"text": "{name}\n{alert_level}  ·  Score {congestion_score}  ·  Delay {avg_arrival_delay_secs}s"},
    ),
    use_container_width=True,
)
st.caption(
    "🔴 SEVERE · 🟡 MODERATE · 🟢 NORMAL · ⚫ No data    —    Marker size scales with congestion score"
)

# ---------------------------------------------------------------------------
# Station congestion table — sorted by severity, relative timestamps
# ---------------------------------------------------------------------------

st.subheader("Station Congestion Status")
if statuses.empty:
    st.info("No live data yet. The feed populates automatically once the pipeline is running.")
else:
    display = statuses[
        ["complex_name", "alert_level", "congestion_score", "avg_arrival_delay_secs",
         "event_timestamp", "weather_bucket"]
    ].copy()
    display["_sort"] = display["alert_level"].map(lambda s: _ALERT_SORT.get(s, 99))
    display = display.sort_values("_sort").drop(columns="_sort").reset_index(drop=True)
    display["event_timestamp"] = display["event_timestamp"].apply(_relative_time)
    display.columns = ["Station", "Status", "Score", "Avg Delay (s)", "Updated", "Weather"]
    display["Status"] = display["Status"].map(lambda s: _BADGE.get(s, s))
    st.dataframe(
        display,
        column_config={
            "Score": st.column_config.ProgressColumn("Score", min_value=0, max_value=1, format="%.2f"),
        },
        use_container_width=True,
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# Drill-down tabs: 24h History | Next-Hour Forecast
# ---------------------------------------------------------------------------

tab_hist, tab_forecast = st.tabs(["📈 24h History", "🔮 Next-Hour Forecast"])

with tab_hist:
    station_names = stations["name"].sort_values().tolist()
    selected_name = st.selectbox("Station", station_names, key="history_station")
    selected_id = stations.loc[stations["name"] == selected_name, "station_complex_id"].values[0]

    try:
        hist_resp = requests.get(f"{API_URL}/api/v1/station/{selected_id}/history", timeout=5)
        hist_resp.raise_for_status()
        hist_docs = hist_resp.json()
    except Exception:
        hist_docs = []
        st.warning("Could not load history — check API server.")

    if hist_docs:
        hist_df = pd.DataFrame(hist_docs)
        hist_df["event_timestamp"] = pd.to_datetime(hist_df["event_timestamp"])
        hist_df = hist_df.set_index("event_timestamp").sort_index()
        st.line_chart(hist_df["congestion_score"])
    else:
        st.info("No history yet for this station. Data accumulates as the pipeline runs.")

with tab_forecast:
    now_utc   = datetime.now(timezone.utc)
    next_hour = (now_utc.hour + 1) % 24
    st.caption(
        f"Expected ridership based on historical baseline (clear weather). "
        f"Current: **{now_utc.hour:02d}:00** → Next: **{next_hour:02d}:00**"
    )

    forecast_name = st.selectbox(
        "Station", stations["name"].sort_values().tolist(), key="forecast_station"
    )
    forecast_id  = stations.loc[stations["name"] == forecast_name, "station_complex_id"].values[0]
    baseline_df  = load_station_baseline(forecast_id)

    if baseline_df.empty:
        st.info("Baseline data not yet available. It populates after the first nightly batch run.")
    else:
        cur_row  = baseline_df[baseline_df["hour_of_day"] == now_utc.hour]["avg_entries"].values
        next_row = baseline_df[baseline_df["hour_of_day"] == next_hour]["avg_entries"].values
        cur_val  = int(cur_row[0])  if len(cur_row)  else None
        next_val = int(next_row[0]) if len(next_row) else None

        fc1, fc2, fc3 = st.columns([1, 1, 2])
        fc1.metric(f"Now ({now_utc.hour:02d}:00)", f"{cur_val:,}" if cur_val is not None else "—")
        fc2.metric(
            f"Next ({next_hour:02d}:00)",
            f"{next_val:,}" if next_val is not None else "—",
            delta=f"{next_val - cur_val:+,}" if cur_val is not None and next_val is not None else None,
        )
        with fc3:
            st.caption("Full day baseline")
            st.line_chart(baseline_df.set_index("hour_of_day")["avg_entries"], height=120)
