import os

import pandas as pd
import pydeck
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET
from serving.db_clients import get_cassandra_session

API_URL = os.environ.get("SUBWAY_DASH_API_URL", "http://localhost:8000")

_ALERT_COLORS = {
    "NORMAL":   [0, 200, 0],
    "MODERATE": [255, 200, 0],
    "SEVERE":   [220, 0, 0],
}
_NO_DATA_COLOR = [128, 128, 128]

st.set_page_config(page_title="Subway Dash", layout="wide")
st.title("Subway Dash — Live Congestion")

# Refresh every 30 seconds
st_autorefresh(interval=30_000, key="autorefresh")


@st.cache_data
def load_stations() -> pd.DataFrame:
    df = pd.read_parquet(BRIDGE_PARQUET, columns=["station_complex_id", "complex_name", "lat", "lon"])
    df = df.drop_duplicates("station_complex_id")
    return df.rename(columns={"complex_name": "name"})


def load_station_statuses() -> pd.DataFrame:
    """Returns latest congestion doc per station from the API, or empty DataFrame on failure."""
    try:
        resp = requests.get(f"{API_URL}/api/v1/stations/all", timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        st.warning(f"API unavailable: {exc}")
        return pd.DataFrame()
    docs = resp.json()
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)


def build_map_data(stations_df: pd.DataFrame, statuses_df: pd.DataFrame) -> pd.DataFrame:
    """Merge bridge lat/lon with congestion statuses; add color column for PyDeck."""
    if statuses_df.empty:
        df = stations_df.copy()
        df["alert_level"] = "N/A"
        df["congestion_score"] = None
        df["avg_arrival_delay_secs"] = None
    else:
        df = stations_df.merge(
            statuses_df[["station_complex_id", "alert_level", "congestion_score", "avg_arrival_delay_secs"]],
            on="station_complex_id",
            how="left",
        )
        df["alert_level"] = df["alert_level"].fillna("N/A")
    df["color"] = df["alert_level"].map(lambda lvl: _ALERT_COLORS.get(lvl, _NO_DATA_COLOR))
    return df


def load_times_sq_hourly() -> pd.DataFrame:
    session = get_cassandra_session()
    rows = session.execute(
        "SELECT hour_of_day, avg_entries "
        "FROM subway_dash.station_capacity_baseline "
        "WHERE station_complex_id = '613' AND weather_bucket = 'clear'"
    )
    df = pd.DataFrame(list(rows), columns=["hour_of_day", "avg_entries"])
    if df.empty:
        return df
    return df.groupby("hour_of_day", as_index=False)["avg_entries"].mean().sort_values("hour_of_day")


# --- Fetch data ---
stations = load_stations()
statuses = load_station_statuses()
map_df = build_map_data(stations, statuses)

# --- Map ---
layer = pydeck.Layer(
    "ScatterplotLayer",
    data=map_df,
    get_position="[lon, lat]",
    get_fill_color="color",
    get_radius=200,
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

# --- Station congestion table ---
st.subheader("Station Congestion Status")
if statuses.empty:
    st.info("No congestion data yet. Start the API server and run processing/lambda_merge.py.")
else:
    display_cols = ["complex_name", "alert_level", "congestion_score", "avg_arrival_delay_secs",
                    "event_timestamp", "weather_bucket"]
    st.dataframe(statuses[display_cols], use_container_width=True)

# --- 24-hour time-series chart (drill-down per station) ---
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

# --- Cassandra round-trip: Times Sq hourly capacity baseline ---
st.subheader("Times Sq-42 St — Hourly Capacity Baseline (clear weather)")
hourly = load_times_sq_hourly()
if hourly.empty:
    st.info("No baseline data yet. Run processing/build_baseline.py --sink cassandra to populate.")
else:
    st.line_chart(hourly.set_index("hour_of_day")["avg_entries"])
