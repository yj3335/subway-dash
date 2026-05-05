import os

import pandas as pd
import pydeck
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET
from serving.db_clients import get_cassandra_session

API_URL = os.environ.get("SUBWAY_DASH_API_URL", "http://localhost:8000")

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
    try:
        resp = requests.get(f"{API_URL}/api/v1/stations/all", timeout=5)
        resp.raise_for_status()
    except Exception as exc:
        st.warning(f"API unavailable: {exc}")
        return pd.DataFrame()
    docs = resp.json()
    if not docs:
        return pd.DataFrame()
    return pd.DataFrame(docs)[["complex_name", "alert_level", "congestion_score",
                                "avg_arrival_delay_secs", "event_timestamp", "weather_bucket"]]


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
    # average across all days of week for a clean hourly profile
    return df.groupby("hour_of_day", as_index=False)["avg_entries"].mean().sort_values("hour_of_day")


# --- Map ---
stations = load_stations()

layer = pydeck.Layer(
    "ScatterplotLayer",
    data=stations,
    get_position="[lon, lat]",
    get_fill_color=[220, 0, 0],
    get_radius=200,
    pickable=True,
)

view = pydeck.ViewState(latitude=40.73, longitude=-73.98, zoom=11)

st.pydeck_chart(
    pydeck.Deck(layers=[layer], initial_view_state=view,
                tooltip={"text": "{name}"}),
    use_container_width=True,
)

# --- Station congestion status (via FastAPI) ---
st.subheader("Station Congestion Status")
statuses = load_station_statuses()
if statuses.empty:
    st.info("No congestion data yet. Start the API server and run processing/lambda_merge.py.")
else:
    st.dataframe(statuses, use_container_width=True)

# --- Cassandra round-trip: Times Sq hourly capacity baseline ---
st.subheader("Times Sq-42 St — Hourly Capacity Baseline (clear weather)")
hourly = load_times_sq_hourly()
if hourly.empty:
    st.info("No baseline data yet. Run processing/build_baseline.py --sink cassandra to populate.")
else:
    st.line_chart(hourly.set_index("hour_of_day")["avg_entries"])
