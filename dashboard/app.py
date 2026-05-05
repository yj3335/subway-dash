import pandas as pd
import pydeck
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET
from serving.db_clients import get_cassandra_session, get_mongo_collection

st.set_page_config(page_title="Subway Dash", layout="wide")
st.title("Subway Dash — Live Congestion")

# Refresh every 30 seconds
st_autorefresh(interval=30_000, key="autorefresh")


@st.cache_data
def load_stations() -> pd.DataFrame:
    df = pd.read_parquet(BRIDGE_PARQUET, columns=["station_complex_id", "complex_name", "lat", "lon"])
    df = df.drop_duplicates("station_complex_id")
    return df.rename(columns={"complex_name": "name"})


def load_recent_speed_layer() -> pd.DataFrame:
    col = get_mongo_collection("speed_layer")
    docs = list(
        col.find({}, {"_id": 0,
                      "complex_name": 1,
                      "alert_level": 1,
                      "congestion_score": 1,
                      "avg_arrival_delay_secs": 1,
                      "event_timestamp": 1,
                      "weather_bucket": 1})
           .sort("inserted_at", -1)
           .limit(10)
    )
    if not docs:
        return pd.DataFrame(columns=["complex_name", "alert_level", "congestion_score",
                                     "avg_arrival_delay_secs", "event_timestamp", "weather_bucket"])
    return pd.DataFrame(docs)


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

# --- Recent speed-layer updates ---
st.subheader("Recent Speed-Layer Updates")
recent = load_recent_speed_layer()
if recent.empty:
    st.info("No speed-layer data yet. Run processing/lambda_merge.py to populate.")
else:
    st.dataframe(recent, use_container_width=True)

# --- Cassandra round-trip: Times Sq hourly capacity baseline ---
st.subheader("Times Sq-42 St — Hourly Capacity Baseline (clear weather)")
hourly = load_times_sq_hourly()
if hourly.empty:
    st.info("No baseline data yet. Run processing/build_baseline.py --sink cassandra to populate.")
else:
    st.line_chart(hourly.set_index("hour_of_day")["avg_entries"])
