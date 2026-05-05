import pandas as pd
import pydeck
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET
from serving.db_clients import get_mongo_collection

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

st.subheader("Recent Speed-Layer Updates")
recent = load_recent_speed_layer()
if recent.empty:
    st.info("No speed-layer data yet. Run processing/lambda_merge.py to populate.")
else:
    st.dataframe(recent, use_container_width=True)
