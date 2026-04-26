import pydeck
import streamlit as st

st.set_page_config(page_title="Subway Dash", layout="wide")
st.title("Subway Dash — Live Congestion")

# 5 hardcoded test stations (Times Sq, Grand Central, Atlantic Av, 34 St-Penn, Fulton St)
TEST_STATIONS = [
    {"name": "Times Sq-42 St",     "lat": 40.7558, "lon": -73.9878},
    {"name": "Grand Central-42 St","lat": 40.7527, "lon": -73.9772},
    {"name": "Atlantic Av-Barclays","lat": 40.6843, "lon": -73.9779},
    {"name": "34 St-Penn Station", "lat": 40.7506, "lon": -73.9971},
    {"name": "Fulton St",          "lat": 40.7092, "lon": -74.0072},
]

layer = pydeck.Layer(
    "ScatterplotLayer",
    data=TEST_STATIONS,
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
