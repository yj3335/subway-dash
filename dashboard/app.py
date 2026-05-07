import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Streamlit adds the script directory (dashboard/) to sys.path, not the project
# root. Insert the root explicitly so processing.* and serving.* are importable.
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

import altair as alt
import pandas as pd
import pydeck
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from processing.config import BRIDGE_PARQUET

API_URL = os.environ.get("SUBWAY_DASH_API_URL", "http://localhost:8000")
_NYC = ZoneInfo("America/New_York")

# ---------------------------------------------------------------------------
# MTA brand colors
# ---------------------------------------------------------------------------

MTA_LINE_COLORS = {
    "A": "#0039A6", "C": "#0039A6", "E": "#0039A6",
    "B": "#FF6319", "D": "#FF6319", "F": "#FF6319", "M": "#FF6319",
    "G": "#6CBE45",
    "J": "#996633", "Z": "#996633",
    "L": "#A7A9AC",
    "N": "#FCCC0A", "Q": "#FCCC0A", "R": "#FCCC0A", "W": "#FCCC0A",
    "1": "#EE352E", "2": "#EE352E", "3": "#EE352E",
    "4": "#00933C", "5": "#00933C", "6": "#00933C",
    "7": "#B933AD",
    "S": "#808183",
}
# Lines with light backgrounds that need dark text
_LIGHT_BG_LINES = {"N", "Q", "R", "W", "L", "S"}

_ALERT_COLORS = {
    "NORMAL":   [0, 200, 0, 180],
    "MODERATE": [255, 99, 25, 210],
    "SEVERE":   [238, 53, 46, 230],
}
_NO_DATA_COLOR = [128, 128, 128, 100]
_ALERT_SORT    = {"SEVERE": 0, "MODERATE": 1, "NORMAL": 2}
_BADGE         = {"SEVERE": "🔴 SEVERE", "MODERATE": "🟡 MODERATE", "NORMAL": "🟢 NORMAL"}

st.set_page_config(page_title="Subway Dash", layout="wide")
st_autorefresh(interval=30_000, key="autorefresh")


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------

def _line_badge(line: str) -> str:
    color = MTA_LINE_COLORS.get(line, "#555555")
    text_color = "#000000" if line in _LIGHT_BG_LINES else "#ffffff"
    return (
        f'<span style="display:inline-block;width:20px;height:20px;border-radius:50%;'
        f'background:{color};color:{text_color};text-align:center;line-height:20px;'
        f'font-size:11px;font-weight:700;margin-right:3px;">{line}</span>'
    )


def _lines_html(routes_str) -> str:
    if pd.isna(routes_str):
        return "—"
    return "".join(_line_badge(r) for r in str(routes_str).split())


def _kpi_card(label: str, value, color: str = "#0039A6") -> str:
    return (
        f'<div style="background:{color}22;border-left:4px solid {color};'
        f'padding:12px 16px;border-radius:8px;">'
        f'<div style="font-size:11px;color:#aaa;text-transform:uppercase;'
        f'letter-spacing:0.5px;margin-bottom:4px;">{label}</div>'
        f'<div style="font-size:28px;font-weight:700;color:{color};">{value}</div>'
        f'</div>'
    )


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


def _freshness_str(statuses_df: pd.DataFrame) -> tuple[str, bool]:
    """Returns (display_str, is_stale). Stale = last update > 90s ago."""
    if statuses_df.empty or "inserted_at" not in statuses_df.columns:
        return "—", True
    latest = pd.to_datetime(statuses_df["inserted_at"], utc=True).max()
    if pd.isna(latest):
        return "—", True
    delta = int((datetime.now(timezone.utc) - latest).total_seconds())
    is_stale = delta > 90
    if delta < 60:
        label = f"{delta}s ago"
    else:
        label = f"{delta // 60}m {delta % 60}s ago"
    return label, is_stale


# ---------------------------------------------------------------------------
# Cached loaders
# ---------------------------------------------------------------------------

def _merge_routes(series) -> str | None:
    tokens: set[str] = set()
    for val in series.dropna():
        tokens.update(str(val).split())
    return " ".join(sorted(tokens)) if tokens else None


@st.cache_data(ttl=3600)
def load_stations() -> pd.DataFrame:
    df = pd.read_parquet(
        BRIDGE_PARQUET,
        columns=["station_complex_id", "complex_name", "lat", "lon", "daytime_routes"],
    )
    # Aggregate all route tokens across every stop that shares a complex ID.
    # drop_duplicates would silently discard routes that only appear on some stops
    # (e.g. Atlantic Av-Barclays Ctr serves A/C/2/3/4/5/B/D/N/Q/R across many rows).
    agg = df.groupby("station_complex_id", as_index=False).agg(
        complex_name=("complex_name", "first"),
        lat=("lat", "mean"),
        lon=("lon", "mean"),
        daytime_routes=("daytime_routes", _merge_routes),
    )
    return agg.rename(columns={"complex_name": "name"})


@st.cache_data
def get_all_lines(stations_df: pd.DataFrame) -> list[str]:
    lines: set[str] = set()
    for val in stations_df["daytime_routes"].dropna():
        lines.update(str(val).split())
    return sorted(lines)


@st.cache_data(ttl=3600)
def load_station_baseline(station_id: str) -> pd.DataFrame:
    """Fetch the full 24-hour baseline via the serving layer."""
    try:
        resp = requests.get(
            f"{API_URL}/api/v1/station/{station_id}/baseline", timeout=5
        )
        if resp.status_code == 404:
            return pd.DataFrame()
        resp.raise_for_status()
        rows = resp.json()
    except Exception:
        return pd.DataFrame()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return df.groupby("hour_of_day", as_index=False)["avg_entries"].mean().sort_values("hour_of_day")


# ---------------------------------------------------------------------------
# Live data
# ---------------------------------------------------------------------------

def load_station_statuses() -> pd.DataFrame:
    try:
        resp = requests.get(f"{API_URL}/api/v1/stations/all", timeout=5)
        resp.raise_for_status()
        docs = resp.json()
        df = pd.DataFrame(docs) if docs else pd.DataFrame()
        if not df.empty:
            st.session_state["last_fetch_ok"] = datetime.now(timezone.utc)
            # Remember alert states for escalation toasts
            prev = st.session_state.get("prev_alerts", {})
            curr = dict(zip(df["station_complex_id"], df["alert_level"]))
            for sid, level in curr.items():
                if level == "SEVERE" and prev.get(sid) != "SEVERE":
                    name_rows = df.loc[df["station_complex_id"] == sid, "complex_name"]
                    name = name_rows.values[0] if len(name_rows) else sid
                    st.toast(f"🔴 {name} escalated to SEVERE", icon="🚨")
            st.session_state["prev_alerts"] = curr
        return df
    except Exception as exc:
        st.session_state["last_api_error"] = str(exc)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Map data helpers
# ---------------------------------------------------------------------------

def _crowding_label(score: float) -> str:
    """Translate the internal congestion score into a rider-friendly label."""
    if score >= 0.50:
        return "Very Busy"
    if score >= 0.20:
        return "Busy"
    if score >= 0.05:
        return "Moderate"
    return "Quiet"


def _delay_label(secs) -> str:
    """Format delay seconds into a short human-readable string."""
    if pd.isna(secs) or secs is None:
        return "On time"
    s = float(secs)
    if s <= 30:
        return "On time"
    if s < 60:
        return "<1 min"
    m = s / 60
    if m < 10:
        return f"~{m:.0f} min"
    return f"{m:.0f} min"


def build_map_data(stations_df: pd.DataFrame, statuses_df: pd.DataFrame) -> pd.DataFrame:
    if statuses_df.empty:
        df = stations_df.copy()
        df["alert_level"] = "N/A"
        df["congestion_score"] = 0.0
        df["avg_arrival_delay_secs"] = None
    else:
        df = stations_df.merge(
            statuses_df[["station_complex_id", "alert_level", "congestion_score",
                         "avg_arrival_delay_secs"]],
            on="station_complex_id",
            how="left",
        )
        df["alert_level"] = df["alert_level"].fillna("N/A")
        df["congestion_score"] = df["congestion_score"].fillna(0.0)
    df["color"] = df["alert_level"].map(lambda lvl: _ALERT_COLORS.get(lvl, _NO_DATA_COLOR))
    df["radius"] = (100 + df["congestion_score"].fillna(0.0) * 300).clip(100, 400)

    # Human-readable labels for tooltip
    df["crowding"] = df["congestion_score"].apply(_crowding_label)
    df["delay"] = df["avg_arrival_delay_secs"].apply(_delay_label)

    # Pre-render MTA line badges for HTML tooltip
    df["lines_html"] = df["daytime_routes"].apply(_lines_html)
    return df


def filter_by_lines(map_df: pd.DataFrame, selected_lines: list[str]) -> pd.DataFrame:
    if not selected_lines:
        return map_df
    selected_set = set(selected_lines)
    mask = map_df["daytime_routes"].apply(
        lambda r: bool(set(str(r).split()) & selected_set) if pd.notna(r) else False
    )
    return map_df[mask]


# ---------------------------------------------------------------------------
# Chart helper
# ---------------------------------------------------------------------------

def _history_altair(hist_df: pd.DataFrame):
    data = hist_df.reset_index()
    base = alt.Chart(data).mark_line(
        color="#0039A6", strokeWidth=2, interpolate="monotone"
    ).encode(
        x=alt.X("event_timestamp:T", title="Time"),
        y=alt.Y("congestion_score:Q", title="Congestion Score",
                scale=alt.Scale(domain=[0, 1])),
        tooltip=[
            alt.Tooltip("event_timestamp:T", title="Time"),
            alt.Tooltip("congestion_score:Q", title="Score", format=".3f"),
        ],
    )
    moderate_line = alt.Chart(
        pd.DataFrame({"y": [0.20], "label": ["MODERATE threshold"]})
    ).mark_rule(color="#FF6319", strokeDash=[6, 4], strokeWidth=1.5).encode(y="y:Q")

    severe_line = alt.Chart(
        pd.DataFrame({"y": [0.50], "label": ["SEVERE threshold"]})
    ).mark_rule(color="#EE352E", strokeDash=[6, 4], strokeWidth=1.5).encode(y="y:Q")

    return base + moderate_line + severe_line


def _system_health_strip():
    try:
        resp = requests.get(f"{API_URL}/health", timeout=3)
        h = resp.json()
        api_ok       = resp.status_code == 200
        mongo_ok     = h.get("mongo") == "ok"
        cassandra_ok = h.get("cassandra") == "ok"
        parts = [
            f"{'🟢' if mongo_ok else '🔴'} MongoDB",
            f"{'🟢' if cassandra_ok else '🔴'} Cassandra",
            f"{'🟢' if api_ok else '🟡'} API",
        ]
        st.caption("  ·  ".join(parts))
    except Exception:
        st.caption("🔴 API offline")


# ---------------------------------------------------------------------------
# Fetch live data
# ---------------------------------------------------------------------------

stations = load_stations()
statuses = load_station_statuses()
map_df   = build_map_data(stations, statuses)

freshness_label, is_stale = _freshness_str(statuses)

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### Filter by line")
    all_lines = get_all_lines(stations)
    selected_lines = st.multiselect("Subway line", all_lines, label_visibility="collapsed")

    st.divider()

    # Prominent freshness indicator
    if statuses.empty:
        last_ok = st.session_state.get("last_fetch_ok")
        if last_ok:
            delta = int((datetime.now(timezone.utc) - last_ok).total_seconds())
            ago = f"{delta}s ago" if delta < 60 else f"{delta // 60}m ago"
            st.error(f"⚠ Pipeline offline\nLast data: {ago}")
        else:
            st.warning("⚠ Waiting for data…")
        err = st.session_state.get("last_api_error", "")
        if err:
            st.caption(f"Last error: {err[:120]}")
    elif is_stale:
        st.warning(f"⚠ Data may be stale\n{freshness_label}")
    else:
        st.success(f"🟢 Live · {freshness_label}")

filtered_map_df = filter_by_lines(map_df, selected_lines)

# ---------------------------------------------------------------------------
# Styled header
# ---------------------------------------------------------------------------

st.markdown("""
<div style="display:flex;align-items:center;gap:14px;margin-bottom:8px;">
    <div style="background:#0039A6;color:#fff;font-size:26px;font-weight:800;
                padding:8px 18px;border-radius:8px;letter-spacing:-0.5px;">
        🚇 SUBWAY DASH
    </div>
    <div style="font-size:13px;color:#888;">Real-Time Congestion Monitor · NYC MTA</div>
</div>
""", unsafe_allow_html=True)

_system_health_strip()

# SEVERE-only alert banner
if not statuses.empty:
    severe_n = int((statuses["alert_level"] == "SEVERE").sum())
    if severe_n:
        st.error(f"🔴 **SEVERE** congestion at **{severe_n}** station(s) — see red markers on map.")

# ---------------------------------------------------------------------------
# Styled KPI tiles
# ---------------------------------------------------------------------------

c1, c2, c3, c4, c5 = st.columns(5)
if statuses.empty:
    tiles = [("Stations Monitored", "—", "#555"), ("Severe", "—", "#EE352E"),
             ("Moderate", "—", "#FF6319"), ("Normal", "—", "#00933C"),
             ("Last Updated", "—", "#555")]
else:
    tiles = [
        ("Stations Monitored", len(statuses), "#0039A6"),
        ("Severe",   int((statuses["alert_level"] == "SEVERE").sum()),   "#EE352E"),
        ("Moderate", int((statuses["alert_level"] == "MODERATE").sum()), "#FF6319"),
        ("Normal",   int((statuses["alert_level"] == "NORMAL").sum()),   "#00933C"),
        ("Last Updated", freshness_label, "#555555" if is_stale else "#0039A6"),
    ]

for col, (label, value, color) in zip([c1, c2, c3, c4, c5], tiles):
    col.markdown(_kpi_card(label, value, color), unsafe_allow_html=True)

st.markdown("<div style='margin-top:8px;'></div>", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# Map — dark basemap, pitch, HTML tooltip with MTA badges
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

tooltip = {
    "html": """
    <div style="font-family:'Inter',sans-serif;padding:10px 14px;min-width:210px;">
        <div style="font-size:15px;font-weight:700;margin-bottom:2px;">{name}</div>
        <div style="margin-bottom:8px;">{lines_html}</div>
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
            <span style="font-size:12px;color:#aaa;">Status</span>
            <span style="font-size:13px;font-weight:600;">{alert_level}</span>
        </div>
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
            <span style="font-size:12px;color:#aaa;">Crowding</span>
            <span style="font-size:13px;font-weight:600;">{crowding}</span>
        </div>
        <div style="display:flex;justify-content:space-between;">
            <span style="font-size:12px;color:#aaa;">Delay</span>
            <span style="font-size:13px;font-weight:600;">{delay}</span>
        </div>
    </div>
    """,
    "style": {
        "backgroundColor": "#1A1D23",
        "color": "#FAFAFA",
        "border": "1px solid #333",
        "borderRadius": "8px",
    },
}

st.pydeck_chart(
    pydeck.Deck(
        layers=[layer],
        initial_view_state=pydeck.ViewState(
            latitude=40.73, longitude=-73.98, zoom=11, pitch=30
        ),
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
        tooltip=tooltip,
    ),
    width="stretch",
)
st.caption(
    "🔴 SEVERE · 🟡 MODERATE · 🟢 NORMAL · ⚫ No data    —    "
    "Marker size scales with congestion level"
)

# ---------------------------------------------------------------------------
# Station congestion table
# ---------------------------------------------------------------------------

st.subheader("Station Congestion Status")

with st.expander("ℹ️ How scores are calculated"):
    st.markdown("""
| Field | Meaning |
|---|---|
| **Congestion Score** | `service_deficit × demand_intensity` — a 0–1 index combining train reliability and passenger load |
| **Service Deficit** | How late trains are, normalised: `0` = on time, `1` = fully saturated (arrival delay ≥ 300 s) |
| **Demand Intensity** | Station entries relative to its historical peak: `actual entries ÷ max recorded entries` |
| **Avg Arrival Delay** | Mean seconds trains sat at this station beyond their scheduled arrival in the last window |
| **Crowding label** | Quiet < 0.05 · Moderate 0.05–0.20 · Busy 0.20–0.50 · Very Busy ≥ 0.50 |
| **Alert level** | 🟢 NORMAL < 0.20 · 🟡 MODERATE 0.20–0.50 · 🔴 SEVERE ≥ 0.50 |
""")

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
    display["crowding"] = display["congestion_score"].apply(_crowding_label)
    display["delay"] = display["avg_arrival_delay_secs"].apply(_delay_label)
    display["Status"] = display["alert_level"].map(lambda s: _BADGE.get(s, s))
    display["score_fmt"] = display["congestion_score"].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
    display = display[["complex_name", "Status", "score_fmt", "crowding", "delay",
                       "event_timestamp", "weather_bucket"]]
    display.columns = ["Station", "Status", "Score", "Crowding", "Delay", "Updated", "Weather"]
    st.dataframe(
        display,
        width="stretch",
        hide_index=True,
    )

# ---------------------------------------------------------------------------
# Drill-down tabs
# ---------------------------------------------------------------------------

tab_hist, tab_forecast = st.tabs(["📈 24h History", "🔮 Next-Hour Forecast"])

with tab_hist:
    station_names = stations["name"].sort_values().tolist()
    selected_name = st.selectbox("Station", station_names, key="history_station")
    selected_id = stations.loc[
        stations["name"] == selected_name, "station_complex_id"
    ].values[0]

    with st.spinner("Loading history…"):
        try:
            hist_resp = requests.get(
                f"{API_URL}/api/v1/station/{selected_id}/history", timeout=5
            )
            hist_resp.raise_for_status()
            hist_docs = hist_resp.json()
        except Exception:
            hist_docs = []
            st.warning("Could not load history — check API server.")

    if hist_docs:
        hist_df = pd.DataFrame(hist_docs)
        # Parse as UTC so Vega-Lite converts to browser local time for display
        hist_df["event_timestamp"] = pd.to_datetime(hist_df["event_timestamp"], utc=True)
        hist_df = hist_df.set_index("event_timestamp").sort_index()
        st.altair_chart(_history_altair(hist_df), width="stretch")
        st.caption("— — Dashed orange = MODERATE threshold (0.20)  ·  Dashed red = SEVERE threshold (0.50)  ·  Times shown in your local timezone")
    else:
        st.info("No history yet for this station. Data accumulates as the pipeline runs.")

with tab_forecast:
    now_local = datetime.now(_NYC)  # NYC local time — matches how baseline hour_of_day was recorded
    next_hour = (now_local.hour + 1) % 24
    st.caption(
        f"Expected ridership based on historical baseline (clear weather). "
        f"Current: **{now_local.hour:02d}:00** → Next: **{next_hour:02d}:00** (NYC time)"
    )

    forecast_name = st.selectbox(
        "Station", stations["name"].sort_values().tolist(), key="forecast_station"
    )
    forecast_id  = stations.loc[
        stations["name"] == forecast_name, "station_complex_id"
    ].values[0]

    with st.spinner("Loading baseline…"):
        baseline_df = load_station_baseline(forecast_id)

    if baseline_df.empty:
        st.info("Baseline data not yet available. It populates after the first nightly batch run.")
    else:
        cur_row  = baseline_df[baseline_df["hour_of_day"] == now_local.hour]["avg_entries"].values
        next_row = baseline_df[baseline_df["hour_of_day"] == next_hour]["avg_entries"].values
        cur_val  = int(cur_row[0])  if len(cur_row)  and pd.notna(cur_row[0])  else None
        next_val = int(next_row[0]) if len(next_row) and pd.notna(next_row[0]) else None

        fc1, fc2, fc3 = st.columns([1, 1, 2])
        fc1.metric(
            f"Now ({now_local.hour:02d}:00)",
            f"{cur_val:,}" if cur_val is not None else "—",
        )
        fc2.metric(
            f"Next ({next_hour:02d}:00)",
            f"{next_val:,}" if next_val is not None else "—",
            delta=f"{next_val - cur_val:+,}"
            if cur_val is not None and next_val is not None else None,
        )
        with fc3:
            st.caption("Full day baseline")
            st.line_chart(
                baseline_df.set_index("hour_of_day")["avg_entries"], height=120
            )
