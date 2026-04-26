# Subway Dash

Real-time MTA subway congestion pipeline and live dashboard.

Ingests live GTFS-Realtime feeds, computes per-station congestion scores using a Lambda architecture, and surfaces them on an interactive map.

**Team**
| Member | Role |
|---|---|
| Arjun Bajpai | Data Ingestion & Infrastructure |
| Preyansh Agrawal | Processing, Joins & Prediction |
| Yash Jain | Storage, Serving Layer & Visualization |

---

## Stack

| Layer | Technology |
|---|---|
| Message bus | Apache Kafka |
| Batch + stream processing | PySpark |
| Speed-layer store | MongoDB |
| Batch-layer store | Cassandra |
| Serving | FastAPI |
| Dashboard | Streamlit + PyDeck |

---

## Prerequisites

- Docker Desktop
- Python 3.8+
- `pymongo`, `cassandra-driver`, `streamlit`, `pydeck` (see below)

Install Python dependencies:
```bash
pip install pymongo cassandra-driver streamlit pydeck fastapi uvicorn
```

---

## Quick Start

**1. Start databases**
```bash
cd infra
docker compose up -d
```

**2. Initialize MongoDB TTL index** (run once after first `docker compose up`)
```bash
python infra/init_mongo.py
```

**3. Verify connectivity**
```bash
python infra/verify_connections.py
```

**4. Run the dashboard**
```bash
streamlit run dashboard/app.py
```
Dashboard available at `http://localhost:8501`.

---

## Configuration

All connection settings are read from environment variables:

| Variable | Default | Description |
|---|---|---|
| `MONGO_URI` | `mongodb://localhost:27017` | MongoDB connection string |
| `CASSANDRA_HOSTS` | `localhost` | Comma-separated Cassandra host list |

---

## Project Structure

```
subway-dash/
├── infra/           # Docker Compose, DB init scripts
├── ingestion/       # Kafka producers, GTFS + weather pollers
├── processing/      # PySpark batch and streaming jobs
├── serving/         # FastAPI app
├── dashboard/       # Streamlit app
├── data/            # Bridge table and local data files
└── docs/            # Analytics report
```

---

## Data Sources

- **MTA GTFS-Realtime** — live vehicle positions and trip updates (public, no API key required)
- **MTA Hourly Ridership** — historical tap-in counts per station (NY Open Data)
- **MTA Stations Dataset** — station complex metadata and GTFS stop mappings (NY Open Data)
- **NOAA GHCND** — historical daily weather for baseline computation
- **NWS API** — real-time current weather (`api.weather.gov`, no API key required)
