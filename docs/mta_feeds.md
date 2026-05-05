# MTA GTFS-Realtime Feed Endpoints

**Task A2.1** — Source-of-truth list of the public MTA GTFS-Realtime URLs used
by `ingestion/gtfs_producer.py`. The MTA removed the API-key requirement around
2021, so all endpoints are reachable over plain HTTP GET with a descriptive
`User-Agent` header.

## Active feeds

| Feed | URL | Lines covered | Topic |
|---|---|---|---|
| ACE | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace` | A, C, E, S (Franklin) | `gtfs-vehicle`, `gtfs-trips`, `gtfs-alerts` |
| BDFM | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm` | B, D, F, M | same |
| G | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g` | G | same |
| JZ | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz` | J, Z | same |
| NQRW | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw` | N, Q, R, W | same |
| L | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l` | L | same |
| 1234567 (default) | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs` | 1, 2, 3, 4, 5, 6, 7, S 42 | same |
| SIR | `https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si` | Staten Island Railway | same |

Each feed is a single Protobuf payload that contains entities of three kinds:

- `vehicle` → routed to `gtfs-vehicle`
- `trip_update` → routed to `gtfs-trips`
- `alert` → routed to `gtfs-alerts`

The producer fetches all 8 feeds every 15 seconds, deserializes each one with
`google.transit.gtfs_realtime_pb2.FeedMessage`, and emits one Kafka record per
entity. Failures publish the raw bytes + error to `gtfs-dlq`.

## Verifying access

```bash
curl -sS -A "subway-dash/1.0 (contact@example.com)" \
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs" \
    -o /tmp/vp.pb
ls -l /tmp/vp.pb   # expect a non-empty Protobuf payload (a few hundred KB)
```

A 200 response with non-empty body is sufficient. The endpoints occasionally
return 5xx during MTA maintenance windows; the producer retries with
exponential backoff and routes terminal failures to the DLQ.

## Partition routing

The Kafka producer extracts `trip.route_id` from each entity and looks it up
in `ROUTE_PARTITION_MAP` (defined in `ingestion/config.py`). Unknown routes
fall back to `hash(route_id) % 12` so they distribute uniformly rather than
piling onto one overflow bucket.
