# Kafka Configuration

**Task A9.3** — Source-of-truth reference for the Kafka topology used by
Subway Dash. Topic shapes here are the **final** values committed in Week 1
and are not changed mid-project (recreating a topic invalidates Spark
checkpoints).

## Cluster

| Setting | Value |
|---|---|
| Brokers | 1 (single-node dev cluster) |
| Replication factor | 1 (acceptable limitation for student project) |
| Bootstrap (host) | `localhost:9092` |
| Bootstrap (in-cluster) | `kafka:29092` |
| Auto-create topics | disabled |
| Retention | 24h (default) |

## Topics

| Topic | Partitions | Retention / Cleanup | Producer | Consumer(s) |
|---|---|---|---|---|
| `gtfs-vehicle` | 12 | 24h delete | `ingestion.gtfs_producer` | `processing.gtfs_vehicle_consumer` |
| `gtfs-trips` | 12 | 24h delete | `ingestion.gtfs_producer` | `processing.gtfs_trips_consumer` |
| `gtfs-alerts` | 2 | 24h delete | `ingestion.gtfs_producer` | (Phase 3, dashboard alerts feed) |
| `gtfs-dlq` | 2 | 24h delete | `ingestion.gtfs_producer`, `ingestion.weather_poller` | `ingestion.gtfs_monitor` (operator review) |
| `weather-feed` | 1 | **compact** | `ingestion.weather_poller` | replay/debug only |

## Partition routing for `gtfs-vehicle` / `gtfs-trips`

The producer extracts `route_id` from each entity and looks it up in
`ROUTE_PARTITION_MAP` (defined in [`ingestion/config.py`](../ingestion/config.py)).
The map intentionally biases traffic by ridership volume so high-volume groups
get their own partition rather than sharing one.

| Partition | Routes |
|---|---|
| 0 | A, C, E |
| 1 | B, D, F, M |
| 2 | G |
| 3 | J, Z |
| 4 | L |
| 5 | N, Q, R, W |
| 6 | 1, 2, 3 |
| 7 | 4, 5, 6 |
| 8 | 7 |
| 9 | S, FS, GS, H (shuttles) |
| 10 | SIR, SI |
| 11 | (overflow / null route_id) |

Unknown routes fall back to `hash(route_id) % 12` (not the overflow bucket) so
they distribute uniformly. The audit in Task A6.1 confirms
`max_partition / median_partition ≤ 3` under rush-hour load. If a single
partition exceeds 40% of total traffic, split the dominant group rather than
recreating the topic.

The `gtfs-alerts` topic uses 2 partitions; the producer applies
`route_partition % 2` to keep the same routing logic at lower fan-out.

## Consumer groups

| Group ID / prefix | Topic(s) | Owner | Visible in `--list`? |
|---|---|---|---|
| `gtfs-monitor` | all GTFS topics + DLQ | Track A — `gtfs_monitor.py` | yes |
| `spark-vehicle-consumer-<uuid>` | `gtfs-vehicle` | Track A — `gtfs_vehicle_consumer.py` | **no** (see below) |
| `spark-trips-consumer-<uuid>` | `gtfs-trips` | Track A — `gtfs_trips_consumer.py` | **no** (see below) |

**Important — Spark Structured Streaming's Kafka source does not join a Kafka
consumer group.** It uses the partition-assignment API directly and manages
offsets through Spark checkpoints. The `groupIdPrefix` option only sets a
label that appears in broker logs and JMX metrics; the prefix will **not**
register a group visible to `kafka-consumer-groups --list` or
`--describe --group spark-vehicle-consumer`.

To check Spark consumer progress, use the Spark UI Streaming tab
(`http://localhost:8080` → application → Streaming Query) or read the
checkpoint directory's `offsets/` files. The `gtfs-monitor` group, which
uses the regular `KafkaConsumer` API, IS visible in `--list`.

The plain Python `gtfs_monitor` consumer always pairs the same checkpoint
directory with the same job across restarts so it resumes cleanly.

## Producer settings

`ingestion/gtfs_producer.py`:

| Setting | Value | Reason |
|---|---|---|
| `acks` | `all` | Single broker, but no reason to weaken durability |
| `linger_ms` | 50 | Small batching to amortize TCP/serialization |
| `retries` | 5 | Handles transient broker hiccups |
| `value_serializer` | JSON UTF-8 | Spark `from_json` consumes it directly |
| `key_serializer` | UTF-8 string | Key is `route_id` for routing visibility |

## Consumer settings (Spark Structured Streaming)

| Setting | Default | Tuning notes |
|---|---|---|
| `startingOffsets` | `latest` | Use `earliest` only for replay/backfill |
| `maxOffsetsPerTrigger` | 5,000 | Backpressure during burst; tuned in Task A8.1 |
| `trigger(processingTime=...)` | 30s | Matches dashboard refresh cadence |
| `failOnDataLoss` | true | Catches misconfigured offset resets |

`maxOffsetsPerTrigger = 5000` is the starting estimate from Assumption A-11.
Task A8.1 measures actual records-per-second during a 4-hour rush-hour window
and adjusts up if LAG grows monotonically.

## DLQ semantics

`gtfs-dlq` receives:

1. **Fetch failures** — empty `raw_b64`, error tag `fetch_failed`
2. **Parse failures** — base64 of the raw Protobuf payload + parse error
3. **Send failures** — Kafka send threw on a downstream topic

Each DLQ message has the shape:
```json
{
  "feed": "ace",
  "error": "parse_error: ...",
  "received_at": 1740250200,
  "raw_b64": "..."
}
```

The Week 3 DLQ review (Task A3.2) categorizes recurring errors and either
patches the producer (for fixable parse errors) or marks them as documented
non-blocking edge cases.

## Operations

Create topics (idempotent — safe to re-run):
```bash
./infra/create_topics.sh                  # uses localhost:9092 by default
./infra/create_topics.sh broker:29092     # override bootstrap
```

Inspect a topic:
```bash
docker exec -it subway_kafka kafka-topics \
    --bootstrap-server localhost:9092 --describe --topic gtfs-vehicle
```

Check consumer lag:
```bash
docker exec -it subway_kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 --describe --group spark-vehicle-consumer
```

Tail a topic (debug):
```bash
docker exec -it subway_kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic gtfs-dlq --from-beginning --max-messages 10
```
