# Real-Time Distributed Market Data Streaming Platform

A streaming pipeline that ingests synthetic market-data events, moves them through
Kafka with correct partitioning and ordering guarantees, and (in later stages)
aggregates them into windowed price statistics served live to a browser dashboard.

The project is built in deliberate stages, each one proven before the next begins.
See `CHANGELOG.md` for the stage-by-stage history and the reasoning behind each step.

## What it does today

A single market-data event flows end-to-end: a producer generates events, publishes
them to a Kafka topic partitioned by symbol, and a consumer reads them back and prints
them. Partitioning and per-symbol ordering are verified from the consumer's output.

- **Producer** (`producer/producer.py`) — generates events (`symbol`, `price`,
  `size`, `event_time`, `seq`), serializes them to JSON, and publishes to the
  `market-events` topic keyed by symbol so each symbol lands on a consistent partition.
- **Consumer** (`consumer/consumer.py`) — reads `market-events` as a consumer group,
  deserializes each event, and prints partition, symbol, seq, and offset so that
  partitioning and ordering can be confirmed by eye.
- **Infrastructure** (`docker-compose.yml`) — a single Kafka broker in KRaft mode plus
  a Kafka UI, orchestrated with a `Makefile` for common commands.

## Architecture

```
  producer (host)  ──▶  Kafka broker (Docker)  ──▶  consumer (host)
                            │  market-events
                            │  3 partitions, keyed by symbol
                            ▼
                        Kafka UI (Docker, localhost:8080)
```

The broker exposes two listeners: an internal one (`kafka:9092`) for processes inside
the Docker network, such as the Kafka UI, and an external one (`localhost:29092`) for
processes on the host, such as the producer and consumer.

## Tech stack

- **Kafka** (Apache Kafka 3.8.0, KRaft mode — no ZooKeeper)
- **Python** (`kafka-python`) for producer and consumer
- **Docker Compose** for orchestration
- **Kafka UI** (Provectus) for topic and partition inspection

## Getting started

Prerequisites: Docker Desktop and Python 3.12+.

```bash
# 1. Start the broker and UI
make up

# 2. Activate the virtual environment
source .venv/bin/activate

# 3. Run the producer to publish events
python producer/producer.py

# 4. In another terminal (venv active), run the consumer to read them
python consumer/consumer.py
```

The Kafka UI is available at http://localhost:8080 for inspecting topics, partitions,
and messages.

To stop everything:

```bash
make down
```

## Notes

- The `market-events` topic has 3 partitions and persists across broker restarts;
  it does not need to be recreated between sessions.
- The consumer uses `auto_offset_reset="earliest"`, which only takes effect the first
  time a given consumer group reads. To replay all events from the start, use a group
  id that has not been used before.

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **Stage 2** — multiple consumers in one group; partition rebalancing and delivery semantics
- **Stage 3** — windowed OHLC + VWAP aggregation
- **Stage 4** — WebSocket fan-out to a live React dashboard
- **Rigor tiers** — failure/recovery testing, observability, load testing