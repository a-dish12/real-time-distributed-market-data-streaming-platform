# Real-Time Distributed Market Data Streaming Platform

A streaming pipeline that ingests synthetic market-data events, moves them through
Kafka with correct partitioning and ordering guarantees, and (in later stages)
aggregates them into windowed price statistics served live to a browser dashboard.

The project is built in deliberate stages, each one proven before the next begins.
See `CHANGELOG.md` for the stage-by-stage history and the reasoning behind each step.

## What it does today

Market-data events flow end-to-end continuously: a producer generates a live feed,
publishes to a Kafka topic partitioned by symbol, and one or more consumers in a group
read them back. Partitioning, per-symbol ordering, and consumer-group rebalancing are
all verified from consumer output.

- **Producer** (`producer/producer.py`) — generates a continuous feed of events
  (`symbol`, `price`, `size`, `event_time`, `seq`), serializes them to JSON, and
  publishes to the `market-events` topic keyed by symbol so each symbol lands on a
  consistent partition. Runs until interrupted (Ctrl-C), closing cleanly on exit.
- **Consumer** (`consumer/consumer.py`) — reads `market-events` as a member of a
  consumer group, deserializes each event, and prints partition, symbol, seq, and
  offset so partitioning, ordering, and partition assignment can be confirmed by eye.
  Run multiple instances with the same group id to split partitions across them.
- **Infrastructure** (`docker-compose.yml`) — a single Kafka broker in KRaft mode plus
  a Kafka UI, orchestrated with a `Makefile` for common commands.

## Architecture

producer (host)  ──▶  Kafka broker (Docker)  ──▶  consumer group (host)
│  market-events            consumer 1 ─┐
│  3 partitions,            consumer 2 ─┼─ partitions
│  keyed by symbol          consumer 3 ─┘  split across
▼
Kafka UI (Docker, localhost:8080)




The broker exposes two listeners: an internal one (`kafka:9092`) for processes inside
the Docker network, such as the Kafka UI, and an external one (`localhost:29092`) for
processes on the host, such as the producer and consumers.

Consumers sharing a group id divide the topic's 3 partitions between them. Adding or
removing a consumer triggers a rebalance, reassigning partitions across the group.

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

# 3. Run the producer to publish a continuous feed (Ctrl-C to stop)
python producer/producer.py

# 4. In another terminal (venv active), run a consumer to read the feed
python consumer/consumer.py

# 5. To see partitions split, run more consumers in additional terminals —
#    same group id, so they share the load
python consumer/consumer.py
```

The Kafka UI is available at http://localhost:8080 for inspecting topics, partitions,
and messages. Kill a consumer and watch the remaining ones rebalance to cover its
partitions.

To stop everything:

```bash
make down
```

## Notes

- The `market-events` topic has 3 partitions and persists across broker restarts;
  it does not need to be recreated between sessions.
- Symbols are chosen so all three partitions carry live traffic (partition assignment
  is deterministic per symbol; extra symbols were added to cover the middle partition).
- The consumer uses `auto_offset_reset="earliest"`, which only takes effect the first
  time a given consumer group reads. To replay all events from the start, use a group
  id that has not been used before.
- Partition assignment is recomputed on every group membership change (default range
  assignor, not sticky): surviving consumers can lose partitions they were already
  reading, not just inherit an orphaned one.
- Offsets auto-commit on a ~5s interval, not per message, so a consumer killed
  mid-interval will replay its most recent events on restart (at-least-once delivery).

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **Stage 3** — windowed OHLC + VWAP aggregation
- **Stage 4** — WebSocket fan-out to a live React dashboard
- **Rigor tiers** — failure/recovery testing, observability, load testing