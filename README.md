# Real-Time Distributed Market Data Streaming Platform

A streaming pipeline that ingests synthetic market-data events, moves them through
Kafka with correct partitioning and ordering guarantees, and aggregates them into
windowed price statistics (OHLC candles). Later stages serve these live to a browser
dashboard.

The project is built in deliberate stages, each one proven before the next begins.
See `CHANGELOG.md` for the stage-by-stage history and the reasoning behind each step.

## What it does today

Market-data events flow end-to-end continuously: a producer generates a live feed,
publishes to a Kafka topic partitioned by symbol, and a consumer group reads them back
and aggregates each symbol's ticks into per-second OHLC candles. Partitioning,
per-symbol ordering, consumer-group rebalancing, and windowed aggregation are all
verified from consumer output.

- **Producer** (`producer/producer.py`) — generates a continuous feed of events
  (`symbol`, `price`, `size`, `event_time`, `seq`), serializes them to JSON, and
  publishes to the `market-events` topic keyed by symbol so each symbol lands on a
  consistent partition. Prices follow an independent per-symbol random walk, so each
  symbol traces its own price path and candles show real open/high/low/close spread.
  Runs until interrupted (Ctrl-C), closing cleanly on exit.
- **Consumer** (`consumer/consumer.py`) — reads `market-events` as a member of a
  consumer group and aggregates ticks into event-time windows. For each (symbol,
  window) it tracks OHLC, sealing and emitting a candle once a watermark passes the
  window's end. Ticks arriving after their window has sealed are dropped. Run multiple
  instances with the same group id to split partitions across them.
- **Infrastructure** (`docker-compose.yml`) — a single Kafka broker in KRaft mode plus
  a Kafka UI, orchestrated with a `Makefile` for common commands.

## How the aggregation works

The consumer buckets ticks by **event-time** (the moment a trade happened, carried on
each event) rather than by when it reads them — so candles reflect market reality, not
consumer speed, and are identical no matter how fast or slow a consumer runs.

A **watermark** trails a fixed grace period (currently 200ms) behind the newest
event-time seen. A window seals once the watermark passes that window's end; the grace
period gives slightly late or out-of-order ticks a chance to land in their window
before it closes. The 200ms is a placeholder — with producer and consumer co-located,
real lateness is near zero, so a production value would come from profiling observed
lateness.

Per tick: the window is the event-time floored to the window width; if the watermark
has already passed that window's end the tick is late and dropped; otherwise the
(symbol, window) accumulator is updated (or created on first sight), with open frozen
by the first tick, high/low tracked, and close overwritten each tick.

## Known limitation

The consumer currently uses a **single watermark across all partitions**. Because Kafka
guarantees ordering only *within* a partition, a consumer that drains one partition's
backlog faster drags the shared watermark ahead of slower partitions, whose
older-timestamped events then arrive past the watermark and are dropped. This is
visible on a fresh `earliest` run as a burst of drops on the slower partitions until
the backlog clears.

The robust fix (next stage) is **per-partition watermarks combined by minimum**: the
stream watermark advances only as fast as the slowest partition, so no partition
starves another — and it stays correct regardless of how partitions map to consumers,
including after a rebalance. Running one consumer per partition avoids the bug only
while every consumer stays alive; a single failure reassigns a partition to a survivor
that then juggles two, and the starvation returns.

## Architecture

```
producer (host)  ──▶  Kafka broker (Docker)  ──▶  consumer group (host)
   │  market-events            consumer 1 ─┐        aggregates ticks into
   │  3 partitions,            consumer 2 ─┼─ per-symbol OHLC candles
   │  keyed by symbol          consumer 3 ─┘  (partitions split across group)
   ▼
Kafka UI (Docker, localhost:8080)
```

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
#    and emit OHLC candles as windows seal
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
  id that has not been used before. Note that replaying a backlog is what makes the
  single-watermark limitation above most visible.
- Partition assignment is recomputed on every group membership change (default range
  assignor, not sticky): surviving consumers can lose partitions they were already
  reading, not just inherit an orphaned one.
- Offsets auto-commit on a ~5s interval, not per message, so a consumer killed
  mid-interval will replay its most recent events on restart (at-least-once delivery).

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **Per-partition watermarks** — fix the single-watermark limitation above
- **VWAP** — volume-weighted average price alongside OHLC per window
- **WebSocket fan-out** — publish sealed candles to a new topic and stream them to a
  live browser dashboard (candlestick + VWAP)
- **Rigor tiers** — failure/recovery testing, observability, load testing