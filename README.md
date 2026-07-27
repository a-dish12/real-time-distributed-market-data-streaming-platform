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

Each partition carries its own watermark, so a consumer holding several partitions at
different depths — including on a fresh replay of a full backlog — aggregates all of
them correctly without one partition starving another.

- **Producer** (`producer/producer.py`) — generates a continuous feed of events
  (`symbol`, `price`, `size`, `event_time`, `seq`), serializes them to JSON, and
  publishes to the `market-events` topic keyed by symbol so each symbol lands on a
  consistent partition. Prices follow an independent per-symbol random walk, so each
  symbol traces its own price path and candles show real open/high/low/close spread.
  Runs until interrupted (Ctrl-C), closing cleanly on exit.
- **Consumer** (`consumer/consumer.py`) — reads `market-events` as a member of a
  consumer group and aggregates ticks into event-time windows. State is held per
  partition; for each (symbol, window) it tracks OHLC, sealing and emitting a candle
  once that partition's watermark passes the window's end. Ticks arriving after their
  window has sealed are dropped. Run multiple instances with the same group id to split
  partitions across them.
- **Infrastructure** (`docker-compose.yml`) — a single Kafka broker in KRaft mode plus
  a Kafka UI, orchestrated with a `Makefile` for common commands.

## How the aggregation works

The consumer buckets ticks by **event-time** (the moment a trade happened, carried on
each event) rather than by when it reads them — so candles reflect market reality, not
consumer speed, and are identical no matter how fast or slow a consumer runs.

Each partition carries its **own watermark**, trailing a fixed grace period (currently
200ms) behind the newest event-time seen *on that partition*. The watermark is a running
maximum, so it only ever moves forward; an out-of-order or slightly late tick cannot drag
it backwards and un-seal a window that has already closed. A window seals once its
partition's watermark passes the window's end, and each accumulator is judged only against
the watermark of the partition it belongs to.

Per-partition rather than shared is the point: Kafka guarantees ordering only *within* a
partition, so a single watermark fed by all of them assumes they are all at the same point
in time — which cross-partition disorder makes false. Scoping each watermark to its own
partition makes that wrong comparison structurally impossible, and it stays correct however
partitions happen to map to consumers, including after a rebalance.

The 200ms grace is a placeholder — with producer and consumer co-located, real lateness is
near zero, so a production value would come from profiling observed lateness. Its only
source of out-of-order arrival is symbols sharing a partition (e.g. AAPL and MSFT both hash
to partition 0 and interleave); a single symbol's ticks are generated in order and land in
one partition in order.

State is nested `windows[partition][(symbol, window_start)]`, with `watermarks[partition]`
alongside it. Per tick: advance that partition's watermark; sweep that partition's windows,
sealing any the watermark has passed; then bucket the tick by its event-time floored to the
window width, dropping it if its window has already sealed, otherwise updating (or creating)
the (symbol, window) accumulator — open frozen by the first tick, high/low tracked, close
overwritten each tick.

## Known limitation

The consumer sweeps a partition's windows only when a tick arrives *on that partition*. A
partition that goes quiet is therefore never swept, so its last open window never seals —
the data is complete, it simply never gets emitted. In this project every symbol ticks
continuously, so it does not surface, but a real feed with illiquid symbols would need a
time-driven seal (sweep on a timer, not only on arrival) to close trailing windows. This is
the next thing to address.

The partition count is also fixed at 3. Because symbols are hashed to partitions, adding a
partition would rehash some symbols onto a different partition — splitting a symbol's ticks
across two partitions, breaking the per-symbol ordering the aggregation relies on, and
producing two partial candles for the same (symbol, window). Repartitioning a keyed,
stateful topic is an operational action (stop, drain, reprocess), not something the consumer
can absorb online.

## Architecture

```
producer (host)  ──▶  Kafka broker (Docker)  ──▶  consumer group (host)
   │  market-events            consumer 1 ─┐        aggregates ticks into
   │  3 partitions,            consumer 2 ─┼─ per-symbol OHLC candles
   │  keyed by symbol          consumer 3 ─┘  (partitions split across group,
   ▼                                           each with its own watermark)
Kafka UI (Docker, localhost:8080)
```

The broker exposes two listeners: an internal one (`kafka:9092`) for processes inside
the Docker network, such as the Kafka UI, and an external one (`localhost:29092`) for
processes on the host, such as the producer and consumers.

Consumers sharing a group id divide the topic's 3 partitions between them. Adding or
removing a consumer triggers a rebalance, reassigning partitions across the group. Because
each partition's watermark is scoped to that partition, a consumer that inherits a second
partition on rebalance keeps both correct independently — the fix does not depend on any
particular partition-to-consumer mapping.

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
  it does not need to be recreated between sessions. Kafka is a log, not a queue —
  reading events does not remove them, so the full backlog stays on disk until
  retention evicts it.
- Symbols are chosen so all three partitions carry live traffic (partition assignment
  is deterministic per symbol; extra symbols were added to cover the middle partition).
- The consumer uses `auto_offset_reset="earliest"`, which only takes effect the first
  time a given consumer group reads. To replay the whole backlog from the start, use a
  group id that has not been used before — its cursor starts at offset 0. Replaying a
  backlog under a fresh group is what makes the per-partition watermark behaviour
  observable, since it recreates the uneven-partition-depth conditions that the old
  single watermark handled incorrectly.
- Partition assignment is recomputed on every group membership change (default range
  assignor, not sticky): surviving consumers can lose partitions they were already
  reading, not just inherit an orphaned one.
- Offsets auto-commit on a ~5s interval, not per message, so a consumer killed
  mid-interval will replay its most recent events on restart (at-least-once delivery).

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **Time-driven seal** — sweep on a timer so trailing windows on quiet partitions close
- **VWAP** — volume-weighted average price alongside OHLC per window
- **WebSocket fan-out** — publish sealed candles to a new topic and stream them to a
  live browser dashboard (candlestick + VWAP)
- **Rigor tiers** — failure/recovery testing, observability, load testing