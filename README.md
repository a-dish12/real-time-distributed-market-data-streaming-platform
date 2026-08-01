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
them correctly without one partition starving another. Windows on a partition that
falls silent are closed by a wall-clock backstop rather than hanging open indefinitely.

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
  window has sealed are dropped. A timer-driven backstop closes trailing windows on
  partitions that have gone quiet. Run multiple instances with the same group id to
  split partitions across them.
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

### Closing windows on a quiet partition

The watermark only advances when a tick arrives, so a partition with no incoming traffic
would never seal its last window — the data is complete, it simply never gets emitted. A
wall-clock backstop covers that gap. The consumer polls with a timeout rather than blocking
on message arrival, so the loop keeps cycling when the feed is silent, and a throttled check
runs a few times a second regardless of traffic.

When a partition qualifies as idle, the backstop does **not** seal windows directly. It
advances that partition's watermark to the end of its furthest-along open window and calls
the same sweep the normal path uses. Keeping one seal path rather than two means a late tick
for a backstop-closed window still meets the existing late-check and is dropped rather than
resurrecting the window as a new accumulator.

The new watermark value is derived from window ends already held in state, so it stays pure
event-time; wall-clock only decides *when* to intervene. That separation matters because the
two clocks are not comparable — replaying a stored backlog puts event-times days behind the
consumer's clock, and any rule comparing wall-clock directly against a window's end would
seal everything on the first tick of the timer.

Two independent thresholds govern this. The **grace period** (200ms) is how long a window
waits for stragglers. The **idle threshold** (800ms) is how long a partition must be silent
before the backstop steps in — sized to genuine idleness rather than lateness, and
deliberately larger than the grace period, which is what makes it safe to seal every open
window on an idle partition at once. Both are calibrated to this simulator's tick rate; a
real feed would size the idle threshold to the liquidity of the symbols involved.

A partition qualifies as idle only if it is **both** silent in wall-clock terms **and**
caught up to the end of its log (`position >= end_offset`). Silence alone is not enough: a
single consumer draining three partitions leaves the ones it is not currently fetching
looking silent while their records still sit unread. Sealing on that signal alone would
advance the watermark past data that is still coming, and those records would be dropped on
arrival — one partition's drain speed corrupting another partition's output.

## Known limitation

A tick that arrives for a window the backstop has already closed is dropped, which is
correct, but the case has not been exercised: it requires a partition to go quiet, seal,
then receive a tick whose event-time falls back inside the sealed window. Every symbol here
ticks continuously, so the situation does not arise.

The partition count is fixed at 3. Because symbols are hashed to partitions, adding a
partition would rehash some symbols onto a different partition — splitting a symbol's ticks
across two partitions, breaking the per-symbol ordering the aggregation relies on, and
producing two partial candles for the same (symbol, window). Repartitioning a keyed,
stateful topic is an operational action (stop, drain, reprocess), not something the consumer
can absorb online.

State for a partition lost on rebalance is not cleaned up. The backstop correctly declines
to seal windows for a partition the consumer no longer owns, but the entry lingers in memory
until the process exits. This belongs with the failure/recovery work.

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
- Replaying a backlog is a different regime from a live feed. Records already on disk
  come back in large fetches, so arrival is limited by fetch speed rather than by the
  producer, and partitions drain unevenly — one can run for hundreds of records while
  another delivers nothing. The backstop's idle check accounts for this by requiring a
  partition to be caught up to its log end, not merely quiet.
- Piping consumer output to a file block-buffers stdout, which can make the process look
  stalled while it is running normally. Use `python -u` when redirecting.

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **VWAP** — volume-weighted average price alongside OHLC per window
- **WebSocket fan-out** — publish sealed candles to a new topic and stream them to a
  live browser dashboard (candlestick + VWAP)
- **Rigor tiers** — failure/recovery testing, observability, load testing