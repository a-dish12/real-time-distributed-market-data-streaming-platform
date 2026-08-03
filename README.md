# Real-Time Distributed Market Data Streaming Platform

A streaming pipeline that ingests synthetic market-data events, moves them through
Kafka with correct partitioning and ordering guarantees, aggregates them into windowed
price statistics (OHLC candles), and republishes those candles as a stream of their own.
Later stages serve them live to a browser dashboard.

The project is built in deliberate stages, each one proven before the next begins.
See `CHANGELOG.md` for the stage-by-stage history and the reasoning behind each step.

## What it does today

Market-data events flow end-to-end continuously: a producer generates a live feed,
publishes to a Kafka topic partitioned by symbol, and a consumer group reads them back
and aggregates each symbol's ticks into per-second OHLC candles. Sealed candles are
published to a second topic rather than only printed, so aggregation is a step in the
pipeline rather than the end of it. Partitioning, per-symbol ordering, consumer-group
rebalancing, and windowed aggregation are all verified from consumer output and from the
contents of the output topic.

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
- **Aggregator** (`consumer/consumer.py`) — reads `market-events` as a member of a
  consumer group and aggregates ticks into event-time windows. State is held per
  partition; for each (symbol, window) it tracks OHLC and a tick count, sealing and
  emitting a candle once that partition's watermark passes the window's end. Ticks
  arriving after their window has sealed are dropped. A timer-driven backstop closes
  trailing windows on partitions that have gone quiet. Sealed candles are published to
  the `bars` topic. Run multiple instances with the same group id to split partitions
  across them.
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
overwritten each tick, count incremented.

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

## Publishing sealed candles

A sealed candle is published to the `bars` topic rather than only printed. Routing output
through the broker rather than pushing it straight to a socket buys three things: fan-out
to any number of independent readers, none of which the aggregator needs to know about;
lifecycle decoupling, so a downstream service can restart without the aggregator noticing
and without losing the candles produced while it was down; and durability, since the topic
is a log that survives process restarts. The cost is a few milliseconds of extra latency,
which sits against a 200ms grace period that is deliberate and a 1s window cadence — noise
against the budget it belongs to.

Bars are keyed by **symbol**. Kafka orders messages only within a partition, so keying by
`(symbol, window_start)` — or not keying at all — would scatter one symbol's candles across
partitions and lose the ordering a chart depends on. Keying by symbol puts every bar for
that symbol on one partition in produce order; `window_start` travels in the payload, where
a consumer needs it to place the candle, rather than in the key, where it would only break
ordering. The guarantee is unbroken from tick to output: a symbol's ticks arrive on one
partition, that partition's watermark only moves forward, so its windows seal in order.

The message carries `symbol`, `window_start`, `window_end`, `open`, `high`, `low`, `close`,
and `count`. `window_end` is explicit rather than derived from a window width the reader is
assumed to know: the log holds candles produced under whatever configuration was live at the
time, so a message should be interpretable without knowledge of the producer's config.
`count` is carried because nothing else in the message determines it, and because it is the
natural place for volume-weighted statistics to attach later.

`bars` has **one partition**. Bar volume scales with symbol count rather than tick rate —
one message per symbol per window regardless of how many ticks fed it — so the throughput
case for more does not exist at this scale. Unlike `market-events`, the count is not frozen:
consumers of `bars` hold no partition-keyed state, so repartitioning would at worst reorder
a candle briefly rather than corrupt an aggregation.

The aggregator publishes from inside its own seal path rather than handing candles to a
separate publishing process, which would need a channel to receive them and so would add a
hop for the same result. Sends are asynchronous with an error callback attached: the consume
loop must not block on a network round-trip, and a lost bar is reconstructible by replaying
the source topic — but a *silent* failure is not acceptable, so failures are logged with the
`(symbol, window_start)` that identifies the bar. On shutdown the producer is flushed before
the consumer is closed, since closing the consumer commits offsets, and committing while
bars still sit in the send buffer would claim work was finished that never left the machine.

## Known limitations

A tick that arrives for a window the backstop has already closed is dropped, which is
correct, but the case has not been exercised: it requires a partition to go quiet, seal,
then receive a tick whose event-time falls back inside the sealed window. Every symbol here
ticks continuously, so the situation does not arise.

The partition count of `market-events` is fixed at 3. Because symbols are hashed to
partitions, adding a partition would rehash some symbols onto a different partition —
splitting a symbol's ticks across two partitions, breaking the per-symbol ordering the
aggregation relies on, and producing two partial candles for the same (symbol, window).
Repartitioning a keyed, stateful topic is an operational action (stop, drain, reprocess),
not something the consumer can absorb online.

State for a partition lost on rebalance is not cleaned up. The backstop correctly declines
to seal windows for a partition the consumer no longer owns, but the entry lingers in memory
until the process exits. This belongs with the failure/recovery work.

Offsets auto-commit on a timer, which knows nothing about window boundaries. A rebalance can
therefore leave a commit position in the middle of a window: the consumer inheriting the
partition starts part-way through and builds a candle from partial data, or starts far
enough back that it re-emits a window already sealed. Two bars for the same
`(symbol, window_start)` are possible, and one of them may be partial — so overwriting on
key is not unconditionally safe for a downstream reader. The fix is to commit on seal rather
than on a timer, at the lowest offset still feeding an open window on that partition, which
makes every commit position a place where replay reconstructs whole windows. Reaching this
case requires a deliberate mid-run rebalance; it does not arise in normal single-consumer
operation. Deferred to the failure/recovery work.

JSON was chosen for the message format so that messages are readable in the Kafka UI, which
is how output is verified today. A binary format with a schema registry would cut a bar from
roughly 200 bytes to around 50 and would reject malformed messages at write time; neither
matters at this volume.

## Architecture

```
producer (host)  ──▶  Kafka broker (Docker)  ──▶  aggregator group (host)  ──▶  bars
   │  market-events            consumer 1 ─┐        aggregates ticks into        │
   │  3 partitions,            consumer 2 ─┼─ per-symbol OHLC candles            │  1 partition,
   │  keyed by symbol          consumer 3 ─┘  (partitions split across group,    │  keyed by symbol
   ▼                                           each with its own watermark)      ▼
Kafka UI (Docker, localhost:8080)                                    (dashboard — next stage)
```

The broker exposes two listeners: an internal one (`kafka:9092`) for processes inside
the Docker network, such as the Kafka UI, and an external one (`localhost:29092`) for
processes on the host, such as the producer and consumers.

Consumers sharing a group id divide `market-events`' 3 partitions between them. Adding or
removing a consumer triggers a rebalance, reassigning partitions across the group. Because
each partition's watermark is scoped to that partition, a consumer that inherits a second
partition on rebalance keeps both correct independently — the fix does not depend on any
particular partition-to-consumer mapping.

## Tech stack

- **Kafka** (Apache Kafka 3.8.0, KRaft mode — no ZooKeeper)
- **Python** (`kafka-python`) for producer and aggregator
- **Docker Compose** for orchestration
- **Kafka UI** (Provectus) for topic and partition inspection

## Getting started

Prerequisites: Docker Desktop and Python 3.12+.

```bash
# 1. Start the broker and UI
make up

# 2. Create both topics (safe to re-run; no-ops if they exist)
make topics-create

# 3. Activate the virtual environment
source .venv/bin/activate

# 4. Run the producer to publish a continuous feed (Ctrl-C to stop)
python producer/producer.py

# 5. In another terminal (venv active), run an aggregator to read the feed,
#    emit OHLC candles as windows seal, and publish them to `bars`
python consumer/consumer.py

# 6. To see partitions split, run more aggregators in additional terminals —
#    same group id, so they share the load
python consumer/consumer.py
```

Useful Makefile targets: `make topics` lists topics, `make describe` shows partition counts
and replication factors, `make down` stops everything.

The Kafka UI is available at http://localhost:8080 for inspecting topics, partitions,
and messages — including the contents of `bars`, which is where sealed candles can be
verified directly. Kill an aggregator and watch the remaining ones rebalance to cover its
partitions.

## Notes

- Topics persist across broker restarts and do not need to be recreated between sessions.
  Kafka is a log, not a queue — reading events does not remove them, so the full backlog
  stays on disk until retention evicts it.
- Symbols are chosen so all three partitions of `market-events` carry live traffic
  (partition assignment is deterministic per symbol; extra symbols were added to cover the
  middle partition).
- The aggregator uses `auto_offset_reset="earliest"`, which only takes effect the first
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
- Ticks per window follow from the producer's sleep interval: window width divided by
  sleep. Short sleeps drift below nominal, since `sleep` guarantees a floor rather than an
  exact interval and scheduling overshoot is a large fraction of a 10ms target.
- Replaying a backlog is a different regime from a live feed. Records already on disk
  come back in large fetches, so arrival is limited by fetch speed rather than by the
  producer, and partitions drain unevenly — one can run for hundreds of records while
  another delivers nothing. The backstop's idle check accounts for this by requiring a
  partition to be caught up to its log end, not merely quiet.
- Piping consumer output to a file block-buffers stdout, which can make the process look
  stalled while it is running normally. Use `python -u` when redirecting.

## Roadmap

Upcoming stages (see `CHANGELOG.md` for detail as they land):

- **WebSocket fan-out** — a web service consuming `bars` and streaming candles to a live
  browser dashboard, with recent history replayed to each client on connect
- **VWAP** — volume-weighted average price alongside OHLC per window
- **Rigor tiers** — failure/recovery testing (commit-on-seal, rebalance state cleanup),
  observability, load testing