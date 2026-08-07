# Changelog

Stage-by-stage history. Each entry records what landed and why. Full reasoning for the
larger design decisions lives in [`docs/`](docs/).

---

## Stage 5 — WebSocket fan-out and the browser dashboard

A FastAPI service consumes `bars` and streams candles to browsers over a WebSocket at
`/ws/{symbol}`, one connection per chart. Each client receives the last sixty bars for its
symbol on connect, then a live feed, with no gap and no duplicate at the join.

This is the first process in the pipeline that must wait on several things at once — the
broker socket and every open browser socket — so it runs on an asyncio event loop with
`aiokafka` rather than the blocking `kafka-python` client used upstream. The rejected
alternative was a background thread bridging a blocking consumer into the server through a
queue; that would put `bars` and `connections` under two threads and require locking every
append and every iteration. The absence of any lock in `webserver.py` is that decision made
visible, not an oversight.

Recent history is served from an in-process `deque(maxlen=60)` per symbol rather than by
reading back from the log. `bars` has one partition carrying every symbol interleaved, so
offset arithmetic yields bulk-recent rather than per-symbol-recent; the deque gives the
latter by construction.

Backfill and the live feed are reconciled per connection. A client registers itself before
its history is read, so a bar sealing mid-backfill is buffered against that connection rather
than lost or sent out of order; the buffer is drained immediately after the history and the
connection is then flipped to live. The flag and buffer are per connection because ten
browsers can be at different phases simultaneously — a single shared flag lets one client
finishing its backfill splice a future bar into another client's history, which is the same
failure shape as the global watermark of Stage 3.5.

Each server instance joins Kafka under a unique `group_id` derived from its PID. A consumer
group *divides* partitions among its members; fan-out needs every instance to see every
symbol, so each instance is a group of one. `auto_offset_reset="latest"` follows from that:
with no committed offset ever, the setting fires on every start, and replaying retained
history would do work proportional to retention to produce a result bounded at sixty bars.

Verified end to end against a terminal WebSocket client: backfill transitions to live at
consecutive `window_start` values with no gap and no overlap. Two apparent anomalies in the
output were traced and are correct behaviour — a gap across a producer restart (the deque has
no notion of missing time) and an occasional missing one-second window (no events landed in
that bucket, so no candle exists to emit).

The dashboard itself is a React and TypeScript application built with Vite, rendering three
symbols as three independent WebSocket connections and three candlestick charts. Charting is
Lightweight Charts, which creates and owns its own canvas and cannot be rendered into by React.

That single constraint decides the shape of the client. The chart and its series are created
once in an effect with an empty dependency array, held in refs, and disposed in that effect's
cleanup; no bar data ever enters `useState` or `useReducer`. Only connection status, retry
attempt, and a flat summary snapshot rebuilt at most once per bar cross into React state. The
rejected alternative — retaining the bar array in state, which is what every React charting
example does — re-renders the panel on every message, and keying the chart effect on that array
destroys and rebuilds the chart once a second, discarding whatever zoom or pan the viewer had
applied. Socket handling lives in a module that does not import React at all, so the rule is
structural rather than a comment someone has to remember.

Candles are positioned by `window_start` and never by arrival order, because `live` means
"arrived after your backfill" rather than "just happened". Backfill is sorted before it reaches
`setData`. A live bar for a window older than the one drawn is declined and counted rather than
allowed to throw: reinserting it would mean rebuilding the series and resetting the viewport to
correct a candle that has already scrolled into history, so the cost would land on the viewer
rather than on the CPU. A re-emission of the drawn window is declined unless its tick `count` is
strictly higher — the aggregator suppresses its wall-clock backstop while draining a backlog, so
a replayed window can absorb a tick the live pass sealed past and be the more complete copy. The
three outcomes are tallied separately and surfaced in the panel footer, because an ignored
duplicate and a dropped late bar indicate different upstream conditions.

The WebSocket URL is derived from `window.location`, so the page and its socket are always one
origin and no backend host appears anywhere in application code. `npm run build` emits to
`backend/static`, which uvicorn serves, and Vite proxies `/ws` with `ws: true` in development so
that one expression is correct in both environments. `webserver.py` had imported `StaticFiles`
without ever mounting it, so a build was invisible and `GET /` returned 404; the mount is now
registered last, since a catch-all at `/` would otherwise shadow `/ws/{symbol}`, and guarded by
an `isdir` check so the server still starts before the first build exists.

Verified against the running pipeline rather than mock data. Canvas DOM node identity held
across a thirty-second run while the series grew, confirming the chart is never rebuilt;
`ResizeObserver` construct and disconnect counts matched StrictMode's deliberate double-mount
exactly, confirming the cleanup path runs; and bars carrying backdated and duplicated
`window_start` values, injected directly into the `bars` topic, exercised each declined path
with no exception thrown.

Design records: [`async_runtime.md`](async_runtime.md),
[`websocket_fanout.md`](websocket_fanout.md), [`frontend.md`](frontend.md)

---

## Stage 4 — publishing sealed candles

Sealed candles are published to a new `bars` topic rather than only printed, making
aggregation a step in the pipeline rather than its end. Accumulators additionally track a
tick count, carried on the message.

Bars are keyed by symbol so a symbol's candles stay ordered within one partition;
`window_start` travels in the payload. The topic has one partition. Sends are asynchronous
with an error callback, and the producer is flushed before the consumer is closed.

Design record: [`docs/output-pipeline.md`](docs/output-pipeline.md)

---

## Seal trigger — closing windows on a quiet partition

Sealing no longer rides on tick arrival. A partition that went quiet previously never swept,
so its last windows were fully accumulated but never emitted.

The consumer now polls with a timeout instead of blocking on message arrival, so the loop
cycles even when the feed is silent. A throttled backstop runs a few times a second; when a
partition is both wall-clock quiet and caught up to the end of its log, it advances that
partition's watermark to the end of its furthest open window and calls the existing sweep —
it does not seal directly. Keeping one seal path means a late tick for a backstop-closed
window still meets the existing drop check.

Verified by replaying a fixed backlog under a fresh group id: without the backstop, five
windows accumulate and are never emitted; with it, every drain-time seal comes from the
watermark path with zero drops, followed by a single burst of five.

Design record: [`docs/idle-partitions.md`](docs/idle-partitions.md)

---

## Stage 3.5 — per-partition watermarks

Aim of this stage was to fix the singular watermark used across all partitions. Kafka
guarantees ordering within a partition but not across them.

Observed: one consumer drained partition 2's backlog first, dragging the shared watermark to
~t+6.8. It then started partition 0 from the beginning, where every event was older than that
watermark. Those events failed the late-check and were dropped in a burst — around seven
seconds of AAPL/MSFT/INTC data, for which no accumulator was ever created and no candle ever
emitted. Drops stopped only when partition 0's event times climbed past t+6.8.

Each partition now carries its own watermark, and each accumulator seals against the
watermark of the partition it lives under. State is nested `windows[partition][(symbol,
window)]` with `watermarks[partition]` alongside it.

Design record: [`docs/watermarks.md`](docs/watermarks.md)

---

## Stage 3 — event-time windowing

- Consumers now place ticks in 1 second window buckets to keep track of the OHLC.
- An arbitrary watermark trails 200ms behind the newest `event_time` seen, sealing the window
  once it passes the window's end. The existence of the watermark is to cater for out-of-order
  or late-arriving events.
- At this stage a global watermark is used for the 3 partitions, which led to a series of
  drops on subsequent partitions as only one consumer was run. The consumer drained one
  partition's backlog first (partition 2, TSLA/NVDA), which pushed the shared watermark far
  forward, and then when it reached the other partitions their older-timestamped events landed
  past that advanced watermark and were dropped. This means each partition would need its own
  watermark. Initially the plan was to have a watermark per partition and, as we have 3 of the
  latter, one consumer per partition — however the issue would be that if one consumer dies,
  its partition is reassigned to a surviving consumer which then juggles 2 partitions and 2
  watermarks, bringing back the starvation.
- Bucket placement is as follows: a tick arrives, its intended window is calculated from the
  floor of the `event_time`; if `watermark >= window_end` we discard the tick (a future version
  may cater for the audit trail). If the `(symbol, window)` exists in the dict we adjust the
  OHLC (except the opening price). Otherwise this represents the need to create a new window.
- Prices now use a random walk (`random.gauss`), adding price realism over the flat
  `price = 100.0`.

---

## Stage 2 — consumer groups and rebalancing

- Added more ticker symbols to make full use of partitions.
- Producer bug: `close()` was inside the loop, which killed the feed after a single pass.
  `try/finally` now handles the close after a Ctrl-C. The same mechanism was implemented for
  consumers, where earlier there was no handling of Ctrl-C, meaning uncommitted offsets got
  replayed on restart.
- Ran 3 consumer terminals and experimented with killing and restarting them. What was
  noticed is that partition assignment is reshuffled completely — restarting a consumer does
  not mean the others keep their previous partitions (assignment is recomputed on every
  membership change).
- `enable_auto_commit` fires on a 5s interval and not per message, which is why the replay
  window on restart happened.

---

## Stage 1 — walking skeleton

- Built dummy producer and consumer to show the flow source → producer → broker → consumer.
- Producer connects to the broker on `localhost:29092`, generates events using `make_event()`,
  serializes to JSON and publishes to `market-events`.
- Consumer connects to the same broker, deserializes each event, and prints partition, offset,
  seq and symbol.
- Each symbol maps deterministically to one partition only (AAPL and MSFT both hashed to
  partition 0 — a real collision; TSLA to partition 2). Proved from consumer output of the 30
  events, 10 per symbol.
- Offset is per-partition, not per-symbol.

---

## Stage 0 — infrastructure skeleton

- `docker-compose.yml` defines one Kafka broker and one Kafka UI on a private Docker network,
  so the two containers can reach each other by name.
- Ports of that network are exposed to the host: 29092 for host processes to reach the broker,
  8080 for the browser to reach the Kafka UI.
- `Makefile` shortens the common container commands.
- Topic `market-events` created with 3 partitions, which persists across broker restarts.