# Real-Time Distributed Market Data Streaming Platform

A streaming pipeline that ingests synthetic market-data events, moves them through Kafka with
correct partitioning and ordering guarantees, aggregates them into per-second OHLC candles,
republishes those candles as a stream of their own, and fans them out live to browsers over
WebSockets.

Built in deliberate stages, each proven before the next begins. [`CHANGELOG.md`](CHANGELOG.md)
records what landed at each stage; the design records in [`docs/`](docs/) carry the full
reasoning behind the decisions that needed it.

```
producer ──▶ market-events ──▶ aggregator ──▶ bars ──▶ web server ──▶ browser
             3 partitions,     OHLC windows   1 partition,  WebSocket fan-out
             keyed by symbol   per symbol     keyed by symbol
```

## Components

**Producer** (`producer/producer.py`) — generates a continuous feed of events (`symbol`,
`price`, `size`, `event_time`, `seq`) and publishes them to `market-events` keyed by symbol, so
each symbol lands consistently on one partition. Prices follow an independent per-symbol random
walk, so candles show real open/high/low/close spread.

**Aggregator** (`consumer/consumer.py`) — reads `market-events` as a member of a consumer group
and buckets ticks into one-second event-time windows, tracking OHLC and a tick count per
(symbol, window). Windows seal against a watermark and are published to `bars`. Run several
instances with the same group id to split partitions across them.

**Web server** (`backend/webserver.py`) — consumes `bars` and streams candles to browsers at
`/ws/{symbol}`, one connection per chart. Each client receives the last sixty bars on connect
and then a live feed, with no gap at the seam.

**Infrastructure** (`docker-compose.yml`) — one Kafka broker in KRaft mode plus a Kafka UI,
with a `Makefile` for the common commands.

## Design records

The interesting decisions, each with its rejected alternatives and accepted costs:

| | |
|---|---|
| [**Per-partition watermarks**](docs/watermarks.md) | Kafka orders within a partition, not across them. A single shared watermark lets a fast-draining partition advance past a slow one's data, which is then dropped on arrival. |
| [**Idle partitions**](docs/idle-partitions.md) | A watermark only advances when a tick arrives, so a partition that goes quiet never seals its last window. A wall-clock backstop closes it — without introducing a second seal path. |
| [**Output pipeline**](docs/output-pipeline.md) | Why sealed candles go through a topic rather than straight to a socket, why bars are keyed by symbol, and why `bars` has one partition. |
| [**Async runtime**](docs/async-runtime.md) | Why the web server runs on an event loop with `aiokafka` instead of bridging a blocking consumer through a thread and a queue, and what that buys elsewhere in the file. |
| [**WebSocket fan-out**](docs/websocket-fanout.md) | Per-connection state, the race between replaying history and joining a live feed, and what happens when a client disappears mid-send. |

## Getting started

Prerequisites: Docker Desktop and Python 3.12+.

```bash
make up                              # start the broker and Kafka UI
make topics-create                   # create both topics (safe to re-run)
source .venv/bin/activate
pip install -r requirements.txt

python producer/producer.py          # terminal 1 — continuous feed
python consumer/consumer.py          # terminal 2 — aggregate into candles
cd backend && uvicorn webserver:app  # terminal 3 — serve them
```

Then connect a client. Without a dashboard yet, a terminal WebSocket client shows the stream
directly:

```bash
pip install websockets
python -m websockets ws://localhost:8000/ws/AAPL
```

Expect roughly one `backfill` message per second of uptime (capped at sixty), then a `live`
message each second as windows seal.

`make topics` lists topics, `make describe` shows partition counts, `make down` stops
everything. The Kafka UI at http://localhost:8080 shows topics, partitions, and message
contents — including `bars`, where sealed candles can be verified directly. Kill an aggregator
and watch the rest rebalance.

## Things worth knowing when running it

- Topics persist across broker restarts. Kafka is a log, not a queue: reading does not remove,
  so the full backlog stays on disk until retention evicts it.
- The aggregator uses `auto_offset_reset="earliest"`, which only takes effect the first time a
  given group id reads. **Use a fresh group id to replay the whole backlog** — that is what
  makes the per-partition watermark behaviour observable, since it recreates the uneven
  partition depths the old shared watermark handled incorrectly.
- Replaying a backlog is a different regime from a live feed: records come back in large
  fetches, so partitions drain unevenly and one can run for hundreds of records while another
  delivers nothing.
- Offsets auto-commit on a ~5s timer, not per message, so a consumer killed mid-interval
  replays its most recent events on restart (at-least-once).
- Partition assignment is recomputed on every membership change (default range assignor, not
  sticky), so surviving consumers can lose partitions they were already reading.
- `uvicorn --reload` restarts the worker on every save: new event loop, new consumer group,
  **empty deques**. Backfill will look broken immediately after an edit. This is expected.
- Piping consumer output to a file block-buffers stdout and can make a healthy process look
  stalled. Use `python -u` when redirecting.

## Known limitations

**Windowing.** The partition count of `market-events` is fixed at 3: symbols are hashed to
partitions, so adding one would split a symbol's ticks across two partitions and produce two
partial candles for the same (symbol, window). Repartitioning a keyed, stateful topic is an
operational action, not something a consumer can absorb online. State for a partition lost on
rebalance is also not cleaned up — the backstop correctly declines to seal it, but the entry
lingers until the process exits.

**Delivery.** Offsets auto-commit on a timer that knows nothing about window boundaries, so a
rebalance can leave a commit position mid-window: the inheriting consumer either builds a candle
from partial data or re-emits one already sealed. Two bars for the same (symbol, window_start)
are therefore possible and one may be partial, so overwriting on key is not unconditionally safe
downstream. The fix is to commit on seal, at the lowest offset still feeding an open window.
Deferred to the failure/recovery work.

**Fan-out.** Bar history lives in the web server's memory, so it is lost on restart and has no
notion of missing time — if the producer stops for an hour, a client connecting afterwards
receives sixty bars that look contiguous but straddle the gap.

**Format.** JSON was chosen so messages are readable in the Kafka UI, which is how output is
verified today. A binary format with a schema registry would cut a bar from ~200 bytes to ~50
and reject malformed messages at write time; neither matters at this volume.

## Tech stack

Apache Kafka 3.8.0 (KRaft mode, no ZooKeeper) · Python with `kafka-python` (producer,
aggregator) and `aiokafka` + FastAPI (web server) · Docker Compose · Kafka UI (Provectus)

## Roadmap

- **Browser dashboard** — charts fed by the WebSocket stream, replaying history on connect
- **VWAP** — volume-weighted average price alongside OHLC per window
- **Rigor tiers** — failure/recovery testing (commit-on-seal, rebalance state cleanup),
  observability, load testing