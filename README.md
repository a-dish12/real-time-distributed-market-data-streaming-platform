# Real-Time Distributed Market Data Streaming Platform

<!-- TODO: 30s recording of `docker compose up` → three charts filling. Goes here, above everything. -->

A streaming pipeline that ingests synthetic market-data events, moves them through Kafka with
correct partitioning and ordering guarantees, aggregates them into per-second OHLC candles,
republishes those candles as a stream of their own, and fans them out live to browsers over
WebSockets.

Built in deliberate stages, each proven before the next begins.
[`docs/changelog.md`](docs/changelog.md) records what landed at each stage; the design records
in [`docs/`](docs/) carry the reasoning behind the decisions that needed it.

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

**Dashboard** (`dashboard/`) — a React and TypeScript client on Vite, drawing three symbols as
three independent WebSocket connections and three candlestick charts. The chart is an imperative
object held in refs rather than React state, so a zoom or pan survives the incoming stream.
`npm run build` emits into `backend/static`, which the web server serves, so the page and its
socket share an origin.

**Infrastructure** (`docker-compose.yml`, `Dockerfile`) — one Kafka broker in KRaft mode plus a
Kafka UI, and the three application processes as optional services behind a Compose profile, so
they can run either in containers or on the host against the same broker. One multi-stage image
serves all three. A `Makefile` covers the common commands.

## Design records

The interesting decisions, each with its rejected alternatives and accepted costs:

| | |
|---|---|
| [**Per-partition watermarks**](docs/watermarks.md) | Kafka orders within a partition, not across them. A single shared watermark lets a fast-draining partition advance past a slow one's data, which is then dropped on arrival. |
| [**Idle partitions**](docs/idle_partitions.md) | A watermark only advances when a tick arrives, so a partition that goes quiet never seals its last window. A wall-clock backstop closes it — without introducing a second seal path. |
| [**Output pipeline**](docs/output_pipeline.md) | Why sealed candles go through a topic rather than straight to a socket, why bars are keyed by symbol, and why `bars` has one partition. |
| [**Async runtime**](docs/async_runtime.md) | Why the web server runs on an event loop with `aiokafka` instead of bridging a blocking consumer through a thread and a queue, and what that buys elsewhere in the file. |
| [**WebSocket fan-out**](docs/websocket_fanout.md) | Per-connection state, the race between replaying history and joining a live feed, and what happens when a client disappears mid-send. |
| [**Shared-watermark experiment**](docs/watermark-experiment.md) | A controlled before/after measurement of the bug per-partition watermarks fixed: a single shared watermark dropped 3000 of 3000 events on the lagging partitions and lost 30 of 32 candles, silently. Includes the documented deviation from `bd12fed`. |
| [**Browser dashboard**](docs/frontend.md) | Where React stops and the charting library starts, why no bar data lives in React state, how a late or duplicated candle is handled, and why the page and its socket share an origin. |

## Getting started

The three processes run either on your machine or in containers, from the same code. Both
modes need the broker and the topics first:

```bash
make up              # broker and Kafka UI
make topics-create   # both topics, safe to re-run
```

### Everything in containers

Prerequisite: Docker Desktop.

```bash
make up-apps         # builds the image, starts producer, aggregator and web
```

That is the whole system. Open http://localhost:8000; `make logs` follows the three processes.

### Processes on the host

Prerequisites: Python 3.12+, Node 20+. Useful when you want a debugger, or fast edit-and-restart
on one process while the rest keeps running.

```bash
source .venv/bin/activate
pip install -r requirements.txt
cd dashboard && npm install && npm run build && cd ..   # UI into backend/static

python -m producer.producer          # terminal 1
python -m consumer.consumer          # terminal 2
uvicorn backend.webserver:app        # terminal 3
```

All three run **from the repository root**, and the `-m` form matters: it puts the repo root on
`sys.path`, which is what makes `from config import ...` resolve. `python producer/producer.py`
puts `producer/` there instead and fails with `ModuleNotFoundError: No module named 'config'`.

Then open http://localhost:8000. Charts fill at one candle per second from whatever history the
web server holds.

For frontend work, leave uvicorn running and start Vite alongside it with
`cd dashboard && npm run dev`, then use http://localhost:5173 — Vite proxies `/ws` through to
uvicorn.

Kafka UI is at http://localhost:8080. `make topics`, `make describe`, `make down` cover the rest.

### Configuration

`config.py` at the repository root is the only place the broker address and topic names are
named. Each value reads an environment variable and falls back to the host default:

| Variable | Default | Set to |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:29092` | `kafka:9092` inside a container |
| `MARKET_EVENTS_TOPIC` | `market-events` | |
| `BARS_TOPIC` | `bars` | |

The two broker addresses are the same broker through two listeners, declared in
`KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`: `localhost:29092` is published to the host,
`kafka:9092` is reachable on the Compose network. Inside a container `localhost` means that
container, so the default cannot work there — which is why the three app services set
`KAFKA_BOOTSTRAP` explicitly and nothing needs to be set for host mode.

[`docs/running.md`](docs/running.md) covers replaying a backlog, the offset and rebalance
behaviour worth knowing about, and the surprises that look like bugs and are not.

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
aggregator) and `aiokafka` + FastAPI (web server) · React and TypeScript on Vite with
Lightweight Charts (dashboard) · Docker Compose · Kafka UI (Provectus)

## Roadmap

- **VWAP** — volume-weighted average price alongside OHLC per window
- **Rigor tiers** — failure/recovery testing (commit-on-seal, rebalance state cleanup),
  observability, load testing