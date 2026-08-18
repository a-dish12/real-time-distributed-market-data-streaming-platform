# Build and deployment

How the TypeScript dashboard becomes files uvicorn serves, how those files get into an image,
and how the four containers relate at startup. The WebSocket protocol they carry is in
[`websocket_fanout.md`](websocket_fanout.md); this document is about the packaging around it.

Several decisions here look like mistakes at the point of edit. Each one is called out with the
assumption a reader would plausibly make instead, and why acting on it breaks something.

## Shape

```
  dashboard/src/*.tsx
        │  tsc -b && vite build          (Dockerfile stage 1, node:22-slim)
        ▼
  backend/static/                        ← build output, gitignored, never in the build context
        │  COPY --from=dashboard
        ▼
  ┌─────────────────────────────────────────────────┐
  │ one image (python:3.12-slim)                    │
  │   config.py  producer/  consumer/  backend/     │
  │   backend/static/                               │
  └─────────────────────────────────────────────────┘
        │            │            │
     producer    aggregator      web        ← three services, three commands, one image
                                  │
                                  └── uvicorn serves  /ws/{symbol}  +  /health  +  /  (static)
```

## Single origin

The dashboard and the WebSocket are served by the same process on the same port. `webserver.py`
mounts `backend/static` at `/`, and the browser opens `/ws/{symbol}` against
`window.location.host`, so the page never names its backend.

This is the constraint the rest of the packaging bends around. It buys the absence of CORS
middleware, of an environment variable naming the socket, and of any build-time knowledge of
where the backend lives. `vite.config.ts` is the only file permitted to name the backend's host,
and only for development, where Vite's dev server is necessarily on a different port.

## From TypeScript to backend/static

`npm run build` is `tsc -b && vite build`, so a type error fails the build rather than shipping
a broken bundle. `vite.config.ts` sets `build.outDir` to `'../backend/static'`.

That path is **relative to the Vite project root**, which is the directory Vite runs in. On a
laptop, running it from `dashboard/` puts the output in the repo's `backend/static`. In the
image, the same relative path has to resolve the same way, which is why the Dockerfile's build
stage sets `WORKDIR /app/dashboard` — mirroring the repo layout — so the output lands at
`/app/backend/static`.

> **You would assume** the build stage's working directory is cosmetic, and that `/app` or
> `/build` would do as well. It is not: `outDir` is relative, so changing that directory moves
> the bundle to a path the runtime stage does not copy from. The build still succeeds and the
> image still starts; the UI is simply missing, and `/health` reports `static_mounted: false`.

`emptyOutDir: true` is set explicitly because the output directory is outside the project root,
and Vite refuses to clear such a directory unless told to.

`webserver.py` resolves the same directory from `__file__` rather than from the working
directory, so it does not matter where uvicorn is launched from. The mount is guarded by an
`isdir` check, which keeps the server startable before the first build has ever run.

## Why the static mount is registered last

Starlette matches routes in registration order, and a mount at `/` matches everything. The
WebSocket route and `/health` are registered above it in `webserver.py`; moving the mount up
shadows both, and `/ws/AAPL` starts returning the index page instead of upgrading.

## One image, three services

The producer, the aggregator and the web server are one Python codebase started three ways. The
image is built once and all three services reference it with a different `command:`, rather than
three images copying identical layers.

The image declares **no `CMD`**. There is no single main process for it to name, and a default
that all three services override would be a claim about the image that is not true.

The runtime stage sets `WORKDIR /app` so that `python -m producer.producer` puts `/app` on the
import path and `from config import KAFKA_BOOTSTRAP` resolves to `/app/config.py` — the same
reason those commands must be run from the repo root on a laptop.

### backend/static is excluded from the build context

`.dockerignore` excludes `backend/static/`, a directory the image genuinely needs.

> **You would assume** this is a mistake — the image serves that directory, so surely it must be
> in the context. Removing the line appears to work, and that is the problem.

`backend/static` is build output. On a laptop it is whatever `npm run build` last produced:
possibly stale, possibly from an abandoned branch, possibly absent on a fresh clone since it is
gitignored. Leaving it in the context lets `COPY backend/ backend/` sweep that into the image
before the build stage's output arrives, so the shipped UI would depend on when someone last ran
a build by hand. Excluding it leaves `COPY --from=dashboard` as the only path in, which makes
the image a function of the repository alone.

This is also why that `COPY --from` sits **after** `COPY backend/` in the Dockerfile. The order
is what guarantees the built bundle is the last thing written to that path.

## The four services at startup

```
  kafka ──(healthy)──▶ kafka-init ──(exited 0)──▶ producer
                                                  aggregator
                                                  web
```

`kafka` is the broker, running KRaft with no separate ZooKeeper. Two listeners, and which one to
use depends on where the caller is:

| caller | address | listener |
|---|---|---|
| containers on the Compose network | `kafka:9092` | `PLAINTEXT` |
| processes on the host | `localhost:29092` | `HOST` |

Only 29092 is published to the host. `config.py` defaults to `localhost:29092` and the compose
services override `KAFKA_BOOTSTRAP` to `kafka:9092`, because inside a container `localhost` is
that container rather than the machine.

`kafka-init` creates `market-events` with three partitions and `bars` with one, then exits. It
reuses the broker image only because that image carries `kafka-topics.sh`; it is not a second
broker. `--if-not-exists` makes it a no-op after the first run, which is what allows it to run
unconditionally on every `up`.

The app services wait on two different gates. `service_healthy` on the broker means it answers
API requests, not merely that its container exists. `service_completed_successfully` on
`kafka-init` means the topics exist. Both matter most to the aggregator, which builds its
`KafkaProducer` at module level and therefore connects during import, before `main()` runs.

### Automatic topic creation is disabled

`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"` and `kafka-init` are a single decision.

With automatic creation left on, a producer that starts before anything creates the topic has
`market-events` created for it at the broker default of one partition. Nothing errors. The
pipeline runs, bars appear, and the three-partition topic the aggregator is designed around has
silently become a one-partition topic — so the per-partition watermarks and the idle-partition
backstop of [`idle_partitions.md`](idle_partitions.md) have nothing to exercise. Disabling it
converts that silence into a visible failure, and `kafka-init` is what ensures the failure does
not occur in the first place.

Only *implicit* creation is disabled. Explicit `kafka-topics.sh --create` still works, so both
`kafka-init` and the Makefile topic targets are unaffected.

### kafka-init's command is a YAML list

```yaml
    entrypoint: ["/bin/sh", "-c"]
    command:
      - |
        set -e
        ...
```

> **You would assume** the list wrapper is noise and a plain block string would read better.
> It does read better, and it silently does nothing.

Compose word-splits a bare string `command:`. With `sh -c` as the entrypoint, the script arrives
as separate argv entries and the shell runs only the first word — `set` — which prints the
shell's variables, creates no topics, and **exits 0**. The `service_completed_successfully` gate
is satisfied by that exit code, so the app services start against a broker with no topics and
the producer dies on a metadata timeout a minute later. One list item is one argument, which
keeps the script intact.

The entrypoint is overridden because the image ships an entrypoint and command that mean "be a
broker"; replacing both is what turns it into a one-shot admin container.

### restart: "no" on kafka-init, unless-stopped on the apps

The three app services restart on their own because the startup gates run at `up` time and do
nothing about a crash an hour later. Without a policy the default is to stay dead, and a dead
aggregator presents as a chart that stops with no visible cause. `unless-stopped` rather than
`always` so an explicit `docker compose stop` stays stopped.

`kafka-init` keeps `restart: "no"`.

> **You would assume** the inconsistency is an oversight and that the init container should
> match the others. Making it match deadlocks startup.

The app services gate on `service_completed_successfully`, which requires the container to have
exited and stayed exited. A restarting one-shot container never reaches that state, so the
services waiting on it never start.

The tradeoff of `unless-stopped` on the apps is that a deterministic failure — a code error, a
genuinely missing topic — now restart-loops rather than dying once. Repeated restarts in
`docker compose ps` are the signal to read logs.

### /health reports the consumer, not the process

`/` is answered by the static mount and returns 200 whenever a bundle exists on disk. That is
true with the broker down and no bar received in an hour, so it cannot distinguish *serving a
page* from *connected to Kafka*. `/health` reports the bars consumer instead, and the container
healthcheck polls it.

Readiness is **the consumer task is alive and subscribed**, never *a bar has arrived*.

> **You would assume** a stream health endpoint should check that data is actually flowing, and
> that `last_bar_at` being `None` means unhealthy.

Bars seal a window plus a watermark delay after the events behind them, so a freshly started
server has a stretch with nothing to report through no fault of its own. Gating on arrival makes
the endpoint report unhealthy for that entire first window on every cold start — and because the
container healthcheck acts on the status code, the container would be marked unhealthy every
time it starts. `last_bar_at` is reported in the body as an observation, not a gate.

Subscription is used for the verdict rather than partition assignment, because assignment can
legitimately be empty mid-rebalance and would flap. An assignment that never fills is visible in
the body, and now means the topic is genuinely missing.

The endpoint returns 503 rather than 200 with a degraded body, because the healthcheck decides
on the status code alone. The probe is a `python -c` one-liner rather than curl, as the runtime
image ships neither curl nor wget.

## Development and production shapes

`docker-compose.yml` alone is the production shape. `docker-compose.override.yml` adds the Vite
dev server and `uvicorn --reload`.

Compose merges both files when no `-f` is given, and `-f` replaces the default file list rather
than adding to it. Naming `docker-compose.yml` explicitly is therefore what opts out:

```
  make dev       docker compose --profile apps up -d --build                       → dev
  make up-apps   docker compose -f docker-compose.yml --profile apps up -d --build → production
```

Both dev services stay in the `apps` profile, so a bare `docker compose up -d` remains broker
and Kafka UI only, with the application processes running on the host against
`localhost:29092`.

### The dev dashboard shares the web container's network namespace

```yaml
    network_mode: "service:web"
```

> **You would assume** this should be an ordinary service with its own `ports: - "5173:5173"`,
> and that sharing a namespace is an unnecessary complication.

`vite.config.ts` proxies `/ws` to `ws://localhost:8000`, and that file is the only place allowed
to name the backend. In a container of its own, `localhost` is *that* container, and the proxy
would forward to nothing. Sharing the web container's namespace makes `localhost:8000` genuinely
uvicorn, which preserves single origin in development without CORS middleware, without an
environment variable naming the socket, and without editing the Vite config.

The cost is that the dashboard container has no network stack of its own, so **5173 is published
on the `web` service** rather than on the dashboard.

### Anonymous volumes over bind-mounted directories

Two of them, both for the same reason: a bind-mount would otherwise bury something the image
built.

`/app/dashboard/node_modules` keeps the image's Linux-built dependencies from being shadowed by
the host's macOS-built ones, which are the wrong architecture. Because it is seeded from the
image, `npm ci` does not re-run at start — and because it persists across restarts, a
`package.json` change needs `--force-recreate`.

`/app/backend/static` keeps the bundle the image built rather than whatever stale output sits in
the host's `backend/static`, which the `./backend` bind-mount would otherwise bring in. In
development the live UI is on 5173; this keeps 8000 serving something real rather than an
artifact of unknown age.

## Known limitations

**`/health` does not detect an unreachable broker.** aiokafka retries internally, so an outage
leaves the consumer task alive and subscribed and the endpoint reports ready. Verified: the
broker was stopped for thirty seconds and both the endpoint and the container healthcheck stayed
green. Only `last_bar_at` goes stale, and nothing gates on it. This is the cost side of the
readiness rule above — chosen to avoid false alarms on every cold start, paid for with false
calm during an outage. Closing it means degrading on staleness only after a first bar has been
seen, which is a different contract than the one this endpoint currently offers.

**The aggregator connects to Kafka at import time.** `consumer.py` builds its `KafkaProducer` at
module level, so the connection is opened during import, before `main()`. The startup gates
cover the cold-start case and `restart: unless-stopped` covers a restart landing during an
outage — kafka-python retries bootstrap for about thirty seconds before raising, and Docker then
restarts the container with its own backoff. What remains is that the module cannot be imported
without opening a socket, which is as much a testability problem as a resilience one. Retry
logic wrapped around the constructor would close the last sliver while entrenching the import
time connection; moving construction into `main()` addresses both, and is where this should go.

**The broker has no restart policy.** If `kafka` dies, nothing brings it back, and the three app
services will restart-loop against it until it returns. The loop is self-healing once it does,
but the broker itself is the one component whose death needs manual intervention.
