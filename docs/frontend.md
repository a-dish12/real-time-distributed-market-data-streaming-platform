# The browser dashboard

*Design record — completes Stage 5. Changelog entry: [Stage 5](changelog.md#stage-5--websocket-fan-out-in-progress).*

Three symbols, three WebSockets, three candlestick charts. The server side of this is in
[`websocket_fanout.md`](websocket_fanout.md); this record covers what happens after a frame
arrives in the browser.

Almost every decision here follows from one fact: **Lightweight Charts is an imperative
library that owns a canvas, and React is a declarative renderer that owns the DOM.** Those two
statements are in tension, and most of this document is about where the boundary between them
sits and what happens on either side of it.

---

## Why the chart cannot be a React component

React's model is that you describe what the DOM should contain and React works out the
mutations. That model requires React to own the elements it is describing. Lightweight Charts
does not participate in it at all: `createChart(host)` takes a plain DOM element, appends its
own canvas elements inside it, and from then on draws pixels directly. React has no idea those
canvases exist, cannot diff them, and cannot reconcile them.

So the boundary is drawn at a single empty div. `CandleChart` renders exactly this:

```tsx
return <div ref={hostRef} className="sp__plotHost" />
```

React owns that div and nothing inside it. The library owns everything inside it and nothing
outside it. Neither ever touches the other's territory, and the `ref` is the only thing that
crosses.

### What each ref holds and why it is a ref

There are five refs in [`CandleChart.tsx`](../dashboard/src/CandleChart.tsx), and they exist
for two different reasons.

`hostRef` holds the div element itself. It is a ref because `createChart` needs a real DOM
node, and a DOM node only exists after React has committed the render — which is precisely
when an effect runs and precisely what a ref is for.

`chartRef` holds the `IChartApi` instance and `seriesRef` holds the `ISeriesApi<'Candlestick'>`.
Both are created in one effect and used in a different one. Without somewhere to put them, the
sink callbacks would have no way to reach the objects the lifecycle effect built. They are refs
rather than variables in the component body because a plain `const` is re-initialised on every
render; a ref is a stable box that survives them.

`lastTimeRef` holds the floored timestamp of the newest bar applied to the series. This is the
guard state. `series.update()` throws when handed a bar older than the last one it received, so
something has to remember what the last one was.

`lastCountRef` holds the tick count of the candle currently drawn at `lastTimeRef`. This one was
added late and the reason is worth stating precisely, because it is not obvious.

**The series is write-only.** You can push data into it with `setData` and `update`, but you
cannot ask it what it currently holds. Even if you could, it would not help: `toChartBar` maps a
`Bar` down to `{ time, open, high, low, close }` and throws `count` away, because `count` is not
something Lightweight Charts has any concept of. So by the time a bar reaches the canvas, its
completeness signal is gone.

That did not matter while equal timestamps were simply allowed through to `update()`. It started
mattering the moment the rule became *first wins unless the newcomer is more complete*, because
that rule needs to compare the incoming `count` against the count of the bar already drawn — and
nothing anywhere held that number. `feed.ts` has a `lastCount` field in its `Summary`, but it is
unusable for this: it is React state, so it is a render behind, it lives across a module
boundary, and it is written from the outcome the chart has not yet returned. `lastCountRef` is
written on exactly the lines where `lastTimeRef` is written, which is what keeps the pair
consistent.

### What the empty dependency array actually does

```tsx
useEffect(() => {
  const chart = createChart(host, { ... })
  const series = chart.addSeries(CandlestickSeries, { ... })
  const ro = new ResizeObserver(...)
  ro.observe(host)
  return () => { ro.disconnect(); chart.remove(); /* null the refs */ }
}, [])
```

React runs an effect after the commit in which its dependencies changed. An empty array means
there is nothing that can change, so the effect body runs once after the first commit and the
cleanup runs once at unmount. The chart is therefore created exactly once for the lifetime of
the component and destroyed exactly once.

The dependency array is the entire mechanism. Put `bars` in it and the chart is destroyed and
rebuilt every time a bar arrives.

Cleanup order is deliberate. `ro.disconnect()` comes before `chart.remove()` so the observer
cannot fire a resize callback into a chart that has already been destroyed — a `ResizeObserver`
callback is delivered asynchronously, so an observer that is still connected when the chart goes
away has a real window in which to call `applyOptions` on a dead object. `chart.remove()` then
tears down the canvases, detaches the mouse and wheel listeners the library attached, and
releases the drawing context. Skipping it leaks a detached DOM subtree and a set of live event
listeners on every mount.

The refs are nulled and `lastTimeRef`/`lastCountRef` zeroed in the same cleanup, so a remount
starts from a known state rather than comparing new bars against timestamps belonging to a chart
that no longer exists.

### What would actually go wrong with bar data in state

Two failure modes, and they are not the same size.

The mild one: `const [bars, setBars] = useState<Bar[]>([])`, appended once per bar, with the
chart effect still keyed on `[]`. The chart survives. What you get is a full React render of the
panel subtree once per second per symbol, three times over, to update numbers that a small
snapshot object could have carried. Session high and low would be computed by mapping the whole
retained array on every render — which is what the design-system reference implementation does,
and is the thing that makes retaining the array feel necessary in the first place.

The severe one follows almost immediately, because the natural next step is to make the chart
react to the data:

```tsx
useEffect(() => { /* create chart, setData(bars) */ }, [bars])
```

Now every arriving bar destroys the chart and builds a new one. The concrete symptom is
specific: the canvas is replaced, so the viewport returns to its default, and `fitContent()`
runs again on the fresh chart. **Any zoom or pan the user has applied is discarded once per
second.** Dragging the chart backwards in time becomes impossible — the view snaps back before
the mouse is released. Alongside that, a visible flash as one canvas is swapped for another, and
a frame budget spent rebuilding a chart rather than drawing one.

This is why the invariant is worth verifying rather than asserting. The check that actually
proves it is DOM node identity: tag every canvas element, leave the app running while bars
stream in, and confirm the same nodes are still there. Measured on the production build, 21 of
21 canvas nodes survived a thirty-second run during which the series grew from 32 to 62 bars,
with a 0.25% pixel difference across the hold — the moving price line, and nothing else.

### The alternative that was available

React wrappers for Lightweight Charts exist, and the honest version of this decision is that one
could have been used. What they buy is a declarative API: `<Chart data={bars} />`. What they cost
is that the data must then live somewhere React can see it, which reintroduces exactly the
problem above, and the wrapper decides internally when to call `setData` versus `update` — a
decision this application needs to make itself, because the whole late-and-duplicate policy lives
in that choice. A wrapper that calls `setData` on every data change would reset the viewport on
every bar. Owning the imperative calls directly is what makes the policy expressible.

### Questions

1. If the chart is created in an effect with `[]`, what would break if the `host` element were
   conditionally rendered — say, only once bars exist?
2. `lastCountRef` and `lastTimeRef` are written together in three places. What happens if a
   future change updates one without the other, and would anything fail loudly?
3. Why is `chartRef` needed at all, given that `seriesRef` is what the sink writes to?

---

## From socket frame to candle

One bar, followed the whole way.

It arrives as a string in `ws.onmessage`, which calls `handleFrame` in
[`feed.ts`](../dashboard/src/feed.ts). Three things have to happen before it can be drawn: it
has to be proven to be a bar, it has to be placed relative to what is already on screen, and it
has to be turned into the shape Lightweight Charts accepts.

**Validation** happens first, in `parseBarMessage` in
[`types.ts`](../dashboard/src/types.ts). It is covered in its own section below; the relevant
part here is that it returns `Bar | null` and never throws, so a bad frame costs one candle
rather than the connection.

**The transport tag is stripped immediately**:

```ts
const { type, ...bar } = msg
```

From this line onward, `bar` has no `type` field. That is deliberate. The tag answers exactly
one question — *am I still filling initial history?* — and it is answered here and nowhere else.
Nothing downstream can accidentally use `"live"` as a claim about recency, because nothing
downstream can see it.

**Backfill accumulates, then flushes.** A `backfill` frame is pushed onto `backfillBuf` and a
250ms timer is restarted. The burst ends when either the first `live` frame arrives or the timer
fires, whichever comes first, and both paths call `flushBackfill()` exactly once, guarded by
`backfillFlushed`.

The server gives no end-of-backfill marker — `webserver.py` sends its history in a tight loop
and then flips a flag — so the boundary has to be inferred. Two delimiters are needed because
each covers a case the other misses. A live frame alone would never flush when the producer has
stopped and no live bar is coming, leaving sixty bars stranded in a buffer behind an empty
chart. A timer alone would work but would delay the first paint by 250ms in the common case. The
duration itself sits between two well-separated timescales: backfill frames on localhost arrive
microseconds apart, live bars arrive a second apart, so 250ms cannot fire mid-burst and cannot
be mistaken for the gap between live bars. It is a threshold, not a tuned constant.

**The sort happens in `flushBackfill`**, on the raw float, before the sink is called:

```ts
const sorted = [...this.backfillBuf].sort((a, b) => a.window_start - b.window_start)
```

`setData` requires ascending time order and throws otherwise. In practice the frames arrive
ascending already — the server reads a per-symbol deque in append order — and measurement
confirmed it. The sort stays because *arriving ascending is not a guarantee the server makes*,
sixty elements cost nothing to sort, and the failure it prevents is a thrown exception rather
than a cosmetic defect. It sorts a copy rather than the buffer in place, so `lastBackfill` and
the array handed to the sink are the same object and the buffer can be cleared independently.

**Flooring happens on the chart side, in three places, all in `CandleChart`.** `window_start` is
Unix seconds as a float (`1786093433.0`); Lightweight Charts wants integer seconds. So
`toChartBar` floors when building the row, the backfill dedupe floors both sides of its
comparison, and `onLive` floors before comparing against `lastTimeRef`.

The split between sorting on the float and comparing on the floored value is safe because
flooring is monotonic: sorting by the float and then flooring cannot produce a descending
sequence. What it *can* produce is two adjacent rows with the same floored timestamp, which
`setData` rejects, so the backfill dedupes:

```ts
const deduped = bars.filter((b, i) =>
  i === bars.length - 1 ||
  Math.floor(b.window_start) !== Math.floor(bars[i + 1].window_start))
```

This keeps the **last** element of any run of equal floored timestamps. Since `Array.sort` is
stable, equal `window_start` values retain arrival order, so the last one is the most recently
published bar for that window. The filter runs over `Bar` objects rather than the mapped chart
rows specifically so that `count` is still reachable afterwards, which is how `lastCountRef` gets
its initial value.

**The guard sits in `onLive`, inside the sink, in `CandleChart`** — not in the feed. That
placement is the important part. The feed cannot make this decision because the feed does not
know what is drawn: the chart may have deduped rows out of the backfill, may not have mounted
yet, and is the only thing holding `lastTimeRef`. So the chart decides and returns a
`LiveOutcome`, and the feed tallies whatever it is told.

```ts
const time = Math.floor(bar.window_start)

if (time < lastTimeRef.current) return 'late-dropped'

if (time === lastTimeRef.current) {
  if (bar.count <= lastCountRef.current) return 'duplicate-ignored'
  series.update(toChartBar(bar))
  lastCountRef.current = bar.count
  return 'duplicate-superseded'
}

series.update(toChartBar(bar))
lastTimeRef.current = time
lastCountRef.current = bar.count
return 'applied'
```

### The three declined-or-replaced outcomes

**`late-dropped` — `time < lastTimeRef`.** The bar belongs to a window older than the newest one
drawn. `update()` would throw. It is declined and counted.

The alternative is to keep every bar in a `Map` keyed by timestamp, insert the late one, and call
`setData` with the rebuilt series. That works and produces a strictly more correct chart. Its
cost is that `setData` replaces the series wholesale, which resets the viewport — so correcting a
candle that has already scrolled into history throws away the zoom and pan the user is currently
holding. **The cost lands on the person looking at the screen, not on the CPU**, and it is paid
to fix something they are probably not looking at. Declining is cheaper and, because the count
is surfaced in the footer, it is visible rather than silent.

**`duplicate-ignored` — `time === lastTimeRef` and `count <= lastCountRef`.** The same window
published twice, with the newcomer no more complete than what is drawn. Declined.

**`duplicate-superseded` — `time === lastTimeRef` and `count > lastCountRef`.** The same window
published twice, with the newcomer strictly more complete. `update()` is called, which replaces
the last bar in the series rather than appending, and `lastCountRef` moves. `lastTimeRef` does
not, because the timestamp has not changed.

`count` is used as the completeness signal because it is the only field on the wire that carries
that information — this is why [`output_pipeline.md`](output_pipeline.md) says count is carried
"because nothing else in the message determines it." OHLC cannot be compared for completeness:
a partial candle's high can legitimately equal a complete one's.

The comparison is `<=` rather than `<` so that an identical re-emission is declined rather than
redrawn. That matters more than it looks, because identical re-emission is the *normal* case:
reading the entire `bars` topic after several aggregator restarts found 35 re-published windows
out of 9,605, and **every one of them had a count identical to the original**. So `duplicate-
ignored` is the branch that fires in ordinary operation, and `duplicate-superseded` is the rare
one.

That it can fire at all is not obvious, and it is the reason this rule exists rather than a plain
first-wins. The aggregator's wall-clock backstop
([`idle_partitions.md`](idle_partitions.md)) can force-seal a window during live running, after
which a tick that genuinely belongs to that window fails the late check and is dropped. During a
backlog replay that same backstop is suppressed — it requires the partition to be both idle and
caught up, and a draining consumer is neither — so the replayed window stays open long enough to
absorb the tick the live pass rejected. **The replay can therefore be the more complete copy.**
A strict first-wins rule would pin the chart to the worse candle in exactly that case.

### Why they are tallied separately

Each one names a different upstream condition, and the whole point of surfacing them is
diagnosis:

- `late-dropped` means bars are arriving for windows older than the drawn history — a replay
  reaching further back than the sixty bars on screen, or a genuine ordering problem.
- `duplicate-ignored` means the aggregator re-emitted windows it had already published
  identically. That is the expected signature of a restart replaying from its last committed
  offset, and it is benign.
- `duplicate-superseded` means a re-emission was *better* than what was drawn, which points at
  the backstop asymmetry above and at data that was dropped during live running.

Collapsing them into one "anomalies" counter would make a routine restart indistinguishable from
a correctness problem. There is a fifth outcome, `not-drawn`, returned when no series exists; it
increments nothing, because nothing was drawn and there is no upstream condition to report.

### Questions

1. The sort is on the unfloored float and the guard compares floored integers. Construct a
   sequence of `window_start` values where that distinction changes the outcome.
2. Backfill dedupe keeps the last of a run of equal floored timestamps. Why is keeping the last
   correct rather than the first, and what property of `Array.sort` is that relying on?
3. `duplicate-superseded` calls `update()` without moving `lastTimeRef`. Walk through what
   happens on the next three bars if `lastTimeRef` were moved as well.

---

## One origin

An **origin** is the triple of scheme, host, and port. `http://localhost:5173` and
`http://localhost:8000` are different origins — same scheme, same host, different port — and the
browser treats them as separate security domains for cookies, storage, and cross-origin request
policy.

The rule this frontend follows is that **the page and its WebSocket are always the same origin**,
which means the application never has to know where the backend is:

```ts
export function socketUrl(symbol: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}/ws/${encodeURIComponent(symbol)}`
}
```

`window.location.host` is host-and-port. The protocol is upgraded to `wss:` when the page is
served over HTTPS, because a secure page is not permitted to open an insecure WebSocket — the
browser blocks it as mixed content. `encodeURIComponent` is not load-bearing today, since the
symbols are three hardcoded constants, but it is the correct treatment of a variable
interpolated into a URL path.

### Production: why the build lands in the backend

`vite.config.ts` sets `build.outDir` to `../backend/static`, so `npm run build` writes the
bundle *into the Python project*. `webserver.py` then mounts that directory at `/`. The
consequence is that a browser loading `http://localhost:8000/` gets `index.html` from uvicorn,
`window.location.host` evaluates to `localhost:8000`, and the socket opens against
`ws://localhost:8000/ws/AAPL` — the same process that served the page. One origin, no
configuration, nothing to get wrong.

### Development: why the same code would be wrong

Vite's dev server exists because the production bundle is not what you want to iterate against:
it serves modules unbundled over native ESM and pushes edits into the running page without a
reload. It serves on port 5173. Uvicorn is on 8000.

So in development `window.location.host` is `localhost:5173`, and the socket URL derived from it
points at Vite, which knows nothing about `/ws/AAPL`. **The same line of code that is correct in
production is wrong in development**, and it is wrong in the direction that is easy to miss —
the developer's instinct is to hardcode `ws://localhost:8000`, which works immediately on their
machine and breaks the moment the app is served from the backend, where port 8000 is where the
*page* came from and the hardcoded value happens to still work, right up until the port or host
changes.

The proxy is what makes one expression correct in both places:

```ts
server: { proxy: { '/ws': { target: 'ws://localhost:8000', ws: true, changeOrigin: true } } }
```

Vite intercepts requests whose path starts with `/ws` and forwards them to uvicorn. The
application still asks for its own origin; the dev server quietly makes that true.

### Why `ws: true` is required — the upgrade handshake

A WebSocket connection does not begin as a WebSocket. It begins as an ordinary HTTP/1.1 GET
carrying headers that ask to change protocol:

```
GET /ws/AAPL HTTP/1.1
Host: localhost:5173
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

If the server accepts, it replies with a status that is not 200:

```
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

`Sec-WebSocket-Accept` is the client's key concatenated with a fixed GUID, SHA-1'd, and
base64'd. Its only purpose is to prove the responder actually understood the WebSocket handshake
rather than being a cache or proxy that echoed something plausible. After the 101, that same TCP
connection stops carrying HTTP and starts carrying WebSocket frames in both directions until
someone closes it.

This is why `ws: true` exists. Node's HTTP server does not deliver an upgrade request through
the normal `'request'` event — it emits a **separate `'upgrade'` event**, handing over the raw
socket and the parsed headers, precisely because the connection is about to stop being HTTP.
A proxy that only listens for `'request'` never sees the upgrade at all.

`ws: true` is what subscribes the proxy to that second event and gives it the machinery to pipe
the two raw sockets together once the 101 has been relayed. Without it, `/ws/AAPL` is treated as
a normal HTTP route: the request is forwarded, the 101 comes back, and the proxy has nowhere to
put a connection that is no longer speaking HTTP. **The observable symptom is a socket that
opens and immediately closes with no error message**, which reads exactly like a backend fault
and sends you debugging the wrong process.

`changeOrigin: true` rewrites the `Host` header on the forwarded request from `localhost:5173`
to `localhost:8000`. Uvicorn does not inspect `Host` or `Origin` here, so it changes nothing
today. It is set because the forwarded request should be indistinguishable from a direct one,
and the moment anything upstream does virtual-host routing or validates `Origin` — which a
production WebSocket server is expected to do — the unrewritten header becomes a silent
rejection.

### The StaticFiles bug

`webserver.py` imported `StaticFiles` at line 9 and never called `app.mount`. The import had
been there since the file was written; the mount had not. So the intent was recorded and the
behaviour was absent: a build sitting in `backend/static` was completely invisible, `GET /`
returned 404, and uvicorn served nothing but the WebSocket route. This is the failure mode where
reading the imports tells you the opposite of what the code does.

The fix is three lines at the **bottom** of the file, and the position is not stylistic:

```python
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

if os.path.isdir(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    print(f"no build at {STATIC_DIR} — run `npm run build` in dashboard/ to serve the UI")
```

**Mount ordering.** Starlette matches routes in registration order and stops at the first match.
A `Mount` at `"/"` matches every path there is. Registered above `@app.websocket("/ws/{symbol}")`
it would therefore swallow the WebSocket route entirely: an upgrade request for `/ws/AAPL` would
be handed to `StaticFiles`, which would look for a file at `static/ws/AAPL`, fail to find one,
and return 404. The WebSocket endpoint would be unreachable dead code, with no error at import
time and nothing in the logs beyond a 404 — and the frontend symptom would be identical to the
missing-`ws: true` symptom above. Registering the catch-all last means every specific route is
tried first and the mount only sees what nothing else claimed.

**`html=True`** makes `StaticFiles` serve `index.html` for a request to a directory, so `GET /`
returns the application rather than a 404 or a directory listing.

**The `isdir` guard** exists because `StaticFiles(directory=...)` raises `RuntimeError` at
construction time if the directory is missing — and construction happens at module import, so
uvicorn would refuse to start at all. On a fresh clone, before anyone has run `npm run build`,
that turns "the UI is not built yet" into "the server does not boot," with an error message
about a missing directory that has nothing to do with the data pipeline. The guard degrades that
to a printed hint and a fully working WebSocket server.

Its accepted cost is real and was hit during verification: because the check runs at import,
**building while uvicorn is running does not mount anything.** The server must be restarted
after the first build. A `check_dir=False` mount would avoid the restart at the price of
returning 404s from a route that claims to serve files; the guard was preferred because the
printed message says exactly what to do.

### What single-origin costs, and what the alternatives cost

The honest cost is that uvicorn is now a static file server, and it is not a good one — no
compression, no cache headers, no CDN. At this scale that is invisible. If it stopped being
invisible, the fix is a reverse proxy in front serving `/` from disk and forwarding `/ws` to
uvicorn, which *preserves* single-origin rather than abandoning it. So the decision does not
have to be revisited to scale; it has to be extended.

**A separate frontend deploy** — the bundle on a static host or CDN, the backend elsewhere — is
the conventional arrangement and buys independent deploy cadence, edge caching, and separate
scaling of the two tiers. It costs cross-origin: the browser now sends an `Origin` header the
backend is expected to validate, so an allowlist appears in the server config and has to be
correct in every environment. It also forces the frontend to learn the backend's address, which
is the next alternative and its problems.

**CORS** would be the mechanism for the HTTP side of that arrangement — `CORSMiddleware` with an
explicit origin list. The cost is a security-relevant configuration surface that must be right
everywhere, and whose shortcut (`allow_origins=["*"]`) is subtly wrong as soon as credentials are
involved. Under single-origin the question never arises, because there is no cross-origin request
to permit.

**An env-var backend URL** — `VITE_WS_URL=ws://localhost:8000` read as `import.meta.env` — is the
most tempting because it is three lines. It costs more than it looks. Vite inlines env vars at
**build** time, so the URL is baked into the bundle: one artifact can no longer be promoted from
staging to production, and you need either a build per environment, a runtime config fetch, or a
placeholder rewritten at container start. All three are real machinery. And the failure mode is
the specific one worth avoiding — it works on the developer's machine, where the variable is set
in `.env.local`, and breaks in the deployed environment where nobody set it. `window.location`
has no configuration, so it cannot be misconfigured.

### Questions

1. The page is served over HTTPS behind a load balancer that terminates TLS and speaks HTTP to
   uvicorn. Does `socketUrl` still produce a working URL, and what would break it?
2. If `app.mount("/")` were registered before the WebSocket route, what exact HTTP status would
   the browser see for the upgrade request, and why is that hard to diagnose from the frontend?
3. Vite's proxy is a dev-server feature. What serves `/ws` when someone runs `npm run preview`
   against the production build, and does it work?

---

## The four data behaviours

Each of these is a documented property of the upstream pipeline, and each is handled somewhere
specific.

**`live` does not mean recent.** The tag is assigned by the web server on the basis of "this
arrived after I finished replaying your history" — [`websocket_fanout.md`](websocket_fanout.md)
is explicit that it is not a claim about recency. The handling is that `handleFrame` uses `type`
only to decide whether the backfill burst is still open, and then discards it via
`const { type, ...bar } = msg`. Placement is by `Math.floor(bar.window_start)` in `onLive`,
compared against `lastTimeRef`. **Arrival order never determines position.** An implementation
that appended in arrival order would draw a catch-up burst as a jagged excursion into the future
and then throw.

**The series has genuine holes.** The producer emits roughly twice a second with jitter, so
occasionally no tick lands in a one-second bucket and no candle exists. This is handled by
**deliberately doing nothing**. No code detects a missing second, and none fills one. Lightweight
Charts spaces candles by their timestamps, so an absent second renders as a gap.

The two alternatives were both rejected for the same reason. Interpolating invents a price that
never traded. Carrying the previous close forward draws a flat candle that is visually
indistinguishable from a real one-tick bar, so the chart asserts a trade happened when none did.
A gap is the only rendering that does not lie. Its cost is that a viewer cannot distinguish "no
ticks that second" from "the pipeline lost something," and nothing on screen resolves that
ambiguity.

**History can straddle a much larger gap.** The backfill is the last sixty candles *produced*,
not the last sixty seconds, so it can span a producer outage of any length. This is handled by
never assuming contiguity anywhere. The sort in `flushBackfill` does not assume the frames arrive
in order; `barCount` counts bars rather than inferring elapsed seconds; `fitContent()` fits to
whatever range the data actually covers rather than to a fixed window; and the footer's window
width comes from `window_end - window_start` on the message rather than a hardcoded 1, following
the principle in [`output_pipeline.md`](output_pipeline.md) that a message should be
interpretable without knowing the producer's configuration.

**On a fresh server the history is short.** History lives in the web server's memory and fills at
one bar per second from empty, so a client connecting ten seconds after a restart gets ten bars.
`StreamPanel` handles this with `cold = barCount > 0 && barCount < 8`, which renders a "cold
start" badge, and with an empty-state overlay at `barCount === 0` whose text distinguishes
*connected and waiting* from *socket closed*. Critically, the overlay sits **on top of** a chart
host that is always mounted, rather than replacing it — so the chart is never rebuilt when the
first bar arrives, and three candles render as three candles rather than as a failure.

### Questions

1. A gap is rendered as a gap. What would you change if the requirement became "distinguish a
   quiet second from a dropped message" — and what would the backend have to send?
2. `cold` is `barCount < 8`. What is that eight actually a proxy for, and what breaks if the
   window width changes from one second to one minute?
3. The backfill sort is defensive against something measurement showed does not happen. Under
   what upstream change would it start mattering?

---

## Structure, and what state exists

Four modules carry logic, and the split between them is the state boundary.

[`feed.ts`](../dashboard/src/feed.ts) owns the connection, the reconnect policy, the backfill
staging, and the running aggregates. **It does not import React.**
[`useSymbolFeed.ts`](../dashboard/src/useSymbolFeed.ts) is the only place React state is
declared. [`CandleChart.tsx`](../dashboard/src/CandleChart.tsx) owns the imperative boundary and
the placement policy. [`StreamPanel.tsx`](../dashboard/src/StreamPanel.tsx) renders from a
snapshot and holds no state at all. Supporting these,
[`types.ts`](../dashboard/src/types.ts) validates the wire format and
[`color.ts`](../dashboard/src/color.ts) converts design-system `oklch()` tokens to hex, because
Lightweight Charts parses colour strings itself and does not understand `oklch`.

### Why `feed.ts` is a separate, React-free module

Three reasons, in increasing order of importance.

It is testable without a DOM or a renderer — a plain class with a callback interface.

The connection's lifecycle is genuinely not React's. It has to survive re-renders, hold a retry
timer across them, and not be recreated when a parent happens to render. Expressing that inside a
component means fighting the component's lifecycle; expressing it outside means the component
merely starts and stops it.

Most importantly, **it makes the central rule structural rather than disciplinary.** The
requirement is that bar data never enters React state. A file that does not import React cannot
call `useState`, so in the module where every bar is handled, the mistake is not available. The
rule stops depending on whoever edits the file next remembering the comment.

The cost is one indirection. A reader following a bar has to go through the `BarSink` interface
to find out what happens to it, and the outcome comes back through a return value rather than
being visible at the call site. The alternative — socket handling inside a `useEffect` in
`CandleChart` — would have been shorter and would have put the socket and the chart in the same
effect, so a chart remount would drop and reopen the connection, and StrictMode's double-mount
would produce a connect/disconnect/connect cycle on the socket rather than only on the chart.

### The complete inventory of React state

Three `useState` calls, all in `useSymbolFeed`:

`status` is the connection state, driving the chip. `attempt` is the retry counter shown while
reconnecting. Both must re-render text, which is what state is for.

`summary` is a `Summary` object rebuilt at most once per bar. It contains **only flat numbers**:
`lastClose`, `lastCount`, `lastWindowStart`, `lastWindowSpan`, `firstOpen`, `high`, `low`,
`barCount`, and the three outcome tallies. No `Bar` object, no array.

It held `last: Bar | null` at one point, which was arguably within the letter of the spec — the
latest close is explicitly permitted in state — and was changed anyway for two reasons. A
reviewer grepping for bar data in `useState` would find a retained `Bar` and be right to flag it;
and holding the object is the invitation for someone to add `bars: Bar[]` beside it, which is the
failure this whole design exists to prevent. Flat primitives close both.

`feedRef` is a ref, initialised lazily in the render body:

```ts
const feedRef = useRef<SymbolFeed | null>(null)
if (feedRef.current === null) { feedRef.current = new SymbolFeed(symbol, { ... }) }
```

Not `new SymbolFeed(...)` inline, which would construct one on every render and discard it. Not
`useState(() => new SymbolFeed(...))`, because nothing should re-render when it is created. Not
constructed inside the effect, for the StrictMode reason below.

### Session high and low fold declined bars

In `accumulate`, `high` and `low` update on every bar regardless of outcome, while `lastClose`,
`lastCount`, `lastWindowStart` and `lastWindowSpan` update only when the chart actually drew it:

```ts
const drawn = outcome === 'applied' || outcome === 'duplicate-superseded'
lastClose: drawn ? bar.close : a.lastClose,
...
high: a.high === null ? bar.high : Math.max(a.high, bar.high),
low:  a.low  === null ? bar.low  : Math.min(a.low,  bar.low),
```

The reasoning is that these two groups answer different questions. The headline price answers
*what is the candle on screen*, so it must follow the chart exactly — otherwise the header can
show a price that has no candle under it, which was a real inconsistency before the outcome was
threaded back. High and low answer *what did this symbol do*, which is a statement about the
data, not about the rendering. **A bar that arrived too late to plot still reports a price that
genuinely traded in that window.** Excluding it would make the session high quietly under-report
during a replay burst.

The cost is stated plainly because it is real: high or low can display a value with no
corresponding candle visible, which looks like a bug to someone reading only the picture. That
was chosen over a high that silently under-reports, on the grounds that a tool whose job is to
be truthful about prices should prefer the visible inconsistency to the invisible omission.

`barCount` increments only on `applied` — a superseded duplicate replaces a candle rather than
adding one, and declined bars add nothing — so it is the count of distinct candles drawn. The
label "bars held" is slightly wrong, since nothing is held in JavaScript memory; the canvas holds
them.

`firstOpen` is seeded from the first bar of the **current connection**, so the change and
percentage reset on every reconnect. That is honest for what it claims — change across what is
on screen — but it is not a daily change and must not be read as one.

### Questions

1. `summary` is replaced wholesale once per bar, so `StreamPanel` re-renders three times a
   second across three symbols. Why is that acceptable here, and at what point would it stop
   being?
2. High and low deliberately include bars the chart declined. Construct the sequence where a
   user sees a session high that no candle on the chart reaches.
3. `feedRef` is created in the render body rather than in an effect. What is the general rule
   about side effects during render, and why does this not violate it?

---

## The connection: reconnect, backoff, and teardown

`scheduleReconnect` implements capped exponential backoff with jitter:

```ts
const base = Math.min(BACKOFF_MAX_MS, BACKOFF_MIN_MS * 2 ** Math.max(0, this.attempt - 1))
const delay = base * (0.7 + Math.random() * 0.6)
```

500ms, 1s, 2s, 4s, 8s, then held at 8s, each multiplied by a random factor between 0.7 and 1.3.

**The jitter is not decoration.** Three sockets drop simultaneously whenever uvicorn restarts.
Without jitter all three would retry at exactly the same instants forever, so every retry wave
hits the server as three simultaneous connection attempts and either all succeed or all fail
together. Spreading them means a recovering server sees them arrive separately. This is the same
reasoning as jittered retry in any distributed client; it matters here at a scale of three
because they are perfectly correlated.

**The cap is a deliberate trade.** Unbounded doubling would leave a dashboard open through a five
minute outage taking minutes to notice recovery. Capping at 8s bounds the reconnect latency at
the cost of a steady low-rate retry against a server that is genuinely down. For a tool someone
leaves open on a second monitor, noticing recovery quickly is worth more than sparing a dead
server a request every eight seconds.

`attempt` resets to 0 in `onopen`, so a connection that succeeds and later drops starts its next
backoff sequence from the minimum rather than resuming an old one.

### The identity guard

All four handlers begin with the same line:

```ts
ws.onclose = () => { if (this.ws !== ws) return; ... }
```

Each handler closes over the specific socket it was attached to and compares it against the
field. After `stop()` or a fresh `open()`, `this.ws` points at a different socket or at `null`,
but events queued on the *old* socket can still be delivered — a close event scheduled before the
handlers were detached. Comparing identity discards them. Without it, a stale close from a
discarded socket would set the status to disconnected and schedule a reconnect for a feed that
already holds a healthy connection.

`ws.onerror` is deliberately empty, with a comment saying why: `onclose` always follows an error,
so driving reconnection from `onclose` alone guarantees the reconnect is scheduled exactly once.
`scheduleReconnect` additionally returns early if `reconnectTimer` is already set, which is a
second guard against the same double-scheduling.

The `new WebSocket(...)` call is wrapped in `try`/`catch` because the constructor throws
synchronously on a malformed URL rather than reporting through `onerror` — and a throw there
would escape into whatever called `open()`, which on the retry path is a timer callback with
nobody to catch it.

### Teardown

`stop()` runs from the effect cleanup in `useSymbolFeed`, and its ordering is load-bearing:

```ts
this.closed = true                    // a queued open() now returns immediately
clearTimeout(this.reconnectTimer)     // no pending retry
clearTimeout(this.backfillTimer)      // no pending flush
const ws = this.ws
this.ws = null
if (ws) {
  ws.onopen = ws.onclose = ws.onerror = ws.onmessage = null
  ws.close()
}
```

Handlers are detached **before** `close()` is called. Calling `close()` first would fire
`onclose`, which would call `scheduleReconnect`, which would reopen a socket for a feed that is
being torn down. `closed = true` is set first so that anything already in flight — a timer
callback that fired between the `clearTimeout` calls — finds the feed shut and returns.

`CandleChart` has its own cleanup, `() => feed.setSink(null)`, so the feed stops calling into a
chart whose series has been removed. Together with the chart lifecycle cleanup, unmount leaves no
pending timer, no open socket, no connected observer, no chart, and no sink.

### What StrictMode's double-mount does

`main.tsx` wraps the app in `<StrictMode>`. In development React deliberately mounts every
component, runs its effects, runs the cleanups, and mounts again — a test that cleanup is
correct. It does not happen in production builds.

Measured here: our `ResizeObserver` is constructed 6 times and disconnected 3 across three
charts, which is two mounts each and one cleanup each with three left running. WebSocket URLs
are opened twice per symbol in development and once per symbol in the production build. The
chart is created twice and `chart.remove()` called once. **The fact that those numbers line up
exactly is the evidence that the cleanup paths run**, which is the whole reason to leave
StrictMode on.

It forced one design decision. `feedRef` must be created in the render body and survive the
double-mount, rather than being constructed inside the effect. If the feed were built in the
effect, StrictMode would build one, tear it down, and build another — and the chart's sink
registration, which lives in a *different* effect, could bind to the discarded instance and
receive nothing. Creating it once under a null check makes it stable across the cycle.

`setSink` replaying `lastBackfill` to a newly registered sink exists for the same reason: on the
second mount, the connection from the first mount may already have flushed its backfill, so a
chart registering afterwards would otherwise start empty and only fill as live bars arrived.

The development-only cost is two connections per symbol briefly at startup, and a
connect/disconnect pair visible in the uvicorn log on every page load.

### Questions

1. `stop()` detaches handlers before calling `close()`. What sequence of events would you see if
   those two lines were swapped?
2. The backoff caps at 8s and jitters ±30%. What would you change for a client that must not
   hammer a recovering server, and what would you give up?
3. StrictMode opens two sockets per symbol in development. Why does the second connection's
   backfill not produce a burst of `duplicate-ignored` on the chart?

---

## Validation and malformed frames

Three layers, none of which can close the connection:

```ts
if (typeof data !== 'string') return          // binary frame, ignored
try { raw = JSON.parse(data) } catch { return }  // invalid JSON, skipped
const msg = parseBarMessage(raw)
if (!msg) return                              // wrong shape, skipped
```

`parseBarMessage` in `types.ts` checks that `type` is one of the two known discriminants, that
`symbol` is a string, and that all seven numeric fields pass `Number.isFinite`. It returns
`BarMessage | null` and never throws.

`Number.isFinite` rather than `typeof x === 'number'` is a real distinction, not pedantry. `NaN`
and `Infinity` are both numbers in JavaScript, and JSON can produce `Infinity` from a literal
like `1e999`. Either one reaching the chart is worse than a rejection: `Math.floor(NaN)` is
`NaN`, and a `NaN` timestamp handed to `setData` corrupts the series ordering **without
throwing**, which is the failure mode this whole file exists to avoid.

The validated object is rebuilt field by field rather than cast. A cast would let extra fields on
the wire flow through into the chart and would make `Bar` a claim rather than a guarantee;
rebuilding means the object has exactly the declared shape.

**Skipping rather than closing** is the right treatment because the connection carries a stream
of independent messages and one bad frame says nothing about the next. Closing would discard the
good frames still queued and trigger a reconnect, which makes the server resend the entire
backfill — a far larger disruption than losing one candle.

The cost is worth naming because it is a genuine gap: **nothing counts rejected frames.** The
three tallies count interesting outcomes, not invalid input. A backend schema change that
renamed a field would therefore present as a chart that simply stopped updating, with a healthy
connection indicator and no error anywhere. A fourth counter for rejected frames would close
this and has not been added.

### Questions

1. A frame arrives with `count` as the string `"2"` instead of a number. Trace what happens and
   what the user sees.
2. Rejected frames are not counted. What would you have to change to distinguish "the producer
   stopped" from "every frame is being rejected," from the UI alone?
3. `parseBarMessage` returns `null` rather than throwing. What would change if it threw and the
   caller caught it instead?

---

## Build configuration

`package.json` defines the build as `tsc -b && vite build`, and the ordering is the point. Vite
transpiles TypeScript with esbuild, which **erases types without checking them** — a type error
produces a perfectly good bundle. Running `tsc -b` first means a type error fails the build
rather than shipping. Without it, `strict` is decoration.

`tsconfig.app.json` sets `erasableSyntaxOnly: true`, which forbids TypeScript syntax that has
runtime meaning — enums, namespaces, and constructor parameter properties. That is why
`SymbolFeed` declares its fields and assigns them in the constructor body:

```ts
private readonly symbol: string
private readonly cb: FeedCallbacks
constructor(symbol: string, cb: FeedCallbacks) { this.symbol = symbol; this.cb = cb }
```

rather than the shorter `constructor(private readonly symbol: string, ...)`. The flag exists
because Node and browsers can now strip TypeScript types without transforming the code, and it
keeps the source inside the subset where that is possible.

In `vite.config.ts`, two build options beyond the proxy matter. `outDir: '../backend/static'`
writes outside the project root, which Vite permits but treats as unusual. `emptyOutDir: true` is
required as a result: Vite refuses to delete the contents of a directory outside its root without
explicit permission, because doing so silently would be dangerous. It is needed so that
content-hashed assets from previous builds do not accumulate indefinitely. The risk it accepts is
that anything else placed in `backend/static` is deleted on every build — nothing is, and the
directory is gitignored precisely because it is generated.

### Questions

1. If `npm run build` were just `vite build`, what class of error would reach production, and
   would the app necessarily crash?
2. `emptyOutDir: true` deletes a directory inside the Python project. What would go wrong if
   someone committed a file there, and how would they find out?
3. `erasableSyntaxOnly` rules out enums. Where in this codebase would an enum have been the
   natural choice, and what was used instead?

---

## Known limitations

**Rejected frames are invisible.** Covered above: no counter, so a schema mismatch looks like a
stalled feed.

**Backfill boundaries are inferred, not signalled.** The 250ms idle timer plus first-live-frame
heuristic works, but the server could simply send an explicit end-of-backfill message and remove
the ambiguity entirely. That is a one-line change in `webserver.py` and was not made because the
backend contract was fixed for this stage. It is the obvious next improvement.

**The two reset paths must stay in sync.** On reconnect, `flushBackfill` resets the aggregates in
`feed.ts` while `onBackfill` resets `lastTimeRef` and `lastCountRef` in `CandleChart.tsx`. They
are in different files and correctness depends on both happening. Nothing enforces it.

**Changing a mounted panel's `symbol` prop would connect to the wrong endpoint.** `feedRef` is
created once with the initial symbol and never recreated, while the effect's dependency array is
`[symbol]` — so a changed symbol would stop and restart the *same* feed, still pointing at the
original endpoint. This never happens because `App.tsx` renders `<StreamPanel key={s} symbol={s} />`,
and the `key` forces a full remount rather than a prop update. It is a latent bug held shut by a
`key`, and it would surface the moment symbols became dynamic.

**Reconnect refits the viewport.** `onBackfill` calls `fitContent()`, so a reconnection discards
the user's zoom and pan. That is defensible — the series has been replaced wholesale — but it is
not obviously the desired behaviour for a brief drop.

**Session statistics are per-connection.** High, low, and change-since-first all reset on
reconnect, because `firstOpen` is re-seeded from the new backfill. A dashboard that reconnected
once shows a "session" high for a shorter session than the user thinks they have been watching.
The outcome tallies deliberately do *not* reset, because they describe the backend's behaviour
rather than the current view.

**No tests.** The invariants here were verified by instrumenting a real browser against the real
pipeline — canvas node identity, ResizeObserver construct/disconnect counts, WebSocket URLs, and
injected backdated bars — rather than by an automated suite. That verification is repeatable but
it is not committed, so nothing catches a regression on the next change.
