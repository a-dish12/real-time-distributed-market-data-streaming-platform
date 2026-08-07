# WebSocket fan-out

How sealed candles reach a browser: connection state, the backfill race and its fix, and what
happens when a client disappears at an inconvenient moment.

The runtime this all sits on — the event loop, why `aiokafka`, `await` versus `create_task` —
is in [`async-runtime.md`](async-runtime.md).

## Shape

```
  bars topic ──▶ consume_bars ──┬──▶ bars[symbol]  (deque, maxlen 60)   ← history
                 (one task)     │
                                └──▶ connections[symbol][ws]            ← live fan-out
                                        │
   /ws/AAPL ──▶ handler (one task per connection)
                        reads bars[symbol] on connect  ────▶ browser
```

One route, `/ws/{symbol}`, one chart per symbol, one connection per chart. One background task
reads Kafka for the whole process; one handler task per open browser.

## Why the route carries a symbol

`/ws/AAPL` is a promise: this connection is the AAPL feed. Broadcasting every symbol down every
connection would push routing onto the client, which the server was already holding the
information to do; it would make backfill and live disagree, since the handler already sends
only `bars[symbol]` as history; and it would multiply both traffic and `send_json` calls by the
symbol count.

The alternative — one connection carrying everything, client demultiplexes — is legitimate and
real systems do it. It is a *different* design: route `/ws` with no parameter, backfill sends
every deque. Not adopted, because one chart per symbol makes the symbol the natural unit.

## State

```python
bars        = defaultdict(lambda: deque(maxlen=60))   # symbol -> rolling window
connections = defaultdict(dict)                       # symbol -> {websocket: state}
                                                      # state  -> {"buffer": [], "dumping": bool}
```

**Keyed by symbol** so `consume_bars` goes straight to the connections that want a bar rather
than scanning all of them and testing.

**Inner container is a dict, not a list**, because two tabs can watch the same symbol and each
needs its own state. Keyed by the websocket object — hashable by identity, so no ID scheme is
needed — registration and removal are both O(1), and iterating yields the recipient and its
state together. A list of `(websocket, state)` tuples would turn every disconnect into a scan.

**No locks**, because only the event loop's thread ever touches these. That is a direct
consequence of the `aiokafka` decision rather than an omission.

### Why the phase flag is per connection

This is the central decision of the stage. Ten browsers can connect at different moments, each
independently mid-backfill or not, so the flag describes one connection's phase and lives with
that connection.

With a single module-level `dumping`, traced:

```
  t0  A connects to /ws/AAPL, starts sending 60 backfill bars, dumping = True
  t1  B connects to /ws/MSFT, backfills, finishes, dumping = False
  t2  A is still on bar 30 of 60 — but the flag now says "live"
```

A receives:

```
  backfill 1 … 30
  live 61            ← spliced in mid-loop
  backfill 31 … 60
```

Bars 31–60 are not lost; A's handler keeps sending them. The failure is **ordering**. The chart
receives a bar from the future and then continues receiving history behind it, and Lightweight
Charts will either reject the out-of-order timestamps or render a mangled series.

Two properties worth naming. **The bug is silent for the browser that caused it** — B did
nothing wrong and sees nothing wrong; all the damage lands on A, which was mid-flight when B
wrote to shared state. And it is **the same shape as the global watermark bug** of Stage 3.5:
one partition draining fast dragged the shared watermark past a slow partition's data; one
connection finishing backfill drags the shared flag past a slow connection's backfill. Both are
state that describes one member of a set, stored once for the set.

The flag starts `True`. `False` means "caught up, send live bars directly," and a connection
that has just registered has sent zero history — starting at `False` would inflict the failure
above on itself. It earns its way to `False` only after both the history and the drain are
done. Read the name as a phase label: *currently dumping history to this browser*.

### Why history comes from memory, not the log

`bars` has one partition carrying every symbol interleaved, so reading back N messages from the
end of the log gives bulk-recent, not per-symbol-recent — sixty messages might contain four
AAPL bars. The deque gives per-symbol-recent by construction, and `maxlen=60` makes it
self-trimming: append the sixty-first and the first falls off. No eviction logic.

The trade is that history lives and dies with the process. A server restart empties the deques,
and with `auto_offset_reset="latest"` a browser connecting immediately afterwards sees a chart
that grows from nothing over the following minute.

## The backfill race

Between reading the deque and beginning to receive live bars there is a window in which a bar
can seal and belong to neither. It is intermittent rather than reliable — on localhost the
history sends in milliseconds, so most connections never hit it — which is exactly what makes
it worth designing out rather than discovering later.

**Register on connect, buffer during backfill, drain, then flip.**

```
  handler                                   consume_bars
  ───────                                   ────────────
  accept()
  state = {buffer: [], dumping: True}
  connections[sym][ws] = state   ◀────────  now visible to the fan-out
  │
  │ for bar in list(bars[sym]):
  │     await send_json(backfill)  ─────▶   a bar seals here:
  │                                          sees dumping True
  │                                  ◀────   appends to state["buffer"]
  │
  │ for bar in state["buffer"]:
  │     await send_json(live)                a bar sealing here also lands
  │ state["buffer"] = []                     in the buffer — and the loop is
  │ state["dumping"] = False                 still iterating it, so it is picked up
  │
  └ while True: await receive_text()  ◀───   subsequent bars sent directly
```

**Registration precedes the history read.** Register first and a bar sealing mid-backfill lands
in that connection's buffer; register after and the same bar has nowhere to go, because
`consume_bars` does not know the connection exists. This single ordering is the fix.

**The drain needs no coordination.** It is the next statement after the history loop, in the
same function; Python does not reach it until the last history bar has been sent. Nothing polls
and nothing waits on anything.

**The flag is flipped by position, not by testing whether the buffer is empty.** An empty buffer
means two opposite things — backfill not started, and backfill finished — which need opposite
handling, and the difference is not recoverable from the data.

**Nothing can slip between the drain and the flip.** Between one `await` and the next, code runs
uninterrupted; the loop cannot switch tasks at a non-`await` point. There is no `await` between
`state["buffer"] = []` and `state["dumping"] = False`, so no bar can arrive "during" them.

**A bar sealing during the drain closes itself out.** The flag is still `True`, so it is
appended to the buffer — and the drain loop is still iterating that same buffer, so it is picked
up on the next pass. This is safe specifically because appending does not shift elements the
loop has not reached: a `for` over a list is index-based and re-checks length each step, so a
longer list simply runs one more iteration. Removing from the front would shift everything down
and silently skip elements, with no exception raised.

**The state dict is shared by reference, and that is load-bearing.** The handler's local `state`
and `connections[symbol][websocket]` are the same object, so an append made by `consume_bars` is
visible to the drain loop. Were it a copy, the buffer would be unreachable from the fan-out and
the whole scheme collapses.

### Message tagging

| sent by | source | tag |
|---|---|---|
| handler, history loop | `list(bars[symbol])` | `backfill` |
| handler, drain loop | `state["buffer"]` | `live` |
| `consume_bars` | the bar just received | `live` |

Buffered bars are tagged `live` because they *are* live data — bars that sealed while the
browser was connected, indistinguishable from ones arriving a second later. The tag describes
the kind of data, not the code path that sent it. Tagging happens on drain rather than on
buffer, so one place is responsible for the field.

**The tag is not a claim about recency.** `live` means "arrived after your backfill," not "just
happened." If the aggregator restarts with a backlog it will seal bars carrying old event-times
within a second or two of wall clock, and those reach connected clients tagged `live`. Only
`window_start` says when a bar belongs — a frontend that appends by arrival order rather than by
timestamp will draw nonsense during a catch-up burst.

This shape — replay a snapshot, buffer the live stream, drain, switch over — is the same one
used by database replication bootstraps, the Kubernetes watch API, and Debezium's initial
snapshot. The general problem is a consistent snapshot plus a live feed with a seam between
them.

## Snapshots

`list(...)` appears twice, both times around a container that another task can mutate at an
`await`.

A dict's `.items()` builds nothing — it is a **view**, a thin object holding a reference back to
the dict and fetching entries on demand. The dict carries a version counter that ticks on every
structural change; that counter belongs to the dict and would exist with no view at all. The
view records its value when iteration starts and re-reads it each step. Same value, carry on;
different value, `RuntimeError`.

It is a **detector, not a lock**: it does not prevent the mutation, coordinate, or wait. The
mutation succeeds instantly and the view finds out afterwards. It exists as a guard rail against
a silent bug, because the alternative is skipped or repeated entries with no error. Only
*structural* changes count — adding or removing a key. Changing a value at an existing key does
not, which is why `state["dumping"] = False` is safe while `consume_bars` is iterating.

**`list(connections[symbol].items())` in `consume_bars`.** Three tabs on AAPL; a bar seals and
the fan-out suspends at `send_json` to the first. The second tab's browser closes, its handler
resumes, and its cleanup removes it from the dict. The fan-out resumes and asks the view for the
next entry: `RuntimeError: dictionary changed size during iteration`, the task dies, and **the
whole feed stops for every browser, permanently, because one tab closed.**

**`list(bars[symbol])` in the handler.** Same class — iterating the deque directly while
`consume_bars` appends raises `deque mutated during iteration`.

The cost is that the snapshot is **stale by design**: it froze membership when it was taken.
Freezing is what prevents the crash and what creates the staleness — the same property from both
sides — which leads directly to the next section.

## Failure handling

### Registration and cleanup must be paired

`finally` runs on every exit path from a `try`: normal completion, a caught exception, an
uncaught one propagating, even a `return` in the middle.

```python
connections[symbol][websocket] = state     # registration OUTSIDE the try
try:
    # history, drain, flip, receive loop
except WebSocketDisconnect:
    pass                                    # a browser leaving is normal, not an error
finally:
    del connections[symbol][websocket]      # guaranteed
```

Wrapping only the receive loop leaks. A browser closing *during* the history send raises there,
the handler dies, the cleanup never runs, and the entry stays in `connections` forever with the
fan-out sending to it on every bar.

Registration sits above the `try` so the `finally` only runs when there is genuinely something
to remove. `except WebSocketDisconnect: pass` keeps tracebacks out of the logs for ordinary tab
closes; anything else still propagates *after* the cleanup, so a real bug is not swallowed.

### Sending to a socket that closed mid-iteration

The handler's `finally` does nothing for a different task. `consume_bars` still holds a stale
snapshot listing a connection that has since been removed, and sending on a dead socket raises —
with no `try` in `consume_bars`, that kills the fan-out for everyone.

```python
try:
    await websocket.send_json({"type": "live", **bar})
except Exception:
    connections[symbol].pop(websocket, None)
```

**Bare `except Exception` is defensible here** not because the exception is known — Starlette's
behaviour varies by connection state — but because the handling is identical for all of them:
any failure to send means this recipient is unreachable. Elsewhere a bare `except Exception`
hides bugs precisely because different failures need different responses.

**`pop(ws, None)` rather than `del`**, because the handler's `finally` may already have removed
the entry, and a `KeyError` raised inside the `except` would kill the task anyway — the exact
outcome being guarded against.

**A pre-check does not work.**

```python
if websocket in connections[symbol]:     # True — still there
    await websocket.send_json(...)       # suspends here; the other handler's finally runs
                                         # resumes, sends to a dead socket
```

The check and the send are separated by an `await`, which is exactly where the other task gets
to run. **Check-then-act does not survive a suspension point**: any fact verified before it may
be false after. The only reliable pattern is act, then handle the failure.

## Kafka configuration

**`group_id=f"webserver-{os.getpid()}"`.** A consumer group *divides* partitions among its
members; fan-out needs every instance to see every symbol, so each instance must be a group of
one. With a hardcoded group id, leaving one server running while `--reload` starts another — or
simply opening a second terminal — makes the coordinator split partitions between them. Neither
instance sees all symbols, a browser on one shows a chart that never updates, and **nothing
errors anywhere.**

The PID is used because the kernel guarantees no two live processes share one; the prefix is
there because group ids appear in `kafka-consumer-groups --list` and in the Kafka UI, where a
bare number would be unreadable. `uuid.uuid4()` is the more common production choice, since PIDs
restart at 1 per container namespace.

**`auto_offset_reset="latest"`.** This is a fallback for a group with **no committed offset**,
not a general startup policy; a group that has committed offsets resumes from them and never
consults it. Because every instance joins under a fresh group id, this branch fires on *every*
start — in most systems it fires roughly never, so it is load-bearing here in a way it usually
is not.

`earliest` would replay the entire retained topic on every boot, appending each bar to a deque
that discards from the front as it goes. The end state is identical — the same last sixty bars —
reached by doing work proportional to *retention* to produce a result bounded at *sixty*. At a
day of retention or a hundred symbols that is a startup stall with the server up and the charts
empty. The honest cost of `latest` is a thin chart for the first minute after a restart, which
`earliest` does not actually fix; it front-loads the same sixty bars.

The general split: **`earliest` for consumers that own state derived from the log** — CDC into a
database, materialised views, search indexes, warehouse loads, stateful stream processors, where
the log is the source of truth and skipping messages silently corrupts the derived state.
**`latest` for live-view consumers** — dashboards, alerting, monitoring, fan-out. This is the
second.

## Verification

Run against a terminal WebSocket client (`python -m websockets ws://localhost:8000/ws/AAPL`),
with the producer and aggregator live.

Predicted before running: roughly ten `backfill` messages when connecting ten seconds after
boot — one-second windows, empty deques at start, `latest` — and sixty for a client connecting
after two minutes, capped by `maxlen`.

Observed: history followed by a clean transition, `window_start` 1786093526 immediately after
1786093525, no gap and no overlap at the seam. **The race is closed.**

Two apparent anomalies were traced and are both correct behaviour.

**A 32-second gap mid-history with a ~2% price discontinuity across it** (≈101.89 before,
≈99.98 after). The producer and aggregator were restarted; the web server was not. The
diagnostic is decisive: had the server restarted, the deque is in-process memory and would have
been wiped, and with `latest` there would be *zero* bars from before the gap. Fifty-odd
survived, so it ran throughout. The price jump is a fresh random walk starting from its seed —
prices do not move 2% in a second, but a new process does.

This is a real property rather than a testing artifact: **the deque has no notion of missing
time.** If the producer dies for an hour, a browser connecting afterwards receives sixty bars
that look contiguous but straddle an hour of silence, and a chart will draw a line across it.
Nothing detects or marks this.

**A single missing one-second window** (…487 then …489), no restart involved. `count` on
surrounding bars is 2, so the producer emits roughly two events per second per symbol with
jitter; occasionally the gap between consecutive events straddles a whole second, no event lands
in that bucket, and no candle exists to emit. Real feeds do the same for illiquid symbols.

Both matter for the dashboard: the series has genuine holes, and a decision is needed on whether
the chart shows a gap, interpolates, or carries the previous close forward.

## Known limitations

**A client that closes only stops itself.** `consume_bars` keeps appending to the deque and
iterates an empty inner dict at no cost, so reconnecting gets a full sixty-bar history
immediately. The process staying up is what makes history available — which is the whole reason
it is served from memory.

**Empty inner dicts are never removed.** After the last client for a symbol leaves,
`connections["AAPL"]` remains as an empty dict. With a fixed symbol set the outer dict is
bounded at three entries, so this is inert rather than a leak. It *would* be a leak with an
unbounded key space — user ids, session tokens, arbitrary path parameters — and the fix belongs
in the handler's `finally`, where the entry was created, not at shutdown.

**No graceful close frames on shutdown.** Process exit closes the TCP sockets and browsers see a
drop. A polite shutdown would send close frames first.

**No `ConnectionManager` abstraction.** `connections` already is the registry; a class would add
encapsulation and testability at the cost of hiding the `dumping` branch — the most interesting
logic in the file — inside a `broadcast` method. Worth revisiting when a second site needs to
remove a connection.