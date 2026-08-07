# The async runtime

Why the web service runs on an event loop, why `aiokafka` rather than a thread bridging
`kafka-python`, and what that choice costs and buys elsewhere in the file.

## The problem

Every process upstream does one thing at a time. The producer builds an event and sends it.
The aggregator polls, buckets, and publishes. When the aggregator sits inside
`poll(timeout_ms=100)` with nothing on the broker, the process is genuinely idle — there is
no other work waiting — so blocking there costs nothing.

The web service is the first process that must wait on several things at once:

```
  read the bars topic, forever
  hold open a WebSocket to browser A          all of these,
  hold open a WebSocket to browser B          simultaneously,
  accept browser C, connecting now            in one process
  push each sealed bar to whoever wants it
```

None of these has a turn. A bar can seal at any moment; a browser can connect or vanish at
any moment. The design question is how to wait on all of them without one blocking the rest.

## The rejected option: a thread and a queue

The obvious approach keeps the existing client: run `kafka-python`'s blocking `poll()` on a
background thread, have it drop decoded bars into a `queue.Queue`, and have the async web
server drain that queue.

```
  ┌──────────────┐   blocking poll()   ┌────────────┐
  │ Kafka thread │ ──────────────────▶ │   Queue    │
  └──────────────┘                     └─────┬──────┘
                                             │ drained by
                                       ┌─────▼──────┐
                                       │ event loop │ ──▶ browsers
                                       └────────────┘
```

This works. It was rejected because two threads means `bars` and `connections` are touched
from two threads, and every append and every iteration of shared state then needs a lock. The
bug class that buys is the invisible kind: correct on a laptop, corrupt under load, and
reproducible only by luck.

**Accepted cost:** a second Kafka client library in the dependency tree, and a file that
cannot borrow patterns from `consumer.py`. **Bought:** one thread, therefore no locks, and a
Kafka consumer that is simply another task rather than a foreign thing needing a bridge.

## The model

The event loop holds a list of tasks that are **ready to run**. It picks one and runs it
until that task hits an `await` on something that is not ready — a socket with no bytes on
it, a send that cannot complete. At that point the task registers on the relevant file
descriptor, leaves the ready list, and the loop picks the next ready task.

Nothing runs in parallel, ever. One thing runs at a time. The trick is not parallelism but
that **waiting does not occupy the worker**: a parked task costs a row in a table saying
"wake this one when fd 7 is readable." The wake-up service is the OS — `epoll` on Linux,
`kqueue` on macOS — and the broker socket and the browser sockets are all just descriptors in
one list. The loop neither knows nor cares which is which.

This is the process scheduler in miniature: **running / runnable / blocked**, with the event
loop in the kernel's role.

### A trace

Two browsers connected, both past their backfill:

```
ready = [consume_bars, handler_A, handler_B]

  run consume_bars → async for, no bytes on broker socket
                   → register fd(kafka), suspend        ready = [handler_A, handler_B]
  run handler_A    → await receive_text(), nothing sent
                   → register fd(A), suspend            ready = [handler_B]
  run handler_B    → same                               ready = []

  ready list empty → epoll sleep. 0% CPU.
  Three things in progress. Nothing running.

─── a bar arrives on the Kafka socket ───

  kernel: fd(kafka) readable
  loop:   ready = [consume_bars]

  run consume_bars → resumes, decodes, appends to the deque
                   → walks connections["AAPL"]:
                        A: await send_json(...)   ─┐ into the kernel send buffer,
                        B: await send_json(...)   ─┘ returns without suspending
                   → back to async for, suspend         ready = []
```

`consume_bars` and the handlers are never introduced to each other. There is no queue, no
bridge, no handoff — `consume_bars` reaches into `connections` and calls `send_json` directly,
because they share a thread and there is no boundary to cross.

## The rule this imposes

**Any call that waits without yielding freezes the whole server.**

One thread. If `consume_bars` called `time.sleep(2)`, `requests.get(...)`, or
`kafka_python_consumer.poll(timeout_ms=100)`, the loop would never get control back. For that
whole duration nobody is served: a pending message sits unsent, an incoming connection sits
unaccepted. The task did not register on a socket and step aside; it stopped, holding the only
worker.

This is why `kafka-python` cannot appear in this file. Not slower, not less elegant — its
`poll()` is a blocking call, and a blocking call inside an event loop is a stalled server.

The failure would also have been invisible in testing. With one browser connected and bars
sealing once a second, nothing is ever queued behind the stall and a 100ms block finishes long
before the next bar. The chart would have looked correct. Blocking calls in an event loop do
not fail; they degrade in proportion to load, on the machine you do not test on.

Note that none of this is a claim that async is *faster*. Every `await` adds overhead. The win
is that an idle connection costs a table entry rather than a thread stack.

## What a suspended send actually means

`await ws.send_json({...})` does not put bytes on the network. It hands bytes to the kernel.

Every TCP socket has a send buffer inside the kernel — a fixed chunk of memory, typically a
few hundred KB. The process writes into it and returns. The kernel then packetizes what is
there, puts it on the wire, waits for the peer's TCP layer to acknowledge, and only then frees
that space.

```
  your code ──write──▶ [ kernel send buffer ] ──drain──▶ network ──▶ browser
                              ↑                    ↑
                         fills at your        empties at the
                         write speed          link's speed
```

Normally the drain outruns the writes and the buffer is invisible: `send_json` copies a few
hundred bytes and returns, and the `await` completes without suspending at all. The buffer
fills when the drain side falls behind — a backgrounded tab not reading its socket, a phone on
bad wifi, bars pushed faster than the link carries them.

A plain blocking socket would stall the thread there. asyncio sets its sockets **non-blocking**,
so the kernel instead returns immediately with "no room right now," and asyncio converts that
into the familiar move: register on the descriptor for *writable*, suspend, resume when space
appears. Identical mechanism to `async for` suspending on the broker socket; the only
difference is direction.

The consequence that matters: **one slow browser cannot slow the others.** `consume_bars`
steps off the ready list while that buffer drains, the loop serves everyone else, and it
resumes the instant space appears.

## `await` versus `create_task`

Calling an `async def` does not run it. It builds a coroutine object — the function's code
plus a slot for where it is paused and what its locals are — inert until something drives it.
So there are two separate acts: create the coroutine, and get it running.

|  | thread released? | does the caller continue? |
|---|---|---|
| `await coro` | yes, if `coro` does I/O | **no** — parked until `coro` returns |
| `create_task(coro)` | no | **yes** — immediately, on the next line |

`await X` means *drive X until it produces a result and give me that result; I cannot continue
until then*. If X suspends on I/O, the suspension propagates to the loop and other tasks run —
so the thread is released. But the caller is still parked at that line. Releasing the thread is
about not wasting CPU; it is not about the caller making progress.

This is exactly why `consume_bars` is scheduled rather than awaited. Written as
`await consume_bars(consumer)`, the loop stays healthy — the task suspends on the broker socket,
other work runs, bars are processed — but `lifespan` never continues, because `consume_bars`
has no `return`. The next line, `yield`, is never reached, FastAPI never learns startup
finished, and uvicorn never serves.

Precisely: `lifespan` is not repeatedly passed over by a busy loop. It is not on the ready list
at all, parked with the condition "resume when `consume_bars` returns," which never becomes
true. The observable symptom is a process that logs its Kafka connection and then prints
nothing — no error, no traceback, no *Application startup complete* — sitting at zero CPU
refusing connections.

**The test before writing `await`: does this ever return?** If it returns, await it
(`consumer.start()`, `websocket.accept()`, `websocket.send_json()`). If it never returns,
schedule it (`consume_bars`).

## What `poll` was doing, and why there is none here

`kafka-python`'s `poll()` bundles two things. A **fetcher** keeps a connection to the broker,
issues fetch requests, and lands returned bytes in an in-memory buffer grouped by partition. A
**handoff** returns whatever is in that buffer right now and advances the position. There is
also housekeeping in there — heartbeats to the group coordinator, rebalance handling, metadata
refresh — which is why a consumer that stops polling is eventually declared dead.

`timeout_ms` governs only the handoff: if the buffer is empty, wait up to that long, then
return empty. It is not "wait, then check" — data available returns immediately.

The aggregator needs that timeout because its seal check must run whether or not messages
arrived; see [`idle-partitions.md`](idle-partitions.md). `aiokafka` runs the identical fetch
machinery, but on an empty buffer it registers on the broker socket and hands the thread back
rather than parking it. No timeout appears in `webserver.py` because a timeout exists to
periodically reclaim a thread you would otherwise surrender, and the async version never
surrenders it.

The web service also has no periodic work at all — it is purely reactive, bar in, fan out — so
there is no backstop to schedule, and the shape collapses from a `while True` with a timeout to
a single `async for`.

## Startup and shutdown ordering

```python
await consumer.start()                              # 1
task = asyncio.create_task(consume_bars(consumer))  # 2
yield                                               # 3
task.cancel()                                       # 4
await task                                          # 5  (CancelledError absorbed)
await consumer.stop()                               # 6
```

`start()` cannot run at import time: there is no event loop yet, and `await` outside an
`async def` is a syntax error. That is the entire reason a lifespan exists. What it does is all
network round trips — open connections, fetch cluster metadata to learn which broker leads
which partition, wait out topic autocreation, join the group, receive assignments — which is
also why it is not in the constructor.

The ordering is not stylistic. Swap 1 and 2 and `consume_bars` gets an unstarted consumer.
Move `yield` above 2 and the server accepts browsers before anything is reading Kafka —
connections that register, backfill from empty deques, and never receive a live bar.

The task handle is held for two reasons: `cancel()` needs it, and **the loop holds only a weak
reference to tasks**. A task nobody references can be garbage-collected mid-run, and the feed
would stop at a nondeterministic moment with no error.

Shutdown reads oddly until the distinction lands: **`cancel()` is a request, `await task` is
the confirmation.** `cancel()` marks the task and arranges for `CancelledError` to be raised
inside it at its current suspension point, then returns. Nothing has happened yet —
`consume_bars` cannot run to receive that exception while `lifespan` holds the thread.
`await task` releases the thread, letting `consume_bars` resume into its cancellation, unwind
through any `finally` blocks, and reach a final state; only then does `lifespan` continue.
Awaiting a cancelled task re-raises `CancelledError` at the awaiting site — asyncio reporting
*how* it ended, not a failure — so it is caught and ignored.

Without step 5, `consume_bars` still runs: `await consumer.stop()` releases the thread just as
well. The bug is that it then unwinds **concurrently with** the consumer it is reading being
closed. Intermittent, usually harmless, occasionally noisy on shutdown — the same character as
the backfill race in [`websocket-fanout.md`](websocket-fanout.md).

`bars` and `connections` are deliberately not cleared on shutdown. Process exit reclaims the
address space; shutdown is for state that outlives the process. This service holds exactly one
such thing — its Kafka group membership on the broker — and `stop()` releases it, so the
coordinator drops the member immediately rather than waiting out a session timeout.