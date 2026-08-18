import os
import time
import asyncio
import json
from collections import defaultdict, deque
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from config import KAFKA_BOOTSTRAP, BARS_TOPIC

bars=defaultdict(lambda:deque(maxlen=60))
connections=defaultdict(dict) # symbol:{websocket:state} state:{dumping:True/False,buffer:[]}

# Everything /health reports on. The consumer and its task used to be locals inside lifespan,
# which meant nothing outside that function could see whether the stream was alive. Written by
# lifespan and consume_bars only; the endpoint just reads it.
stream={
    "consumer":None,             # AIOKafkaConsumer, once started
    "task":None,                 # the consume_bars task
    "last_bar_at":None,          # wall clock when a bar last arrived
    "last_bar_window_end":None,  # event time of that bar, for comparison against the above
}


# Where the built dashboard lands (vite.config.ts writes here). Defined up here so /health can
# report on it; the actual mount stays at the bottom of the file, where the ordering matters.
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")


async def consume_bars(consumer):
    async for msg in consumer:
        bar=json.loads(msg.value)
        symbol=bar["symbol"]
        bars[symbol].append(bar)
        stream["last_bar_at"]=time.time()
        stream["last_bar_window_end"]=bar.get("window_end")

        for websocket,state in list(connections[symbol].items()):
            if state["dumping"]:
                state["buffer"].append(bar)
            else:
                try:
                    await websocket.send_json({"type":"live",**bar})
                except Exception:
                    connections[symbol].pop(websocket,None)



@asynccontextmanager
async def lifespan(app:FastAPI):
    consumer=AIOKafkaConsumer(
        BARS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"webserver-{os.getpid()}",
        auto_offset_reset="latest",
    )
    await consumer.start()
    task=asyncio.create_task(consume_bars(consumer))
    stream["consumer"]=consumer
    stream["task"]=task
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await consumer.stop()
    stream["consumer"]=None
    stream["task"]=None



app=FastAPI(lifespan=lifespan)


@app.websocket("/ws/{symbol}")
async def handler(websocket: WebSocket,symbol:str):

    await websocket.accept()
    
    
    state={"buffer":[],
            "dumping":True
        }
    connections[symbol][websocket]=state

    try:
        for bar in list(bars[symbol]):
            await websocket.send_json({"type":"backfill",**bar})

        for bar in state["buffer"]:
            await websocket.send_json({"type":"live",**bar})
        state["buffer"]=[]
        state["dumping"]=False
    
        while True:
            
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        del connections[symbol][websocket]


def _consumer_failure(task):
    """Why the consumer task is not running, or None if it is."""
    if task is None:
        return "consumer task never started"
    if not task.done():
        return None
    if task.cancelled():
        return "consumer task was cancelled"
    exc=task.exception()
    return f"consumer task died: {exc!r}" if exc else "consumer task exited"


@app.get("/health")
async def health():
    """Liveness of the bars stream, which is not the same question as "is the page up".

    A probe against "/" hits the static mount and returns 200 whenever a bundle exists on
    disk — true even with the broker down and no bar arriving for an hour. This reports the
    consumer instead, so the two failure modes are distinguishable.

    Readiness is deliberately "the task is alive and subscribed", NOT "a bar has arrived".
    Bars are sealed a window plus a watermark delay after the events that make them, so on a
    cold start there is a stretch with nothing to show; keying off last_bar_at would report
    unhealthy for that entire first window, every single start, and the container healthcheck
    would act on it. last_bar_at is still reported below — as an observation, not a gate.
    """
    consumer=stream["consumer"]
    task=stream["task"]

    failure=_consumer_failure(task)
    # subscription() is set by consumer.start() and holds regardless of rebalancing, so it does
    # not flap. assignment() can legitimately be empty mid-rebalance, which is why it is
    # reported but not part of the verdict — an empty one that never fills also means the bars
    # topic is missing, now that the broker will not create it on demand.
    subscription=sorted(consumer.subscription() or ()) if consumer is not None else []
    assigned=len(consumer.assignment()) if consumer is not None else 0

    if failure is None and not subscription:
        failure="consumer is not subscribed to any topic"

    ready=failure is None
    body={
        "status":"ready" if ready else "degraded",
        "kafka":{
            "bootstrap":KAFKA_BOOTSTRAP,
            "consumer_task":"running" if task is not None and not task.done() else "stopped",
            "subscribed":subscription,
            "assigned_partitions":assigned,
            "last_bar_at":stream["last_bar_at"],
            "last_bar_window_end":stream["last_bar_window_end"],
        },
        # Reported so a green page with a dead stream is visible as exactly that.
        "static_mounted":os.path.isdir(STATIC_DIR),
    }
    if not ready:
        body["reason"]=failure

    # 503 rather than a 200 carrying {"status": "degraded"}: the container healthcheck decides
    # on the status code alone, and a body nobody parses would make it permanently healthy.
    return JSONResponse(body, status_code=200 if ready else 503)


# Serve the built dashboard. `npm run build` in dashboard/ emits here (see vite.config.ts),
# so the page and its WebSocket share an origin in production.
#
# Mounted last on purpose: Starlette matches routes in registration order and a mount at "/"
# matches everything, so registering it above would shadow /ws/{symbol} and /health. html=True
# serves index.html for "/". The directory check keeps the server startable before the first
# build — and /health above reports that case rather than the server simply 404ing.
if os.path.isdir(STATIC_DIR):
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
else:
    print(f"no build at {STATIC_DIR} — run `npm run build` in dashboard/ to serve the UI")