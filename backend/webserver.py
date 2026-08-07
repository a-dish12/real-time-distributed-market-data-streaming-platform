import os
import asyncio
import json
from collections import defaultdict, deque
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI,WebSocket,WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

BOOTSTRAP="localhost:29092"
TOPIC="bars"

bars=defaultdict(lambda:deque(maxlen=60))
connections=defaultdict(dict) # symbol:{websocket:state} state:{dumping:True/False,buffer:[]}


async def consume_bars(consumer):
    async for msg in consumer:
        bar=json.loads(msg.value)
        symbol=bar["symbol"]
        bars[symbol].append(bar)

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
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=f"webserver-{os.getpid()}",
        auto_offset_reset="latest",
    )
    await consumer.start()
    task=asyncio.create_task(consume_bars(consumer))
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await consumer.stop()



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