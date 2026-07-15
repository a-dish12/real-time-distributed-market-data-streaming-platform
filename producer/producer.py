"""
Stage 1: minimal producer.
Sends a handful of market events into the `market-events` topic.

Run from repo root:  python producer/producer.py
"""

import json
import time
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"   # the door into Kafka from your Mac
TOPIC = "market-events"
SYMBOLS = ["AAPL", "MSFT", "TSLA"]


def make_event(symbol: str, seq: int) -> dict:
    d={
       "symbol":symbol,
        "price":100.0,
        "event_time":time.time(),
        "seq":seq,
        "size":1000

    }
    return d
    

def partition_key(event: dict) -> bytes:
    return event["symbol"].encode("utf-8")
    


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    counters = {s: 0 for s in SYMBOLS}

    for _ in range(10):
        for symbol in SYMBOLS:
            event = make_event(symbol, counters[symbol])
            counters[symbol] += 1

            producer.send(TOPIC, key=partition_key(event), value=event)
            print("sent:", event)

        time.sleep(0.5)

    producer.flush()   # wait for everything to actually reach the broker
    producer.close()
    print("done — 30 events sent")


if __name__ == "__main__":
    main()