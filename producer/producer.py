"""
Stage 1: minimal producer.
Sends a handful of market events into the `market-events` topic.

Run from repo root:  python producer/producer.py
"""
import random
import json
import time
from kafka import KafkaProducer

BOOTSTRAP = "localhost:29092"   # the door into Kafka from your Mac
TOPIC = "market-events"
SYMBOLS = ["AAPL", "MSFT", "TSLA","NVDA","INTC"]



def make_event(symbol: str, seq: int,price:int) -> dict:
    
    d={
       "symbol":symbol,
        "price":price,
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
    prices={s:100 for s in SYMBOLS}

    try:
        while True:
            for symbol in SYMBOLS:
                prices[symbol]+=random.gauss(0,0.05)
                event = make_event(symbol, counters[symbol],prices[symbol])
                counters[symbol] += 1

                producer.send(TOPIC, key=partition_key(event), value=event)
                print("sent:", event)

            time.sleep(0.5)

            producer.flush()   # wait for everything to actually reach the broker
    except KeyboardInterrupt:
            producer.close()
    


if __name__ == "__main__":
    main()