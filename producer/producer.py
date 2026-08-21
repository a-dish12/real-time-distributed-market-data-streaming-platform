"""
Stage 1: minimal producer.
Sends a handful of market events into the `market-events` topic.

Run from repo root:  python producer/producer.py            live generation
                     python producer/producer.py --replay   skew workload replay
"""
import argparse
import csv
import os
import random
import json
import time
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP, MARKET_EVENTS_TOPIC


SYMBOLS = ["AAPL", "MSFT", "TSLA","NVDA","INTC"]

WORKLOAD_CSV = os.environ.get("WORKLOAD_CSV", "csvs/skew_workload.csv")
PHASE_GAP = float(os.environ.get("PHASE_GAP", "2.0"))



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
    


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def run_live() -> None:
    producer = make_producer()

    counters = {s: 0 for s in SYMBOLS}
    prices={s:100 for s in SYMBOLS}

    try:
        while True:
            for symbol in SYMBOLS:
                prices[symbol]+=random.gauss(0,0.05)
                event = make_event(symbol, counters[symbol],prices[symbol])
                counters[symbol] += 1

                producer.send(MARKET_EVENTS_TOPIC, key=partition_key(event), value=event)
                print("sent:", event)

            time.sleep(0.5)

            producer.flush()   # wait for everything to actually reach the broker
    except KeyboardInterrupt:
            producer.close()


def replay_row(row: dict) -> dict:
    """event_time comes from the csv, not time.time(), or arrival order and
    event-time order agree and no drop can happen"""
    return {
        "symbol": row["symbol"],
        "price": float(row["price"]),
        "event_time": float(row["event_time"]),
        "seq": int(row["seq"]),
        "size": int(row["size"]),
    }


def run_replay(path: str) -> None:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))

    # csv order kept within each phase, the file is sorted so each partition's
    # stream is monotone in event time
    phase1 = [r for r in rows if int(r["phase"]) == 1]
    phase2 = [r for r in rows if int(r["phase"]) == 2]

    producer = make_producer()
    try:
        for r in phase1:
            event = replay_row(r)
            producer.send(MARKET_EVENTS_TOPIC, key=partition_key(event), value=event)
        # phase 1 has to be on the broker before phase 2 is queued or they interleave
        producer.flush()
        print(f"phase 1: {len(phase1)} rows sent")

        time.sleep(PHASE_GAP)

        for r in phase2:
            event = replay_row(r)
            producer.send(MARKET_EVENTS_TOPIC, key=partition_key(event), value=event)
        producer.flush()
        print(f"phase 2: {len(phase2)} rows sent")
    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--replay",
        action="store_true",
        help="replay the skew workload CSV instead of generating events live",
    )
    parser.add_argument("--csv", default=WORKLOAD_CSV, help="workload CSV for --replay")
    args = parser.parse_args()

    if args.replay:
        run_replay(args.csv)
    else:
        run_live()


if __name__ == "__main__":
    main()
