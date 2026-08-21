import os
import time
import json
from kafka import KafkaConsumer,KafkaProducer
from kafka.errors import BrokerResponseError
from config import KAFKA_BOOTSTRAP, MARKET_EVENTS_TOPIC, BARS_TOPIC



IDLE_THRESHOLD=0.8 # how long to wait before declaring a partition quiet
POLL_TIMEOUT=100  #how often to poll the receiving of messages
BACKSTOP_INTERVAL = 0.2   # run the backstop at most 5x/sec


WINDOW = 1.0      # window width in seconds
DELAY  = 0.2      # 200ms watermark grace

# a stale group id makes the second run of an experiment consume nothing and
# report zero drops for entirely uninteresting reasons, so it is per-run
GROUP_ID = os.environ.get("EXPERIMENT_GROUP_ID", "printer-debug18")
REPORT_PATH = os.environ.get("EXPERIMENT_REPORT")

windows={}      #partition-> {(symbol,window):acc}
watermarks={}   #partition-> float
# watermark=float("-inf")

last_activity={} # partition: time

# ---------------------------------------------------- experiment instrumentation
# the shared-watermark experiment diffs two report files directly, so everything
# below has to stay identical across both branches. it is written here on main
# and cherry-picked, never retyped
events_consumed = 0

drop_count = 0
drops_by_partition = {}   # partition -> int
drop_audit = []           # one record per dropped event

emitted = []              # one record per emitted bar
bar_send_failures = 0     # bumped by on_send_error, only surfaces during flush

producer=KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def caught_up(consumer,partition):
    """return True if partition has been read to end of log"""
    tp=next((t for t in consumer.assignment() if t.topic==MARKET_EVENTS_TOPIC and t.partition==partition),None)

    if tp is None:
        return False
    end =consumer.end_offsets([tp])[tp]
    return consumer.position(tp)>=end



def deserialize(raw_bytes):
    return json.loads(raw_bytes)

def partition_key(symbol):
    return symbol.encode("utf-8")

def make_bar(key,bar):
    d={
        "symbol":key[0],
        "window_start":key[1],
        "window_end":key[1]+WINDOW,
        "open" : bar["o"],
        "high":bar["h"],
        "low":bar["l"],
        "close":bar["c"],
        "count":bar["count"]
    }
    return d


def on_send_error(exc, symbol, window_start):
    # a bar counted as emitted but never landed on the bars topic would make the
    # conservation check overcount, so permanent failures are counted not just printed
    global bar_send_failures
    bar_send_failures += 1

    print(f"BAR SEND FAILED {symbol} window[{window_start}]: {exc}")
    print()
    if isinstance(exc, BrokerResponseError):
        # BrokerResponseError instances include a standard Kafka error code (errno)
        print(f"Kafka Protocol Error Code (errno): {exc.errno}")
        print(f"Kafka Error Message: {exc.message}")
        print(f"Kafka Root Description: {exc.description}")
    else:
        # Client-side error (like KafkaTimeoutError)
        print(f"Standard Error Message: {str(exc)}")
    print("==========================================================")


def emit(key, bar, partition, seal):
    """seal is one of watermark | backstop | drain, and stays distinguishable in
    the report: a drain bar means the consumer was stopped before the backstop
    had finished, which invalidates the run rather than being noise"""
    d=make_bar(key,bar)
    symbol, w_start = key

    emitted.append({
        "symbol": symbol,
        "window_start": w_start,
        "partition": partition,
        "count": bar["count"],
        "seal": seal,
    })

    print(f"SEAL[{seal}] p{partition} {symbol} window[{w_start}] "
        f"O {bar['o']} H {bar['h']} L {bar['l']} C {bar['c']} N {bar['count']}")
    producer.send(
        topic=BARS_TOPIC,
        key=partition_key(symbol),
        value=d
    ).add_errback(
        on_send_error, symbol=symbol,window_start=w_start
    )


def sweep(partition, seal="watermark"):
    watermark=watermarks[partition]
    for key in list(windows.setdefault(partition,{})):
        _,w_start=key
        w_end=w_start+WINDOW
        if watermark>=w_end:
            bar=windows[partition].pop(key)
            emit(key,bar,partition,seal)


def handle_tick(event,partition):
    global drop_count

    windows.setdefault(partition, {})

    symbol,price,event_time=event["symbol"],event["price"],event["event_time"]

    window_start=int(event_time//WINDOW)* WINDOW
    window_end=window_start+WINDOW

    
    watermark=max(watermarks.get(partition,float("-inf")),event_time-DELAY)  
    watermarks[partition]=watermark

    sweep(partition)
    

    key=(symbol,window_start)
    if watermark>=window_end:
        print(f"DROP {symbol} @ {event_time} (late, window[{window_start}] sealed)")

        # audit trail. the per-partition breakdown is what makes "only partitions
        # 0 and 1 lost data" checkable rather than asserted
        drop_count += 1
        drops_by_partition[partition] = drops_by_partition.get(partition, 0) + 1
        drop_audit.append({
            "symbol": symbol,
            "partition": partition,
            "event_time": event_time,
            "window_start": window_start,
            "watermark": watermark,
        })
    elif key in windows[partition]:
        acc = windows[partition][key]
        acc["h"] = max(acc["h"], price)
        acc["l"] = min(acc["l"], price)
        acc["c"] = price
        acc["count"]+=1
    else:
        windows[partition][key]={"o":price,"h":price,"l":price,"c":price,"count":1}


def build_report():
    """conservation check plus the per-partition breakdown the experiment turns on"""
    represented = sum(b["count"] for b in emitted)
    unaccounted = events_consumed - represented - drop_count

    per_partition = {}
    for p in sorted(set(list(drops_by_partition) + [b["partition"] for b in emitted])):
        bars_p = [b for b in emitted if b["partition"] == p]
        seals_p = {}
        for b in bars_p:
            seals_p[b["seal"]] = seals_p.get(b["seal"], 0) + 1
        per_partition[str(p)] = {
            "drops": drops_by_partition.get(p, 0),
            "distinct_bars": len(bars_p),
            "events_represented": sum(b["count"] for b in bars_p),
            "seal_categories": seals_p,
        }

    seal_totals = {}
    for b in emitted:
        seal_totals[b["seal"]] = seal_totals.get(b["seal"], 0) + 1

    return {
        "group_id": GROUP_ID,
        "window": WINDOW,
        "delay": DELAY,
        "events_consumed": events_consumed,
        "events_represented_in_bars": represented,
        "events_dropped": drop_count,
        "unaccounted": unaccounted,
        "bar_send_failures": bar_send_failures,
        "distinct_bars_emitted": len(emitted),
        "seal_categories": seal_totals,
        "drops_by_partition": {str(p): n for p, n in sorted(drops_by_partition.items())},
        "per_partition": per_partition,
        "emitted_bars": sorted(
            ([b["symbol"], b["window_start"], b["partition"], b["count"], b["seal"]] for b in emitted),
            key=lambda r: (r[0], r[1]),
        ),
        "emitted_keys": sorted({(b["symbol"], b["window_start"]) for b in emitted}),
        "drop_audit": drop_audit,
    }


def write_report(report):
    print()
    print("================ experiment report ================")
    print(f"events consumed:            {report['events_consumed']}")
    print(f"events represented in bars: {report['events_represented_in_bars']}")
    print(f"events dropped:             {report['events_dropped']}")
    print(f"unaccounted:                {report['unaccounted']}      <- must be 0")
    print(f"bar send failures:          {report['bar_send_failures']}      <- must be 0")
    print(f"distinct bars emitted:      {report['distinct_bars_emitted']}")
    print(f"seal categories:            {report['seal_categories']}")
    print(f"drops by partition:         {report['drops_by_partition']}")

    if report["unaccounted"] != 0:
        print("!! UNACCOUNTED IS NON-ZERO: there is a loss path no counter catches.")
        print("!! this run is INVALID until it is found.")
    if report["bar_send_failures"] != 0:
        print("!! BAR SEND FAILURES: bars counted as emitted never reached the topic.")
        print("!! this run is INVALID.")
    if report["seal_categories"].get("drain", 0) != 0:
        print("!! DRAIN BARS PRESENT: the consumer was stopped before the idle")
        print("!! backstop had finished. rerun and wait longer.")
    print("===================================================")

    if REPORT_PATH:
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, sort_keys=False)
        print(f"report written to {REPORT_PATH}")
    else:
        print("EXPERIMENT_REPORT unset, report not written to a file")


def main():
    global events_consumed

    consumer = KafkaConsumer(
        MARKET_EVENTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=deserialize,   # Kafka calls function on each message's bytes
    )
    last_backstop=0.0

    print(f"consuming from '{MARKET_EVENTS_TOPIC}' as group '{GROUP_ID}' (earliest)... Ctrl-C to stop")
    try:


        while True:
            batch=consumer.poll(timeout_ms=POLL_TIMEOUT)
            for tp, messages in batch.items():
                for message in messages:
                    event = message.value          # already a dict — deserialize ran on it
                    partition = message.partition  # Kafka metadata: which partition this came from
                    offset = message.offset        # Kafka metadata: position in that partition
                    last_activity[partition]=time.monotonic()
                    events_consumed += 1

                
                    print(f"Partition: {partition}, symbol: {event['symbol']}, seq:{event['seq']}, offset:{offset}")
                    handle_tick(event,partition)

            now = time.monotonic()
            if now - last_backstop >= BACKSTOP_INTERVAL:
                last_backstop = now
                for p in list(windows.keys()):
                    if not windows[p]:
                        continue
                    if (now - last_activity.get(p, now)) < IDLE_THRESHOLD:
                        continue
                    if not caught_up(consumer, p):
                        continue
                    target = max(w_start for (sym, w_start) in windows[p]) + WINDOW
                    watermarks[p] = max(watermarks[p], target)
                    sweep(p, "backstop")

    except KeyboardInterrupt:
        # unconditional drain: every still-open window is emitted regardless of
        # any watermark. deliberately NOT done by pushing the watermark forward,
        # which on the experiment branch would mutate the variable under test
        # during teardown
        for p in list(windows):
            for key in list(windows[p]):
                emit(key, windows[p].pop(key), p, "drain")

        # send failures only surface through on_send_error during flush, so the
        # report has to be built after this flush even though the drain is before it
        producer.flush()
        write_report(build_report())
    finally:
        # in a finally rather than the except so a KeyError in the backstop or a
        # deserialization failure still flushes in-flight bars and leaves the
        # consumer group cleanly
        producer.flush()
        producer.close()
        consumer.close()

if __name__ == "__main__":
    main()
