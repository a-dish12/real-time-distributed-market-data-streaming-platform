import time
import json
from kafka import KafkaConsumer

TOPIC = "market-events"
BOOTSTRAP = "localhost:29092"

IDLE_THRESHOLD=0.8 # how long to wait before declaring a partition quiet
POLL_TIMEOUT=100  #how often to poll the receiving of messages
BACKSTOP_INTERVAL = 0.2   # run the backstop at most 5x/sec



WINDOW = 1.0      # window width in seconds
DELAY  = 0.2      # 200ms watermark grace

windows={}      #partition-> {(symbol,window):acc}
watermarks={}   #partition-> float
# watermark=float("-inf")

last_activity={} # partition: time

def caught_up(consumer,partition):
    """return True if partition has been read to end of log"""
    tp=next((t for t in consumer.assignment() if t.topic==TOPIC and t.partition==partition),None)

    if tp is None:
        return False
    end =consumer.end_offsets([tp])[tp]
    return consumer.position(tp)>=end



def deserialize(raw_bytes):
    return json.loads(raw_bytes)

def emit(key, bar):
    symbol, w_start = key
    print(f"SEAL {symbol} window[{w_start}] "
        f"O {bar['o']} H {bar['h']} L {bar['l']} C {bar['c']}")


def sweep(partition):
    watermark=watermarks[partition]
    for key in list(windows.setdefault(partition,{})):
        _,w_start=key
        w_end=w_start+WINDOW
        if watermark>=w_end:
            bar=windows[partition].pop(key)
            emit(key,bar)


def handle_tick(event,partition):
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

        #TODO :audit trail
    elif key in windows[partition]:
        acc = windows[partition][key]
        acc["h"] = max(acc["h"], price)
        acc["l"] = min(acc["l"], price)
        acc["c"] = price
    else:
        windows[partition][key]={"o":price,"h":price,"l":price,"c":price}



def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="printer-debug15",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,   # Kafka calls function on each message's bytes
    )
    last_backstop=0.0

    print(f"consuming from '{TOPIC}' as group 'printer' (earliest)... Ctrl-C to stop")
    try:


        while True:
            batch=consumer.poll(timeout_ms=POLL_TIMEOUT)
            for tp, messages in batch.items():
                for message in messages:
                    event = message.value          # already a dict — deserialize ran on it
                    partition = message.partition  # Kafka metadata: which partition this came from
                    offset = message.offset        # Kafka metadata: position in that partition
                    last_activity[partition]=time.monotonic()

                
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
                    # if not caught_up(consumer, p):
                    #     continue
                    target = max(w_start for (sym, w_start) in windows[p]) + WINDOW
                    watermarks[p] = max(watermarks[p], target)
                    sweep(p)

    finally:
        consumer.close()

if __name__ == "__main__":
    main()