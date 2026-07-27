import json
from kafka import KafkaConsumer

TOPIC = "market-events"
BOOTSTRAP = "localhost:29092"


WINDOW = 1.0      # window width in seconds
DELAY  = 0.2      # 200ms watermark grace

windows={}      #partition-> {(symbol,window):acc}
watermarks={}   #partition-> float
# watermark=float("-inf")

def deserialize(raw_bytes):
    return json.loads(raw_bytes)

def emit(key, bar):
    symbol, w_start = key
    print(f"SEAL {symbol} window[{w_start}] "
        f"O {bar['o']} H {bar['h']} L {bar['l']} C {bar['c']}")

def handle_tick(event,partition):
    symbol,price,event_time=event["symbol"],event["price"],event["event_time"]

    window_start=int(event_time//WINDOW)* WINDOW
    window_end=window_start+WINDOW

    # watermark=max(watermark,event_time-DELAY)
 
    watermark=max(watermarks.get(partition,float("-inf")),event_time-DELAY)  
    watermarks[partition]=watermark

    for key in list(windows.setdefault(partition,{})):
        _,w_start=key
        w_end=w_start+WINDOW
        if watermark>=w_end:
            bar=windows[partition].pop(key)
            emit(key,bar)
    

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
        group_id="printer-debug2",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,   # Kafka calls your function on each message's bytes
    )

    print(f"consuming from '{TOPIC}' as group 'printer' (earliest)... Ctrl-C to stop")
    try:
        for message in consumer:
            event = message.value          # already a dict — deserialize ran on it
            partition = message.partition  # Kafka metadata: which partition this came from
            offset = message.offset        # Kafka metadata: position in that partition

        
            print(f"Partition: {partition}, symbol: {event['symbol']}, seq:{event['seq']}, offset:{offset}")
            handle_tick(event,partition)
            
    finally:
        consumer.close()

if __name__ == "__main__":
    main()