import json
from kafka import KafkaConsumer

TOPIC = "market-events"
BOOTSTRAP = "localhost:29092"


def deserialize(raw_bytes):
    return json.loads(raw_bytes)
    


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="printer-debug1",
        auto_offset_reset="earliest",
        value_deserializer=deserialize,   # Kafka calls your function on each message's bytes
    )

    print(f"consuming from '{TOPIC}' as group 'printer' (earliest)... Ctrl-C to stop")
    for message in consumer:
        event = message.value          # already a dict — deserialize ran on it
        partition = message.partition  # Kafka metadata: which partition this came from
        offset = message.offset        # Kafka metadata: position in that partition

      
        print(f"Partition: {partition}, symbol: {event['symbol']}, seq:{event['seq']}, offset:{offset}")


if __name__ == "__main__":
    main()