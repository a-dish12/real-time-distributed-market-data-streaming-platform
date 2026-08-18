import os

KAFKA_BOOTSTRAP=os.environ.get("KAFKA_BOOTSTRAP","localhost:29092")
MARKET_EVENTS_TOPIC = os.environ.get("MARKET_EVENTS_TOPIC", "market-events")
BARS_TOPIC = os.environ.get("BARS_TOPIC", "bars")