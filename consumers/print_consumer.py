import json

from confluent_kafka import Consumer

import os
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "system-metrics"
GROUP_ID = "metrics-printer"

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
})

consumer.subscribe([TOPIC])

print(f"consuming '{TOPIC}' as group '{GROUP_ID}'... Ctrl+C to stop")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("consumer error:", msg.error())
            continue
        data = json.loads(msg.value())
        print(f"[partition {msg.partition()}] {data}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()