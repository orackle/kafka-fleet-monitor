import json
import sys
import time

from confluent_kafka import Producer

import os
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "chaos-commands"

if len(sys.argv) < 3:
    print("usage: python scripts\\inject_chaos.py <node_id> <cpu_spike|mem_leak> [duration_seconds]")
    sys.exit(1)

node_id = sys.argv[1]
chaos_type = sys.argv[2]
duration = int(sys.argv[3]) if len(sys.argv) > 3 else 30

producer = Producer({"bootstrap.servers": BROKER})
command = {"node_id": node_id, "type": chaos_type, "duration_seconds": duration, "issued_at": time.time()}
producer.produce(TOPIC, key=node_id, value=json.dumps(command))
producer.flush()
print("sent chaos command:", command)