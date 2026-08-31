import json
import math
import random
import sys
import threading
import time

from confluent_kafka import Consumer, Producer

import os
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
METRICS_TOPIC = "system-metrics"
CHAOS_TOPIC = "chaos-commands"

NODE_PROFILES = {
    "web":    {"cpu_base": 30, "cpu_amp": 15, "mem_base": 45, "mem_amp": 10},
    "db":     {"cpu_base": 55, "cpu_amp": 10, "mem_base": 70, "mem_amp": 8},
    "cache":  {"cpu_base": 20, "cpu_amp": 8,  "mem_base": 60, "mem_amp": 5},
    "worker": {"cpu_base": 45, "cpu_amp": 25, "mem_base": 40, "mem_amp": 15},
}

NODE_ID = sys.argv[1] if len(sys.argv) > 1 else "web-01"
NODE_TYPE = NODE_ID.split("-")[0]
PROFILE = NODE_PROFILES.get(NODE_TYPE, NODE_PROFILES["web"])

producer = Producer({"bootstrap.servers": BROKER})
active_chaos = {"type": None, "until": 0, "started": 0, "duration": 0}


def chaos_listener():
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"chaos-listener-{NODE_ID}",
        "auto.offset.reset": "latest",
    })
    consumer.subscribe([CHAOS_TOPIC])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        cmd = json.loads(msg.value())
        if cmd.get("node_id") != NODE_ID:
            continue
        now = time.time()
        active_chaos.update({
            "type": cmd["type"],
            "started": now,
            "until": now + cmd.get("duration_seconds", 30),
            "duration": cmd.get("duration_seconds", 30),
        })
        print(f"!! chaos received: {cmd['type']} for {cmd.get('duration_seconds', 30)}s")


threading.Thread(target=chaos_listener, daemon=True).start()


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


start = time.time()
while True:
    elapsed = time.time() - start
    now = time.time()

    cpu = PROFILE["cpu_base"] + PROFILE["cpu_amp"] * math.sin(elapsed / 20) + random.uniform(-5, 5)
    mem = PROFILE["mem_base"] + PROFILE["mem_amp"] * math.sin(elapsed / 35 + 1) + random.uniform(-3, 3)

    if active_chaos["type"] and now < active_chaos["until"]:
        progress = (now - active_chaos["started"]) / active_chaos["duration"]
        if active_chaos["type"] == "cpu_spike":
            cpu += 45
        elif active_chaos["type"] == "mem_leak":
            mem += 40 * min(1.0, progress * 1.5)
    elif active_chaos["type"] and now >= active_chaos["until"]:
        print(f"-- chaos '{active_chaos['type']}' ended")
        active_chaos["type"] = None

    cpu = max(0, min(100, cpu))
    mem = max(0, min(100, mem))

    payload = {
        "node_id": NODE_ID,
        "node_type": NODE_TYPE,
        "timestamp": now,
        "cpu_percent": round(cpu, 2),
        "mem_percent": round(mem, 2),
    }

    producer.produce(METRICS_TOPIC, key=NODE_ID, value=json.dumps(payload), callback=delivery_report)
    producer.poll(0)
    print("sent:", payload)
    time.sleep(2)