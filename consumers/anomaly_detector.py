import json
import time
from collections import deque, defaultdict
import os

import numpy as np
from confluent_kafka import Consumer, Producer
from sklearn.ensemble import IsolationForest

BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
IN_TOPIC = "system-metrics"
OUT_TOPIC = "anomalies"
GROUP_ID = "anomaly-detector"

WINDOW_SIZE = 40
MIN_POINTS_FOR_ML = 10
RETRAIN_EVERY = 5
Z_THRESHOLD = 2.5
STATIC_CPU_THRESHOLD = 80.0
STATIC_MEM_THRESHOLD = 85.0

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset": "latest",
})
consumer.subscribe([IN_TOPIC])

producer = Producer({"bootstrap.servers": BROKER})

# Baseline windows store clean normal metrics to prevent model contamination during spikes
windows = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
models = {}
points_since_fit = defaultdict(int)


def static_flags(cpu, mem):
    flags = []
    if cpu >= STATIC_CPU_THRESHOLD:
        flags.append(f"cpu_percent spike ({cpu:.1f}% >= {STATIC_CPU_THRESHOLD:.0f}%)")
    if mem >= STATIC_MEM_THRESHOLD:
        flags.append(f"mem_percent leak ({mem:.1f}% >= {STATIC_MEM_THRESHOLD:.0f}%)")
    return flags


def zscore_flags(node_id, cpu, mem):
    history = list(windows[node_id])
    if len(history) < 5:
        return []
    cpu_hist = np.array([h["cpu_percent"] for h in history])
    mem_hist = np.array([h["mem_percent"] for h in history])
    flags = []
    for name, value, hist in (("cpu_percent", cpu, cpu_hist), ("mem_percent", mem, mem_hist)):
        mean, std = hist.mean(), hist.std()
        if std > 0.01 and abs(value - mean) > Z_THRESHOLD * std:
            flags.append(f"{name} z-score {abs(value - mean) / std:.1f}")
        elif abs(value - mean) > 25.0:
            flags.append(f"{name} sudden delta ({abs(value - mean):.1f}%)")
    return flags


def isolation_forest_flag(node_id, cpu, mem):
    history = list(windows[node_id])
    if len(history) < MIN_POINTS_FOR_ML:
        return False, None

    if node_id not in models or points_since_fit[node_id] >= RETRAIN_EVERY:
        features = np.array([[h["cpu_percent"], h["mem_percent"]] for h in history])
        model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
        model.fit(features)
        models[node_id] = model
        points_since_fit[node_id] = 0

    model = models[node_id]
    score = model.decision_function([[cpu, mem]])[0]
    is_outlier = model.predict([[cpu, mem]])[0] == -1
    return is_outlier, round(float(score), 3)


print(f"anomaly detector running, group '{GROUP_ID}'...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            if msg is not None and msg.error():
                print("consumer error:", msg.error())
            continue

        data = json.loads(msg.value())
        node_id, cpu, mem = data["node_id"], data["cpu_percent"], data["mem_percent"]

        # Run anomaly checks
        st_flags = static_flags(cpu, mem)
        z_flags = zscore_flags(node_id, cpu, mem)
        is_outlier, score = isolation_forest_flag(node_id, cpu, mem)

        is_anomaly = bool(st_flags or z_flags or is_outlier)

        # Only append to rolling baseline history if NOT an anomaly
        # This prevents spiked values from polluting the baseline model
        if not is_anomaly or len(windows[node_id]) < 5:
            windows[node_id].append(data)
            points_since_fit[node_id] += 1

        if is_anomaly:
            reasons = st_flags + z_flags + ([f"isolation_forest (score={score})"] if is_outlier else [])
            # Deduplicate reasons while preserving order
            seen = set()
            unique_reasons = [r for r in reasons if not (r in seen or seen.add(r))]

            anomaly = {
                "node_id": node_id,
                "timestamp": time.time(),
                "cpu_percent": cpu,
                "mem_percent": mem,
                "reasons": unique_reasons,
            }
            producer.produce(OUT_TOPIC, key=node_id, value=json.dumps(anomaly))
            producer.poll(0)
            print("ANOMALY DETECTED:", anomaly)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()