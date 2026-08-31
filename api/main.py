import asyncio
import json
import queue
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles
from confluent_kafka import Consumer, Producer
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import os
BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")


latest_state: dict[str, dict] = {}
recent_anomalies = deque(maxlen=100)
event_queue: "queue.Queue[str]" = queue.Queue()
connected_clients: set[WebSocket] = set()
producer = Producer({"bootstrap.servers": BROKER})


def metrics_consumer_thread():
    consumer = Consumer({
        "bootstrap.servers":BROKER,
        "group.id": "dashboard-metrics",
        "auto.offset.reset": "latest",

    })
    consumer.subscribe(["system-metrics"])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
           continue
        event_queue.put({"kind":"metric","data":json.loads(msg.value())})

def anomaly_consumer_thread():
    consumer = Consumer({
        "bootstrap.servers":BROKER,
        "group.id": "dashboard-anomalies",
        "auto.offset.reset": "latest",

    })
    consumer.subscribe(["anomalies"])
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
           continue
        event_queue.put({"kind":"anomaly","data":json.loads(msg.value())})
    
async def broadcast_loop():
    loop = asyncio.get_event_loop()
    while True:
        event = await loop.run_in_executor(None,event_queue.get)

        if event["kind"] == "metric":
            latest_state[event["data"]["node_id"]] = event["data"]
        elif event["kind"] == "anomaly":
            recent_anomalies.appendleft(event["data"])

        dead = set()
        for ws in connected_clients:
            try:
                await ws.send_json(event)
            except Exception:
                dead.add(ws)
        connected_clients.difference_update(dead)

@asynccontextmanager
async def lifespan(app:FastAPI):
    t1 = threading.Thread(target = metrics_consumer_thread,daemon = True)
    t2 = threading.Thread(target = anomaly_consumer_thread,daemon = True)
    t1.start()
    t2.start()
    broadcast_task = asyncio.create_task(broadcast_loop())
    yield
    broadcast_task.cancel()
    

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws")
async def ws_endpoint(ws:WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    data = await ws.send_json({
        "kind": "snapshot",
        "data": {
        "nodes": latest_state,
        "anomalies":list(recent_anomalies),
        }
    })

    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(ws)
    
class ChaosRequest(BaseModel):
    type: str
    duration_seconds: int=30

@app.get("/api/nodes")
def get_nodes():
    return latest_state

@app.get("/api/anomalies")
def get_anomalies():
    return list(recent_anomalies)

@app.post("/api/chaos/{node_id}")
def trigger_chaos(node_id:str,req:ChaosRequest):
    command = {
        "node_id":node_id,
        "type":req.type,
        "duration_seconds":req.duration_seconds,
        "issued_at":time.time(),
    }
    producer.produce("chaos-commands",key=node_id,value=json.dumps(command))
    producer.flush()
    return {"status":"sent","command":command}

app.mount("/", StaticFiles(directory="dashboard", html=True), name="dashboard")