import os
from diagrams import Diagram, Cluster, Edge
from diagrams.onprem.queue import Kafka
from diagrams.programming.language import Python
from diagrams.onprem.client import Client

try:
    from diagrams.onprem.queue import Zookeeper as ZookeeperIcon
except ImportError:
    from diagrams.generic.blank import Blank as ZookeeperIcon

try:
    from diagrams.programming.framework import Fastapi as ApiIcon
except ImportError:
    from diagrams.generic.blank import Blank as ApiIcon

os.makedirs("docs", exist_ok=True)

graph_attr = {
    "fontsize": "14",
    "bgcolor": "transparent",
    "pad": "0.4",
    "splines": "spline",
}

with Diagram(
    "Kafka Fleet Monitor - Architecture",
    filename="docs/architecture",
    outformat="png",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
):
    browser = Client("Browser\n(dashboard)")

    with Cluster("Simulated Fleet (producers)"):
        nodes = [
            Python("web-01"), Python("web-02"), Python("db-01"),
            Python("cache-01"), Python("worker-01"),
        ]

    with Cluster("Kafka Cluster"):
        zk = ZookeeperIcon("Zookeeper")
        with Cluster("Brokers (RF=3)"):
            brokers = [Kafka("kafka1"), Kafka("kafka2"), Kafka("kafka3")]
        for b in brokers:
            zk - b

    detector = Python("anomaly_detector\n(Isolation Forest)")
    api = ApiIcon("FastAPI\n+ WebSocket")

    nodes >> Edge(label="system-metrics") >> brokers[0]
    brokers[0] >> Edge(label="system-metrics") >> detector
    detector >> Edge(label="anomalies") >> brokers[0]
    brokers[0] >> Edge(label="anomalies + metrics") >> api
    api >> Edge(label="WebSocket") >> browser

    browser >> Edge(label="POST /api/chaos", style="dashed", color="firebrick") >> api
    api >> Edge(label="chaos-commands", style="dashed", color="firebrick") >> brokers[0]
    brokers[0] >> Edge(label="chaos-commands", style="dashed", color="firebrick") >> nodes