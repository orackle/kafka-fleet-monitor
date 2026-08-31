const nodeCards = {};
const knownNodes = new Set();
const MAX_POINTS = 30;
let anomalyCounter = 0;

const grid = document.getElementById("node-grid");
const emptyState = document.getElementById("empty-state");
const anomalyList = document.getElementById("anomaly-list");
const feedEmpty = document.getElementById("feed-empty");
const statusEl = document.getElementById("status");
const chaosNodeSelect = document.getElementById("chaos-node");
const activeNodesEl = document.getElementById("active-nodes-count");
const totalAnomaliesEl = document.getElementById("total-anomalies-count");
const anomalyBadgeEl = document.getElementById("anomaly-badge");
const toastNotify = document.getElementById("toast-notify");

function ensureNodeCard(nodeId, nodeType) {
    if (nodeCards[nodeId]) return nodeCards[nodeId];

    if (emptyState && emptyState.parentNode) {
        emptyState.remove();
    }

    const typeClass = (nodeType || "web").toLowerCase();
    const card = document.createElement("div");
    card.className = "node-card";
    card.id = `card-${nodeId}`;
    
    card.innerHTML = `
    <div class="node-card-header">
      <span class="node-id-tag">${nodeId}</span>
      <span class="node-type-pill ${typeClass}">${nodeType || "NODE"}</span>
    </div>
    <div class="metrics-container">
      <div class="metric-card-inner">
        <div class="metric-header-line">
          <span class="metric-title">CPU</span>
          <span class="metric-num cpu cpu-val">0.0%</span>
        </div>
        <div class="progress-track">
          <div class="progress-fill cpu cpu-bar" style="width: 0%"></div>
        </div>
      </div>
      <div class="metric-card-inner">
        <div class="metric-header-line">
          <span class="metric-title">MEMORY</span>
          <span class="metric-num mem mem-val">0.0%</span>
        </div>
        <div class="progress-track">
          <div class="progress-fill mem mem-bar" style="width: 0%"></div>
        </div>
      </div>
    </div>
    <div class="sparkline-wrapper">
      <canvas height="60"></canvas>
    </div>`;

    grid.appendChild(card);

    const canvas = card.querySelector("canvas");
    const ctx = canvas.getContext("2d");

    const chart = new Chart(ctx, {
        type: "line",
        data: {
            labels: [],
            datasets: [
                {
                    label: "CPU",
                    data: [],
                    borderColor: "#0284c7",
                    borderWidth: 1.8,
                    backgroundColor: "rgba(2, 132, 199, 0.06)",
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                },
                {
                    label: "MEM",
                    data: [],
                    borderColor: "#a855f7",
                    borderWidth: 1.8,
                    backgroundColor: "rgba(168, 85, 247, 0.06)",
                    fill: true,
                    tension: 0.3,
                    pointRadius: 0,
                },
            ],
        },
        options: {
            animation: false,
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    min: 0,
                    max: 100,
                    grid: { color: "#18181b" },
                    ticks: { color: "#71717a", font: { family: "Space Mono", size: 9 } }
                },
                x: { display: false },
            },
            plugins: { legend: { display: false } },
        },
    });

    const entry = { el: card, chart };
    nodeCards[nodeId] = entry;

    if (!knownNodes.has(nodeId)) {
        knownNodes.add(nodeId);
        const opt = document.createElement("option");
        opt.value = nodeId;
        opt.textContent = `${nodeId} (${(nodeType || "node").toUpperCase()})`;
        chaosNodeSelect.appendChild(opt);
    }

    activeNodesEl.textContent = Object.keys(nodeCards).length;
    return entry;
}

function updateNodeCard(data) {
    const entry = ensureNodeCard(data.node_id, data.node_type);
    
    const cpuVal = entry.el.querySelector(".cpu-val");
    const cpuBar = entry.el.querySelector(".cpu-bar");
    const memVal = entry.el.querySelector(".mem-val");
    const memBar = entry.el.querySelector(".mem-bar");

    if (cpuVal && cpuBar) {
        cpuVal.textContent = data.cpu_percent.toFixed(1) + "%";
        cpuBar.style.width = Math.min(100, Math.max(0, data.cpu_percent)) + "%";
    }
    if (memVal && memBar) {
        memVal.textContent = data.mem_percent.toFixed(1) + "%";
        memBar.style.width = Math.min(100, Math.max(0, data.mem_percent)) + "%";
    }

    const chart = entry.chart;
    const timeLabel = new Date(data.timestamp * 1000).toLocaleTimeString();
    chart.data.labels.push(timeLabel);
    chart.data.datasets[0].data.push(data.cpu_percent);
    chart.data.datasets[1].data.push(data.mem_percent);

    if (chart.data.labels.length > MAX_POINTS) {
        chart.data.labels.shift();
        chart.data.datasets.forEach(ds => ds.data.shift());
    }
    chart.update();
}

function flashAnomaly(nodeId) {
    const entry = nodeCards[nodeId];
    if (!entry) return;
    entry.el.classList.add("anomaly-flash");
    setTimeout(() => entry.el.classList.remove("anomaly-flash"), 2500);
}

function addAnomaly(data) {
    flashAnomaly(data.node_id);
    if (feedEmpty && feedEmpty.parentNode) {
        feedEmpty.style.display = "none";
    }

    anomalyCounter++;
    totalAnomaliesEl.textContent = anomalyCounter;
    anomalyBadgeEl.textContent = anomalyCounter;

    const li = document.createElement("li");
    li.className = "feed-item";

    const t = new Date(data.timestamp * 1000).toLocaleTimeString();
    const reasonTags = (data.reasons || [])
        .map(r => `<span class="reason-chip">${r}</span>`)
        .join("");

    li.innerHTML = `
    <div class="feed-item-top">
      <span class="feed-node-id">${data.node_id}</span>
      <span class="feed-time-text">${t}</span>
    </div>
    <div class="feed-tags-wrapper">${reasonTags}</div>`;

    anomalyList.prepend(li);
    while (anomalyList.children.length > 30) {
        anomalyList.removeChild(anomalyList.lastChild);
    }
}

function showToast(msg) {
    toastNotify.textContent = msg;
    toastNotify.classList.add("show");
    setTimeout(() => toastNotify.classList.remove("show"), 3000);
}

function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(`${proto}://${location.host}/ws`);

    ws.onopen = () => {
        statusEl.className = "status-pill online";
        statusEl.querySelector(".status-txt").textContent = "Connected";
    };

    ws.onclose = () => {
        statusEl.className = "status-pill offline";
        statusEl.querySelector(".status-txt").textContent = "Disconnected";
        setTimeout(connect, 2000);
    };

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);
        if (msg.kind === "snapshot") {
            if (msg.data.nodes) {
                Object.values(msg.data.nodes).forEach(updateNodeCard);
            }
            if (msg.data.anomalies) {
                msg.data.anomalies.slice().reverse().forEach(addAnomaly);
            }
        } else if (msg.kind === "metric") {
            updateNodeCard(msg.data);
        } else if (msg.kind === "anomaly") {
            addAnomaly(msg.data);
        }
    };
}

document.getElementById("chaos-btn").addEventListener("click", async () => {
    const node = chaosNodeSelect.value;
    const type = document.getElementById("chaos-type").value;
    const duration = parseInt(document.getElementById("chaos-duration").value, 10);
    if (!node) {
        alert("Please select a target node.");
        return;
    }
    try {
        const res = await fetch(`/api/chaos/${node}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ type, duration_seconds: duration }),
        });
        if (res.ok) {
            showToast(`Fault triggered on ${node}`);
        }
    } catch (err) {
        console.error("Fault error:", err);
    }
});

connect();