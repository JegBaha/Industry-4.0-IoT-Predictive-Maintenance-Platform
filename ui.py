"""Smart Factory Dashboard - Industry 4.0 IoT Platform."""
import logging
import json
import time
from threading import Thread
from typing import Optional
from collections import deque
from dataclasses import dataclass, field

import paho.mqtt.client as mqtt
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from config import mqtt as mqtt_cfg
from mosquitto_runner import ensure_broker_running
from mqtt_simulator import publish_stream
from ingest_consumer import consume_forever
from db import fetch_kpi_oee, fetch_kpi_mttr

log = logging.getLogger(__name__)

app = FastAPI(title="SmartFact Dashboard")

# Thread references
sim_thread: Optional[Thread] = None
consumer_thread: Optional[Thread] = None
listener_thread: Optional[Thread] = None

# Real-time data storage
messages_buffer: deque[str] = deque(maxlen=50)
start_time: float = time.time()


@dataclass
class MachineState:
    code: str
    line: str
    latest: Optional[dict] = None
    history: deque = field(default_factory=lambda: deque(maxlen=200))
    prediction: float = 0.0


machine_states: dict[str, MachineState] = {
    "MX100": MachineState(code="MX100", line="Line1"),
    "MX200": MachineState(code="MX200", line="Line2"),
}

alarms: list[dict] = []
alarm_counter: int = 0

ALARM_THRESHOLDS = {
    "temperature": {"warning": 60.0, "critical": 70.0},
    "vibration": {"warning": 0.5, "critical": 0.7},
    "current": {"warning": 12.0, "critical": 15.0},
    "throughput_low": {"warning": 70, "critical": 50},
}


def check_alarms(machine_code: str, data: dict):
    """Check sensor data against thresholds and create alarms."""
    global alarm_counter, alarms

    checks = [
        ("temperature", data.get("temperature", 0), "high", True),
        ("vibration", data.get("vibration", 0), "high", True),
        ("current", data.get("current", 0), "high", True),
        ("throughput_low", data.get("throughput", 100), "low", False),
    ]

    for sensor, value, _, is_high in checks:
        thresholds = ALARM_THRESHOLDS.get(sensor, {})
        warning = thresholds.get("warning", float("inf") if is_high else 0)
        critical = thresholds.get("critical", float("inf") if is_high else 0)

        severity = None
        if is_high:
            if value >= critical:
                severity = "critical"
            elif value >= warning:
                severity = "warning"
        else:
            if value <= critical:
                severity = "critical"
            elif value <= warning:
                severity = "warning"

        if severity:
            # Check if similar alarm exists in last 30 seconds
            existing = [a for a in alarms[-10:]
                       if a["machine_code"] == machine_code
                       and a["sensor"] == sensor
                       and time.time() - a["timestamp"] < 30]
            if not existing:
                alarm_counter += 1
                alarms.append({
                    "id": alarm_counter,
                    "machine_code": machine_code,
                    "sensor": sensor.replace("_low", ""),
                    "severity": severity,
                    "value": value,
                    "message": f"{sensor.replace('_low', '').title()} {'exceeded' if is_high else 'below'} threshold: {value}",
                    "timestamp": time.time(),
                    "acknowledged": False,
                })
                # Keep only last 100 alarms
                if len(alarms) > 100:
                    alarms = alarms[-100:]


def _start_sim():
    global sim_thread
    if sim_thread and sim_thread.is_alive():
        return
    ensure_broker_running()

    def run_sim():
        try:
            machines = [("MX100", "Line1"), ("MX200", "Line2")]
            publish_stream(machines)
        except Exception as exc:
            log.exception("Simulator stopped with error: %s", exc)

    sim_thread = Thread(target=run_sim, daemon=True)
    sim_thread.start()
    log.info("Simulation thread started")


def _start_consumer():
    global consumer_thread
    if consumer_thread and consumer_thread.is_alive():
        return

    def run_consumer():
        try:
            consume_forever()
        except Exception as exc:
            log.exception("Consumer stopped with error: %s", exc)

    consumer_thread = Thread(target=run_consumer, daemon=True)
    consumer_thread.start()
    log.info("Consumer thread started")


def _start_listener():
    global listener_thread
    if listener_thread and listener_thread.is_alive():
        return

    def on_message(_client, _userdata, msg):
        try:
            payload = msg.payload.decode()
            messages_buffer.append(f"{msg.topic} {payload}")

            # Parse and store sensor data
            try:
                data = json.loads(payload)
                machine_code = data.get("machine_code")
                if machine_code and machine_code in machine_states:
                    machine_states[machine_code].latest = data
                    machine_states[machine_code].history.append({
                        "timestamp": data.get("timestamp", time.time()),
                        "temperature": data.get("temperature", 0),
                        "vibration": data.get("vibration", 0),
                        "current": data.get("current", 0),
                        "throughput": data.get("throughput", 0),
                    })
                    check_alarms(machine_code, data)
            except json.JSONDecodeError:
                pass
        except Exception as exc:
            log.exception("Listener error: %s", exc)

    def run_listener():
        client = mqtt.Client(client_id="ui-dashboard-listener")
        if mqtt_cfg.username and mqtt_cfg.password:
            client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)
        client.on_message = on_message
        client.connect(mqtt_cfg.host, mqtt_cfg.port, keepalive=60)
        client.subscribe(f"{mqtt_cfg.base_topic}/#")
        client.loop_forever()

    listener_thread = Thread(target=run_listener, daemon=True)
    listener_thread.start()
    log.info("Dashboard listener thread started")


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.post("/api/system/start")
def api_start_all():
    _start_sim()
    _start_consumer()
    _start_listener()
    return {"ok": True}


@app.post("/api/system/stop")
def api_stop_all():
    return {"ok": True, "message": "Stop requires restart"}


@app.get("/api/system/status")
def api_system_status():
    return {
        "broker": "running",
        "simulation": "running" if sim_thread and sim_thread.is_alive() else "stopped",
        "consumer": "running" if consumer_thread and consumer_thread.is_alive() else "stopped",
        "listener": "running" if listener_thread and listener_thread.is_alive() else "stopped",
        "uptime_seconds": int(time.time() - start_time),
    }


@app.get("/api/machines")
def api_machines():
    machines = []
    for _, state in machine_states.items():
        machines.append({
            "code": state.code,
            "line": state.line,
            "status": "running" if state.latest else "waiting",
            "latest": state.latest,
            "prediction": state.prediction,
        })
    return {"machines": machines}


@app.get("/api/machines/{code}/history")
def api_machine_history(code: str, limit: int = 100):
    if code not in machine_states:
        return {"error": "Machine not found"}
    history = list(machine_states[code].history)[-limit:]
    return {"code": code, "readings": history}


@app.get("/api/kpis")
def api_kpis():
    oee_data = fetch_kpi_oee()
    mttr_data = fetch_kpi_mttr()

    oee = {d["machine_code"]: d for d in oee_data}
    mttr = {d["machine_code"]: d for d in mttr_data}

    # Default values if DB not connected
    if not oee:
        oee = {
            "MX100": {"availability": 0.95, "performance": 0.88, "quality": 0.99, "oee": 0.83},
            "MX200": {"availability": 0.92, "performance": 0.85, "quality": 0.98, "oee": 0.77},
        }
    if not mttr:
        mttr = {"MX100": {"mttr_minutes": 12.5}, "MX200": {"mttr_minutes": 8.3}}

    return {"oee": oee, "mttr": mttr}


@app.get("/api/alarms")
def api_alarms():
    return {"alarms": alarms[-50:]}


@app.post("/api/alarms/{alarm_id}/ack")
def api_ack_alarm(alarm_id: int):
    for alarm in alarms:
        if alarm["id"] == alarm_id:
            alarm["acknowledged"] = True
            return {"ok": True}
    return {"ok": False, "error": "Alarm not found"}


@app.get("/api/messages")
def api_messages():
    return {"messages": list(messages_buffer)}


# Legacy endpoints for compatibility
@app.post("/actions/start-all")
def start_all():
    return api_start_all()


@app.get("/status")
def status():
    return api_system_status()


@app.get("/messages")
def messages():
    return api_messages()


# ============================================================================
# FRONTEND
# ============================================================================

CSS = """
:root {
    --bg-primary: #0a0e14;
    --bg-secondary: #111a2b;
    --bg-tertiary: #1a2438;
    --bg-hover: #243048;
    --accent-blue: #1e90ff;
    --accent-cyan: #00d4ff;
    --accent-green: #00ff88;
    --accent-yellow: #ffc107;
    --accent-orange: #ff8c00;
    --accent-red: #ff3b3b;
    --text-primary: #f0f4f8;
    --text-secondary: #8892a0;
    --text-muted: #5a6a7a;
    --border-color: #2a3a4a;
    --glow-blue: 0 0 20px rgba(30, 144, 255, 0.3);
    --glow-cyan: 0 0 20px rgba(0, 212, 255, 0.3);
    --glow-green: 0 0 20px rgba(0, 255, 136, 0.3);
    --glow-red: 0 0 20px rgba(255, 59, 59, 0.3);
}

* { margin: 0; padding: 0; box-sizing: border-box; }

body {
    font-family: 'Segoe UI', system-ui, sans-serif;
    background: var(--bg-primary);
    color: var(--text-primary);
    min-height: 100vh;
    line-height: 1.5;
}

/* Header */
.header {
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border-color);
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    z-index: 100;
}

.logo-section {
    display: flex;
    align-items: center;
    gap: 12px;
}

.logo {
    width: 40px;
    height: 40px;
    background: linear-gradient(135deg, var(--accent-cyan), var(--accent-blue));
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 20px;
    font-weight: bold;
}

.brand h1 {
    font-size: 20px;
    font-weight: 600;
    color: var(--text-primary);
}

.brand span {
    font-size: 11px;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 1px;
}

.header-status {
    display: flex;
    align-items: center;
    gap: 20px;
}

.status-indicator {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: var(--text-secondary);
}

.status-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: var(--accent-green);
    box-shadow: 0 0 8px var(--accent-green);
}

.status-dot.warning { background: var(--accent-yellow); box-shadow: 0 0 8px var(--accent-yellow); }
.status-dot.error { background: var(--accent-red); box-shadow: 0 0 8px var(--accent-red); }
.status-dot.stopped { background: var(--text-muted); box-shadow: none; }

.clock {
    font-family: 'Consolas', monospace;
    font-size: 14px;
    color: var(--accent-cyan);
}

/* Navigation */
.nav-tabs {
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border-color);
    display: flex;
    padding: 0 24px;
    gap: 4px;
}

.nav-tab {
    padding: 12px 20px;
    background: transparent;
    border: none;
    color: var(--text-secondary);
    font-size: 14px;
    font-weight: 500;
    cursor: pointer;
    border-bottom: 2px solid transparent;
    transition: all 0.2s;
}

.nav-tab:hover {
    color: var(--text-primary);
    background: var(--bg-tertiary);
}

.nav-tab.active {
    color: var(--accent-cyan);
    border-bottom-color: var(--accent-cyan);
}

/* Main Content */
.main-content {
    padding: 24px;
    max-width: 1600px;
    margin: 0 auto;
}

.tab-content {
    display: none;
}

.tab-content.active {
    display: block;
}

/* Dashboard Grid */
.dashboard-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
    gap: 20px;
    margin-bottom: 24px;
}

/* Machine Card */
.machine-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
    transition: all 0.3s;
}

.machine-card:hover {
    border-color: var(--accent-blue);
    box-shadow: var(--glow-blue);
}

.card-header {
    background: var(--bg-tertiary);
    padding: 16px 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid var(--border-color);
}

.machine-info {
    display: flex;
    align-items: center;
    gap: 12px;
}

.machine-icon {
    width: 44px;
    height: 44px;
    background: linear-gradient(135deg, var(--bg-hover), var(--bg-secondary));
    border: 2px solid var(--border-color);
    border-radius: 10px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 22px;
}

.machine-details h3 {
    font-size: 18px;
    font-weight: 600;
}

.machine-details span {
    font-size: 12px;
    color: var(--text-secondary);
}

.status-badge {
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}

.status-badge.running {
    background: rgba(0, 255, 136, 0.15);
    color: var(--accent-green);
    border: 1px solid rgba(0, 255, 136, 0.3);
}

.status-badge.waiting {
    background: rgba(255, 193, 7, 0.15);
    color: var(--accent-yellow);
    border: 1px solid rgba(255, 193, 7, 0.3);
}

.status-badge.error {
    background: rgba(255, 59, 59, 0.15);
    color: var(--accent-red);
    border: 1px solid rgba(255, 59, 59, 0.3);
}

.card-body {
    padding: 20px;
}

.sensor-grid {
    display: grid;
    gap: 16px;
}

.sensor-row {
    display: grid;
    grid-template-columns: 100px 1fr 70px;
    align-items: center;
    gap: 12px;
}

.sensor-label {
    font-size: 13px;
    color: var(--text-secondary);
}

.sensor-bar {
    height: 8px;
    background: var(--bg-primary);
    border-radius: 4px;
    overflow: hidden;
    position: relative;
}

.sensor-bar-fill {
    height: 100%;
    border-radius: 4px;
    transition: width 0.5s ease, background 0.3s;
}

.sensor-bar-fill.normal { background: linear-gradient(90deg, var(--accent-cyan), var(--accent-blue)); }
.sensor-bar-fill.warning { background: linear-gradient(90deg, var(--accent-yellow), var(--accent-orange)); }
.sensor-bar-fill.critical { background: linear-gradient(90deg, var(--accent-orange), var(--accent-red)); }

.sensor-value {
    font-family: 'Consolas', monospace;
    font-size: 14px;
    font-weight: 600;
    text-align: right;
    color: var(--text-primary);
}

.card-footer {
    padding: 16px 20px;
    background: var(--bg-tertiary);
    border-top: 1px solid var(--border-color);
    display: flex;
    align-items: center;
    justify-content: space-between;
}

/* Prediction Gauge */
.prediction-section {
    display: flex;
    align-items: center;
    gap: 16px;
}

.gauge-container {
    position: relative;
    width: 60px;
    height: 60px;
}

.gauge-svg {
    transform: rotate(-90deg);
}

.gauge-bg {
    fill: none;
    stroke: var(--bg-primary);
    stroke-width: 6;
}

.gauge-fill {
    fill: none;
    stroke-width: 6;
    stroke-linecap: round;
    transition: stroke-dashoffset 0.5s ease, stroke 0.3s;
}

.gauge-fill.low { stroke: var(--accent-green); }
.gauge-fill.medium { stroke: var(--accent-yellow); }
.gauge-fill.high { stroke: var(--accent-orange); }
.gauge-fill.critical { stroke: var(--accent-red); }

.gauge-text {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    font-size: 14px;
    font-weight: 700;
    font-family: 'Consolas', monospace;
}

.prediction-info {
    font-size: 12px;
}

.prediction-info .label {
    color: var(--text-secondary);
    margin-bottom: 2px;
}

.prediction-info .risk-level {
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
}

.risk-level.low { color: var(--accent-green); }
.risk-level.medium { color: var(--accent-yellow); }
.risk-level.high { color: var(--accent-orange); }
.risk-level.critical { color: var(--accent-red); }

/* KPI Section */
.kpi-section {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}

.kpi-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 20px;
    text-align: center;
}

.kpi-card h4 {
    font-size: 12px;
    color: var(--text-secondary);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 8px;
}

.kpi-value {
    font-size: 32px;
    font-weight: 700;
    font-family: 'Consolas', monospace;
    color: var(--accent-cyan);
}

.kpi-value.good { color: var(--accent-green); }
.kpi-value.warning { color: var(--accent-yellow); }
.kpi-value.bad { color: var(--accent-red); }

.kpi-subtitle {
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 4px;
}

/* Alarms Section */
.alarms-panel {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
}

.panel-header {
    background: var(--bg-tertiary);
    padding: 12px 20px;
    border-bottom: 1px solid var(--border-color);
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.panel-header h3 {
    font-size: 14px;
    font-weight: 600;
}

.alarm-count {
    background: var(--accent-red);
    color: white;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
}

.alarms-list {
    max-height: 300px;
    overflow-y: auto;
}

.alarm-item {
    padding: 12px 20px;
    border-bottom: 1px solid var(--border-color);
    display: flex;
    align-items: center;
    gap: 12px;
    transition: background 0.2s;
}

.alarm-item:hover {
    background: var(--bg-tertiary);
}

.alarm-item:last-child {
    border-bottom: none;
}

.alarm-icon {
    width: 32px;
    height: 32px;
    border-radius: 8px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 14px;
}

.alarm-icon.warning {
    background: rgba(255, 193, 7, 0.15);
    color: var(--accent-yellow);
}

.alarm-icon.critical {
    background: rgba(255, 59, 59, 0.15);
    color: var(--accent-red);
}

.alarm-content {
    flex: 1;
}

.alarm-content .message {
    font-size: 13px;
    color: var(--text-primary);
}

.alarm-content .meta {
    font-size: 11px;
    color: var(--text-muted);
    margin-top: 2px;
}

.alarm-item.acknowledged {
    opacity: 0.5;
}

.btn-ack {
    padding: 4px 10px;
    background: var(--bg-hover);
    border: 1px solid var(--border-color);
    border-radius: 4px;
    color: var(--text-secondary);
    font-size: 11px;
    cursor: pointer;
    transition: all 0.2s;
}

.btn-ack:hover {
    background: var(--accent-blue);
    color: white;
    border-color: var(--accent-blue);
}

/* Chart Section */
.chart-panel {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
    margin-top: 24px;
}

.chart-container {
    padding: 20px;
    height: 250px;
}

.chart-canvas {
    width: 100%;
    height: 100%;
}

/* Control Panel */
.control-panel {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 20px;
}

.control-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 24px;
}

.control-card h3 {
    font-size: 16px;
    margin-bottom: 16px;
    color: var(--text-primary);
}

.control-buttons {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
}

.btn {
    padding: 12px 24px;
    border: none;
    border-radius: 8px;
    font-size: 14px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.2s;
    display: flex;
    align-items: center;
    gap: 8px;
}

.btn-primary {
    background: linear-gradient(135deg, var(--accent-blue), var(--accent-cyan));
    color: white;
}

.btn-primary:hover {
    box-shadow: var(--glow-cyan);
    transform: translateY(-2px);
}

.btn-secondary {
    background: var(--bg-tertiary);
    color: var(--text-primary);
    border: 1px solid var(--border-color);
}

.btn-secondary:hover {
    background: var(--bg-hover);
    border-color: var(--accent-blue);
}

.btn-danger {
    background: var(--accent-red);
    color: white;
}

.btn-danger:hover {
    box-shadow: var(--glow-red);
}

/* Status List */
.status-list {
    margin-top: 16px;
}

.status-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 0;
    border-bottom: 1px solid var(--border-color);
}

.status-row:last-child {
    border-bottom: none;
}

.status-row .label {
    color: var(--text-secondary);
    font-size: 13px;
}

.status-row .value {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 13px;
    font-weight: 500;
}

/* Messages Panel */
.messages-panel {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
}

.messages-list {
    font-family: 'Consolas', monospace;
    font-size: 12px;
    padding: 16px;
    max-height: 400px;
    overflow-y: auto;
    background: var(--bg-primary);
}

.message-line {
    padding: 4px 0;
    border-bottom: 1px solid var(--border-color);
    color: var(--text-secondary);
    word-break: break-all;
}

.message-line:last-child {
    border-bottom: none;
}

/* Footer */
.footer {
    background: var(--bg-secondary);
    border-top: 1px solid var(--border-color);
    padding: 12px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 12px;
    color: var(--text-muted);
    margin-top: 24px;
}

.footer-left {
    display: flex;
    align-items: center;
    gap: 16px;
}

/* Animations */
@keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: 0.5; }
}

.pulse {
    animation: pulse 2s infinite;
}

@keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
}

.fade-in {
    animation: fadeIn 0.3s ease;
}

/* Scrollbar */
::-webkit-scrollbar {
    width: 8px;
    height: 8px;
}

::-webkit-scrollbar-track {
    background: var(--bg-primary);
}

::-webkit-scrollbar-thumb {
    background: var(--border-color);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: var(--text-muted);
}

/* Responsive */
@media (max-width: 768px) {
    .header {
        flex-direction: column;
        gap: 12px;
        padding: 12px 16px;
    }

    .nav-tabs {
        overflow-x: auto;
        padding: 0 16px;
    }

    .main-content {
        padding: 16px;
    }

    .dashboard-grid {
        grid-template-columns: 1fr;
    }

    .sensor-row {
        grid-template-columns: 80px 1fr 60px;
    }
}
"""

HTML = """
<div class="header">
    <div class="logo-section">
        <div class="logo">SF</div>
        <div class="brand">
            <h1>SmartFact</h1>
            <span>Industry 4.0 IoT Platform</span>
        </div>
    </div>
    <div class="header-status">
        <div class="status-indicator">
            <div class="status-dot" id="connection-dot"></div>
            <span id="connection-text">Connecting...</span>
        </div>
        <div class="clock" id="clock">--:--:--</div>
    </div>
</div>

<nav class="nav-tabs">
    <button class="nav-tab active" data-tab="dashboard">Dashboard</button>
    <button class="nav-tab" data-tab="machines">Makineler</button>
    <button class="nav-tab" data-tab="control">Kontrol Paneli</button>
    <button class="nav-tab" data-tab="alarms">Alarmlar</button>
</nav>

<main class="main-content">
    <!-- Dashboard Tab -->
    <div class="tab-content active" id="tab-dashboard">
        <div class="dashboard-grid" id="machines-grid"></div>

        <div class="kpi-section" id="kpi-section">
            <div class="kpi-card">
                <h4>Ortalama OEE</h4>
                <div class="kpi-value" id="kpi-oee">--</div>
                <div class="kpi-subtitle">Overall Equipment Effectiveness</div>
            </div>
            <div class="kpi-card">
                <h4>Ortalama MTTR</h4>
                <div class="kpi-value" id="kpi-mttr">--</div>
                <div class="kpi-subtitle">Mean Time To Repair (dk)</div>
            </div>
            <div class="kpi-card">
                <h4>Aktif Alarmlar</h4>
                <div class="kpi-value" id="kpi-alarms">0</div>
                <div class="kpi-subtitle">Onay bekleyen</div>
            </div>
            <div class="kpi-card">
                <h4>Uptime</h4>
                <div class="kpi-value good" id="kpi-uptime">--</div>
                <div class="kpi-subtitle">Sistem suresi</div>
            </div>
        </div>

        <div class="alarms-panel">
            <div class="panel-header">
                <h3>Son Alarmlar</h3>
                <span class="alarm-count" id="alarm-count">0</span>
            </div>
            <div class="alarms-list" id="alarms-list">
                <div class="alarm-item" style="justify-content: center; color: var(--text-muted);">
                    Alarm yok
                </div>
            </div>
        </div>

        <div class="chart-panel">
            <div class="panel-header">
                <h3>Sensor Trend (MX100 - Sicaklik)</h3>
            </div>
            <div class="chart-container">
                <canvas id="chart-canvas" class="chart-canvas"></canvas>
            </div>
        </div>
    </div>

    <!-- Machines Tab -->
    <div class="tab-content" id="tab-machines">
        <div class="dashboard-grid" id="machines-detail-grid"></div>

        <div class="chart-panel">
            <div class="panel-header">
                <h3>Tum Sensorler - Karsilastirma</h3>
            </div>
            <div class="chart-container">
                <canvas id="chart-multi" class="chart-canvas"></canvas>
            </div>
        </div>
    </div>

    <!-- Control Panel Tab -->
    <div class="tab-content" id="tab-control">
        <div class="control-panel">
            <div class="control-card">
                <h3>Sistem Kontrolu</h3>
                <div class="control-buttons">
                    <button class="btn btn-primary" onclick="SmartFactory.startSystem()">
                        <span>&#9654;</span> Baslat
                    </button>
                    <button class="btn btn-secondary" onclick="SmartFactory.refreshAll()">
                        <span>&#8635;</span> Yenile
                    </button>
                </div>
                <div class="status-list" id="system-status-list">
                    <div class="status-row">
                        <span class="label">MQTT Broker</span>
                        <span class="value"><div class="status-dot stopped" id="status-broker"></div> <span id="status-broker-text">Stopped</span></span>
                    </div>
                    <div class="status-row">
                        <span class="label">Simulasyon</span>
                        <span class="value"><div class="status-dot stopped" id="status-sim"></div> <span id="status-sim-text">Stopped</span></span>
                    </div>
                    <div class="status-row">
                        <span class="label">Consumer</span>
                        <span class="value"><div class="status-dot stopped" id="status-consumer"></div> <span id="status-consumer-text">Stopped</span></span>
                    </div>
                    <div class="status-row">
                        <span class="label">Listener</span>
                        <span class="value"><div class="status-dot stopped" id="status-listener"></div> <span id="status-listener-text">Stopped</span></span>
                    </div>
                </div>
            </div>

            <div class="control-card">
                <h3>MQTT Mesajlari</h3>
                <div class="messages-panel">
                    <div class="messages-list" id="messages-list">
                        <div class="message-line">Mesaj bekleniyor...</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- Alarms Tab -->
    <div class="tab-content" id="tab-alarms">
        <div class="alarms-panel" style="max-width: 800px;">
            <div class="panel-header">
                <h3>Tum Alarmlar</h3>
                <span class="alarm-count" id="alarm-count-full">0</span>
            </div>
            <div class="alarms-list" id="alarms-list-full" style="max-height: 600px;">
                <div class="alarm-item" style="justify-content: center; color: var(--text-muted);">
                    Alarm yok
                </div>
            </div>
        </div>
    </div>
</main>

<footer class="footer">
    <div class="footer-left">
        <span><div class="status-dot" id="footer-dot"></div></span>
        <span id="footer-status">Baglanti bekleniyor</span>
        <span>|</span>
        <span id="footer-update">Son guncelleme: --:--:--</span>
    </div>
    <div>SmartFact v1.2 - Industry 4.0 IoT Platform</div>
</footer>
"""

JS = """
const SmartFactory = (function() {
    'use strict';

    const CONFIG = {
        UPDATE_INTERVAL: 2000,
        KPI_INTERVAL: 10000,
        ALARM_INTERVAL: 5000,
        CHART_POINTS: 50
    };

    const state = {
        machines: {},
        alarms: [],
        kpis: {},
        systemStatus: {},
        activeTab: 'dashboard',
        chartData: { MX100: [], MX200: [] }
    };

    // API Module
    const API = {
        async get(url) {
            try {
                const res = await fetch(url);
                return await res.json();
            } catch (e) {
                console.error('API Error:', e);
                return null;
            }
        },
        async post(url) {
            try {
                const res = await fetch(url, { method: 'POST' });
                return await res.json();
            } catch (e) {
                console.error('API Error:', e);
                return null;
            }
        },
        getMachines: () => API.get('/api/machines'),
        getKPIs: () => API.get('/api/kpis'),
        getAlarms: () => API.get('/api/alarms'),
        getSystemStatus: () => API.get('/api/system/status'),
        getMessages: () => API.get('/api/messages'),
        startSystem: () => API.post('/api/system/start'),
        ackAlarm: (id) => API.post(`/api/alarms/${id}/ack`)
    };

    // Utility functions
    function formatTime(date) {
        return date.toLocaleTimeString('tr-TR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    }

    function formatUptime(seconds) {
        const h = Math.floor(seconds / 3600);
        const m = Math.floor((seconds % 3600) / 60);
        const s = seconds % 60;
        return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    }

    function getSensorPercent(sensor, value) {
        const ranges = {
            temperature: { min: 40, max: 80 },
            vibration: { min: 0, max: 1 },
            current: { min: 5, max: 20 },
            throughput: { min: 0, max: 100 }
        };
        const range = ranges[sensor] || { min: 0, max: 100 };
        return Math.min(100, Math.max(0, ((value - range.min) / (range.max - range.min)) * 100));
    }

    function getSensorStatus(sensor, value) {
        const thresholds = {
            temperature: { warning: 60, critical: 70 },
            vibration: { warning: 0.5, critical: 0.7 },
            current: { warning: 12, critical: 15 }
        };
        const t = thresholds[sensor];
        if (!t) return 'normal';
        if (value >= t.critical) return 'critical';
        if (value >= t.warning) return 'warning';
        return 'normal';
    }

    function getRiskLevel(prob) {
        if (prob >= 0.7) return 'critical';
        if (prob >= 0.5) return 'high';
        if (prob >= 0.3) return 'medium';
        return 'low';
    }

    function getRiskText(level) {
        const texts = { low: 'DUSUK', medium: 'ORTA', high: 'YUKSEK', critical: 'KRITIK' };
        return texts[level] || 'DUSUK';
    }

    // Render functions
    function renderMachineCard(machine) {
        const latest = machine.latest || {};
        const prediction = machine.prediction || Math.random() * 0.3; // Simulated for demo
        const riskLevel = getRiskLevel(prediction);
        const circumference = 2 * Math.PI * 24;
        const offset = circumference - (prediction * circumference);

        return `
            <div class="machine-card fade-in" id="card-${machine.code}">
                <div class="card-header">
                    <div class="machine-info">
                        <div class="machine-icon">&#9881;</div>
                        <div class="machine-details">
                            <h3>${machine.code}</h3>
                            <span>${machine.line}</span>
                        </div>
                    </div>
                    <span class="status-badge ${machine.status}">${machine.status.toUpperCase()}</span>
                </div>
                <div class="card-body">
                    <div class="sensor-grid">
                        <div class="sensor-row">
                            <span class="sensor-label">Sicaklik</span>
                            <div class="sensor-bar">
                                <div class="sensor-bar-fill ${getSensorStatus('temperature', latest.temperature || 0)}"
                                     style="width: ${getSensorPercent('temperature', latest.temperature || 0)}%"></div>
                            </div>
                            <span class="sensor-value">${(latest.temperature || 0).toFixed(1)}°C</span>
                        </div>
                        <div class="sensor-row">
                            <span class="sensor-label">Titresim</span>
                            <div class="sensor-bar">
                                <div class="sensor-bar-fill ${getSensorStatus('vibration', latest.vibration || 0)}"
                                     style="width: ${getSensorPercent('vibration', latest.vibration || 0)}%"></div>
                            </div>
                            <span class="sensor-value">${(latest.vibration || 0).toFixed(3)}</span>
                        </div>
                        <div class="sensor-row">
                            <span class="sensor-label">Akim</span>
                            <div class="sensor-bar">
                                <div class="sensor-bar-fill ${getSensorStatus('current', latest.current || 0)}"
                                     style="width: ${getSensorPercent('current', latest.current || 0)}%"></div>
                            </div>
                            <span class="sensor-value">${(latest.current || 0).toFixed(1)}A</span>
                        </div>
                        <div class="sensor-row">
                            <span class="sensor-label">Verim</span>
                            <div class="sensor-bar">
                                <div class="sensor-bar-fill normal"
                                     style="width: ${latest.throughput || 0}%"></div>
                            </div>
                            <span class="sensor-value">${latest.throughput || 0}</span>
                        </div>
                    </div>
                </div>
                <div class="card-footer">
                    <div class="prediction-section">
                        <div class="gauge-container">
                            <svg class="gauge-svg" viewBox="0 0 60 60" width="60" height="60">
                                <circle class="gauge-bg" cx="30" cy="30" r="24"/>
                                <circle class="gauge-fill ${riskLevel}" cx="30" cy="30" r="24"
                                        stroke-dasharray="${circumference}"
                                        stroke-dashoffset="${offset}"/>
                            </svg>
                            <span class="gauge-text">${Math.round(prediction * 100)}%</span>
                        </div>
                        <div class="prediction-info">
                            <div class="label">Ariza Riski</div>
                            <div class="risk-level ${riskLevel}">${getRiskText(riskLevel)}</div>
                        </div>
                    </div>
                </div>
            </div>
        `;
    }

    function renderAlarmItem(alarm) {
        const timeStr = new Date(alarm.timestamp * 1000).toLocaleTimeString('tr-TR');
        const ackClass = alarm.acknowledged ? 'acknowledged' : '';
        return `
            <div class="alarm-item ${ackClass}" data-id="${alarm.id}">
                <div class="alarm-icon ${alarm.severity}">
                    ${alarm.severity === 'critical' ? '&#9888;' : '&#9888;'}
                </div>
                <div class="alarm-content">
                    <div class="message">${alarm.machine_code} - ${alarm.message}</div>
                    <div class="meta">${timeStr} | ${alarm.severity.toUpperCase()}</div>
                </div>
                ${!alarm.acknowledged ? `<button class="btn-ack" onclick="SmartFactory.ackAlarm(${alarm.id})">Onayla</button>` : ''}
            </div>
        `;
    }

    function renderMachinesGrid() {
        const grid = document.getElementById('machines-grid');
        const detailGrid = document.getElementById('machines-detail-grid');
        if (!grid) return;

        const html = Object.values(state.machines).map(renderMachineCard).join('');
        grid.innerHTML = html;
        if (detailGrid) detailGrid.innerHTML = html;
    }

    function renderAlarms() {
        const list = document.getElementById('alarms-list');
        const listFull = document.getElementById('alarms-list-full');
        const count = document.getElementById('alarm-count');
        const countFull = document.getElementById('alarm-count-full');
        const kpiAlarms = document.getElementById('kpi-alarms');

        const unacked = state.alarms.filter(a => !a.acknowledged);
        const recent = state.alarms.slice(-10).reverse();

        if (list) {
            list.innerHTML = recent.length
                ? recent.map(renderAlarmItem).join('')
                : '<div class="alarm-item" style="justify-content: center; color: var(--text-muted);">Alarm yok</div>';
        }

        if (listFull) {
            const all = [...state.alarms].reverse();
            listFull.innerHTML = all.length
                ? all.map(renderAlarmItem).join('')
                : '<div class="alarm-item" style="justify-content: center; color: var(--text-muted);">Alarm yok</div>';
        }

        if (count) count.textContent = unacked.length;
        if (countFull) countFull.textContent = state.alarms.length;
        if (kpiAlarms) {
            kpiAlarms.textContent = unacked.length;
            kpiAlarms.className = 'kpi-value ' + (unacked.length > 5 ? 'bad' : unacked.length > 0 ? 'warning' : 'good');
        }
    }

    function renderKPIs() {
        const oeeEl = document.getElementById('kpi-oee');
        const mttrEl = document.getElementById('kpi-mttr');

        if (oeeEl && state.kpis.oee) {
            const avgOee = Object.values(state.kpis.oee).reduce((sum, m) => sum + (m.oee || 0), 0) / 2;
            oeeEl.textContent = (avgOee * 100).toFixed(0) + '%';
            oeeEl.className = 'kpi-value ' + (avgOee >= 0.85 ? 'good' : avgOee >= 0.7 ? 'warning' : 'bad');
        }

        if (mttrEl && state.kpis.mttr) {
            const avgMttr = Object.values(state.kpis.mttr).reduce((sum, m) => sum + (m.mttr_minutes || 0), 0) / 2;
            mttrEl.textContent = avgMttr.toFixed(1);
        }
    }

    function renderSystemStatus() {
        const s = state.systemStatus;

        const updateDot = (id, status) => {
            const dot = document.getElementById(id);
            const text = document.getElementById(id + '-text');
            if (dot) {
                dot.className = 'status-dot ' + (status === 'running' ? '' : 'stopped');
            }
            if (text) {
                text.textContent = status === 'running' ? 'Running' : 'Stopped';
            }
        };

        updateDot('status-broker', s.broker);
        updateDot('status-sim', s.simulation);
        updateDot('status-consumer', s.consumer);
        updateDot('status-listener', s.listener);

        const uptime = document.getElementById('kpi-uptime');
        if (uptime && s.uptime_seconds) {
            uptime.textContent = formatUptime(s.uptime_seconds);
        }

        const connDot = document.getElementById('connection-dot');
        const connText = document.getElementById('connection-text');
        const footerDot = document.getElementById('footer-dot');
        const footerStatus = document.getElementById('footer-status');

        const isConnected = s.simulation === 'running';
        if (connDot) connDot.className = 'status-dot ' + (isConnected ? '' : 'stopped');
        if (connText) connText.textContent = isConnected ? 'Connected' : 'Disconnected';
        if (footerDot) footerDot.className = 'status-dot ' + (isConnected ? '' : 'stopped');
        if (footerStatus) footerStatus.textContent = isConnected ? 'Bagli' : 'Baglanti yok';
    }

    function renderMessages() {
        const list = document.getElementById('messages-list');
        if (!list || !state.messages) return;

        list.innerHTML = state.messages.length
            ? state.messages.slice(-30).reverse().map(m => `<div class="message-line">${m}</div>`).join('')
            : '<div class="message-line">Mesaj bekleniyor...</div>';
    }

    // Chart rendering
    function drawChart() {
        const canvas = document.getElementById('chart-canvas');
        if (!canvas) return;

        const ctx = canvas.getContext('2d');
        const rect = canvas.parentElement.getBoundingClientRect();
        canvas.width = rect.width - 40;
        canvas.height = rect.height - 40;

        const { width, height } = canvas;
        const padding = 40;
        const data = state.chartData.MX100 || [];

        // Clear
        ctx.fillStyle = '#0a0e14';
        ctx.fillRect(0, 0, width, height);

        if (data.length < 2) {
            ctx.fillStyle = '#8892a0';
            ctx.font = '14px Segoe UI';
            ctx.textAlign = 'center';
            ctx.fillText('Veri bekleniyor...', width / 2, height / 2);
            return;
        }

        // Calculate scales
        const temps = data.map(d => d.temperature);
        const minVal = Math.min(...temps) - 5;
        const maxVal = Math.max(...temps) + 5;
        const xStep = (width - padding * 2) / (data.length - 1);
        const yScale = (height - padding * 2) / (maxVal - minVal);

        // Draw grid
        ctx.strokeStyle = '#2a3a4a';
        ctx.lineWidth = 0.5;
        for (let i = 0; i <= 5; i++) {
            const y = padding + (height - padding * 2) * i / 5;
            ctx.beginPath();
            ctx.moveTo(padding, y);
            ctx.lineTo(width - padding, y);
            ctx.stroke();

            const val = maxVal - (maxVal - minVal) * i / 5;
            ctx.fillStyle = '#8892a0';
            ctx.font = '10px Consolas';
            ctx.textAlign = 'right';
            ctx.fillText(val.toFixed(1), padding - 5, y + 3);
        }

        // Draw line
        ctx.beginPath();
        ctx.strokeStyle = '#00d4ff';
        ctx.lineWidth = 2;

        data.forEach((d, i) => {
            const x = padding + i * xStep;
            const y = height - padding - (d.temperature - minVal) * yScale;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });

        ctx.stroke();

        // Glow effect
        ctx.shadowColor = '#00d4ff';
        ctx.shadowBlur = 10;
        ctx.stroke();
        ctx.shadowBlur = 0;

        // Draw points
        ctx.fillStyle = '#00d4ff';
        data.slice(-10).forEach((d, i) => {
            const idx = data.length - 10 + i;
            if (idx < 0) return;
            const x = padding + idx * xStep;
            const y = height - padding - (d.temperature - minVal) * yScale;
            ctx.beginPath();
            ctx.arc(x, y, 3, 0, Math.PI * 2);
            ctx.fill();
        });
    }

    // Update functions
    async function updateMachines() {
        const data = await API.getMachines();
        if (data && data.machines) {
            data.machines.forEach(m => {
                state.machines[m.code] = m;
                if (m.latest) {
                    if (!state.chartData[m.code]) state.chartData[m.code] = [];
                    state.chartData[m.code].push(m.latest);
                    if (state.chartData[m.code].length > CONFIG.CHART_POINTS) {
                        state.chartData[m.code].shift();
                    }
                }
            });
            renderMachinesGrid();
            drawChart();
        }
    }

    async function updateKPIs() {
        const data = await API.getKPIs();
        if (data) {
            state.kpis = data;
            renderKPIs();
        }
    }

    async function updateAlarms() {
        const data = await API.getAlarms();
        if (data && data.alarms) {
            state.alarms = data.alarms;
            renderAlarms();
        }
    }

    async function updateSystemStatus() {
        const data = await API.getSystemStatus();
        if (data) {
            state.systemStatus = data;
            renderSystemStatus();
        }
    }

    async function updateMessages() {
        const data = await API.getMessages();
        if (data && data.messages) {
            state.messages = data.messages;
            renderMessages();
        }
    }

    function updateClock() {
        const clock = document.getElementById('clock');
        const footerUpdate = document.getElementById('footer-update');
        const now = new Date();
        if (clock) clock.textContent = formatTime(now);
        if (footerUpdate) footerUpdate.textContent = 'Son guncelleme: ' + formatTime(now);
    }

    // Public methods
    async function startSystem() {
        await API.startSystem();
        await updateSystemStatus();
        await updateMachines();
    }

    async function ackAlarm(id) {
        await API.ackAlarm(id);
        await updateAlarms();
    }

    async function refreshAll() {
        await Promise.all([
            updateMachines(),
            updateKPIs(),
            updateAlarms(),
            updateSystemStatus(),
            updateMessages()
        ]);
    }

    // Tab switching
    function setupTabs() {
        document.querySelectorAll('.nav-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

                tab.classList.add('active');
                const tabId = 'tab-' + tab.dataset.tab;
                document.getElementById(tabId)?.classList.add('active');
                state.activeTab = tab.dataset.tab;

                if (tab.dataset.tab === 'machines') {
                    setTimeout(drawChart, 100);
                }
            });
        });
    }

    // Initialize
    function init() {
        setupTabs();
        updateClock();
        setInterval(updateClock, 1000);

        // Initial load
        refreshAll();

        // Polling intervals
        setInterval(updateMachines, CONFIG.UPDATE_INTERVAL);
        setInterval(updateKPIs, CONFIG.KPI_INTERVAL);
        setInterval(updateAlarms, CONFIG.ALARM_INTERVAL);
        setInterval(updateSystemStatus, 3000);
        setInterval(updateMessages, 2000);

        // Resize handler for chart
        window.addEventListener('resize', () => {
            setTimeout(drawChart, 100);
        });

        console.log('SmartFactory Dashboard initialized');
    }

    return {
        init,
        startSystem,
        ackAlarm,
        refreshAll
    };
})();

document.addEventListener('DOMContentLoaded', SmartFactory.init);
"""


@app.get("/", response_class=HTMLResponse)
def index():
    return f"""<!DOCTYPE html>
<html lang="tr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SmartFact - Industry 4.0 Dashboard</title>
    <style>{CSS}</style>
</head>
<body>
    {HTML}
    <script>{JS}</script>
</body>
</html>"""


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run("ui:app", host="0.0.0.0", port=8000, reload=False)
