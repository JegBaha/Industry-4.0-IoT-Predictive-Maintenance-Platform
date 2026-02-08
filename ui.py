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
from rag.rag_api import router as rag_router, init_rag
from erp_mes.erp_api import router as erp_router

log = logging.getLogger(__name__)

app = FastAPI(title="SmartFact Dashboard")
app.include_router(rag_router)
app.include_router(erp_router)

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
    init_rag()
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

/* Mode Switcher */
.mode-switcher {
    display: flex;
    gap: 4px;
    background: var(--bg-primary);
    border-radius: 8px;
    padding: 3px;
    margin-right: 16px;
}

.mode-btn {
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    background: transparent;
    color: var(--text-secondary);
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: all 0.2s;
    white-space: nowrap;
}

.mode-btn:hover {
    color: var(--text-primary);
    background: var(--bg-hover);
}

.mode-btn.active {
    background: var(--accent-blue);
    color: #fff;
    box-shadow: 0 2px 8px rgba(30, 144, 255, 0.3);
}

/* ERP Navigation */
.nav-tabs-erp {
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border-color);
    display: none;
    padding: 0 24px;
    gap: 4px;
}

.nav-tabs-erp.active {
    display: flex;
}

.nav-tabs-iot {
    display: flex;
}

.nav-tabs-iot.hidden {
    display: none;
}

/* ERP KPI Cards */
.erp-kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}

.erp-kpi-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 20px;
    text-align: center;
    transition: all 0.3s;
}

.erp-kpi-card:hover {
    border-color: var(--accent-blue);
    box-shadow: var(--glow-blue);
}

.erp-kpi-card .kpi-icon {
    font-size: 28px;
    margin-bottom: 8px;
}

.erp-kpi-card h4 {
    font-size: 13px;
    color: var(--text-secondary);
    margin-bottom: 8px;
    font-weight: 500;
}

.erp-kpi-card .kpi-value {
    font-size: 32px;
    font-weight: 700;
    margin-bottom: 4px;
}

.erp-kpi-card .kpi-subtitle {
    font-size: 11px;
    color: var(--text-muted);
}

.erp-kpi-card .kpi-value.good { color: var(--accent-green); }
.erp-kpi-card .kpi-value.warning { color: var(--accent-yellow); }
.erp-kpi-card .kpi-value.bad { color: var(--accent-red); }

/* ERP Charts Grid */
.erp-charts-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 24px;
}

/* Orders Table */
.orders-table-wrapper {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    overflow: hidden;
}

.orders-table-header {
    background: var(--bg-tertiary);
    padding: 16px 20px;
    border-bottom: 1px solid var(--border-color);
    display: flex;
    align-items: center;
    justify-content: space-between;
}

.orders-table-header h3 {
    font-size: 16px;
    font-weight: 600;
}

.orders-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}

.orders-table thead th {
    background: var(--bg-tertiary);
    padding: 12px 16px;
    text-align: left;
    font-weight: 600;
    color: var(--text-secondary);
    border-bottom: 1px solid var(--border-color);
    position: sticky;
    top: 0;
}

.orders-table tbody tr {
    border-bottom: 1px solid var(--border-color);
    transition: background 0.15s;
}

.orders-table tbody tr:hover {
    background: var(--bg-hover);
}

.orders-table td {
    padding: 10px 16px;
    color: var(--text-primary);
}

.orders-table .badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
}

.orders-table .badge.good { background: rgba(0,255,136,0.15); color: var(--accent-green); }
.orders-table .badge.warning { background: rgba(255,193,7,0.15); color: var(--accent-yellow); }
.orders-table .badge.bad { background: rgba(255,59,59,0.15); color: var(--accent-red); }

.orders-scroll {
    max-height: 500px;
    overflow-y: auto;
}

/* Predict Form */
.erp-predict-layout {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}

.erp-form-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 24px;
}

.erp-form-card h3 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 20px;
    color: var(--accent-cyan);
}

.form-group {
    margin-bottom: 16px;
}

.form-group label {
    display: block;
    font-size: 12px;
    color: var(--text-secondary);
    margin-bottom: 6px;
    font-weight: 500;
}

.form-group input,
.form-group select {
    width: 100%;
    padding: 10px 14px;
    background: var(--bg-primary);
    border: 1px solid var(--border-color);
    border-radius: 8px;
    color: var(--text-primary);
    font-size: 14px;
    outline: none;
    transition: border-color 0.2s;
}

.form-group input:focus,
.form-group select:focus {
    border-color: var(--accent-cyan);
}

/* Prediction Result */
.predict-result-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 24px;
}

.predict-result-card h3 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 20px;
    color: var(--accent-cyan);
}

.predict-gauge {
    text-align: center;
    margin-bottom: 20px;
}

.predict-prob-bar {
    height: 24px;
    background: var(--bg-primary);
    border-radius: 12px;
    overflow: hidden;
    margin: 12px 0;
}

.predict-prob-fill {
    height: 100%;
    border-radius: 12px;
    transition: width 0.5s ease;
}

.predict-detail-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
    margin-top: 16px;
}

.predict-detail-item {
    background: var(--bg-primary);
    border-radius: 8px;
    padding: 12px;
    text-align: center;
}

.predict-detail-item .label {
    font-size: 11px;
    color: var(--text-muted);
    margin-bottom: 4px;
}

.predict-detail-item .value {
    font-size: 20px;
    font-weight: 700;
}

/* Analytics */
.erp-analytics-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}

.analytics-card {
    background: var(--bg-secondary);
    border: 1px solid var(--border-color);
    border-radius: 12px;
    padding: 24px;
}

.analytics-card h3 {
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
    color: var(--accent-cyan);
}

.finding-item {
    background: var(--bg-primary);
    border-radius: 8px;
    padding: 14px;
    margin-bottom: 12px;
    border-left: 3px solid var(--accent-blue);
}

.finding-item.high { border-left-color: var(--accent-red); }
.finding-item.medium { border-left-color: var(--accent-yellow); }
.finding-item.low { border-left-color: var(--accent-green); }

.finding-item h4 {
    font-size: 14px;
    font-weight: 600;
    margin-bottom: 6px;
}

.finding-item p {
    font-size: 13px;
    color: var(--text-secondary);
    line-height: 1.5;
}

.recommendation-list {
    list-style: none;
    padding: 0;
}

.recommendation-list li {
    padding: 10px 14px;
    background: var(--bg-primary);
    border-radius: 8px;
    margin-bottom: 8px;
    font-size: 13px;
    color: var(--text-primary);
    display: flex;
    align-items: center;
    gap: 10px;
}

.recommendation-list li::before {
    content: "\\2713";
    color: var(--accent-green);
    font-weight: bold;
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

    .erp-charts-grid,
    .erp-predict-layout,
    .erp-analytics-grid {
        grid-template-columns: 1fr;
    }

    .mode-switcher {
        margin-right: 0;
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
    <div class="mode-switcher">
        <button class="mode-btn active" data-mode="iot" onclick="SmartFactory.switchMode('iot')">IoT Izleme</button>
        <button class="mode-btn" data-mode="erp" onclick="SmartFactory.switchMode('erp')">ERP / MES</button>
    </div>
    <div class="header-status">
        <div class="status-indicator">
            <div class="status-dot" id="connection-dot"></div>
            <span id="connection-text">Connecting...</span>
        </div>
        <div class="clock" id="clock">--:--:--</div>
    </div>
</div>

<nav class="nav-tabs nav-tabs-iot" id="nav-iot">
    <button class="nav-tab active" data-tab="dashboard">Dashboard</button>
    <button class="nav-tab" data-tab="machines">Makineler</button>
    <button class="nav-tab" data-tab="control">Kontrol Paneli</button>
    <button class="nav-tab" data-tab="alarms">Alarmlar</button>
    <button class="nav-tab" data-tab="rag">RAG Asistan</button>
</nav>
<nav class="nav-tabs nav-tabs-erp" id="nav-erp">
    <button class="nav-tab active" data-tab="erp-dashboard">KPI Dashboard</button>
    <button class="nav-tab" data-tab="erp-oee">OEE Detay</button>
    <button class="nav-tab" data-tab="erp-downtime">Durus Analizi</button>
    <button class="nav-tab" data-tab="erp-suppliers">Tedarikci</button>
    <button class="nav-tab" data-tab="erp-orders">Siparisler</button>
    <button class="nav-tab" data-tab="erp-predict">Hata Tahmini</button>
    <button class="nav-tab" data-tab="erp-analytics">Analitik</button>
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

    <!-- RAG Asistan Tab -->
    <div class="tab-content" id="tab-rag">
        <div class="control-panel">
            <!-- Soru-Cevap Paneli -->
            <div class="control-card" style="grid-column: 1 / -1;">
                <h3>&#128218; Dokuman Tabanli Akilli Asistan</h3>
                <p style="color: var(--text-secondary); font-size: 13px; margin-bottom: 16px;">
                    ISO 10816 titresim standartlari, OEE hesaplamalari, alarm esikleri ve bakim rehberleri hakkinda soru sorun.
                </p>
                <div style="display: flex; gap: 12px; margin-bottom: 16px;">
                    <input type="text" id="rag-question" placeholder="Ornek: ISO 10816'ya gore 6.2 mm/s titresim degeri hangi seviyede?"
                        style="flex: 1; padding: 12px 16px; background: var(--bg-primary); border: 1px solid var(--border-color);
                        border-radius: 8px; color: var(--text-primary); font-size: 14px; outline: none;"
                        onkeypress="if(event.key==='Enter') SmartFactory.ragQuery()">
                    <button class="btn btn-primary" onclick="SmartFactory.ragQuery()">
                        <span>&#128269;</span> Sor
                    </button>
                </div>

                <!-- Hizli Sorular -->
                <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 16px;">
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('ISO 10816 titresim limitleri nelerdir?')">
                        Titresim Limitleri
                    </button>
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('OEE nasil hesaplanir? World class degerler nedir?')">
                        OEE Hesaplama
                    </button>
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('Rulman ariza belirtileri ve frekanslari nelerdir?')">
                        Rulman Ariza Teshisi
                    </button>
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('MTBF ve MTTR nedir? Nasil hesaplanir?')">
                        MTBF / MTTR
                    </button>
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('Alarm yonetimi standartlari ve oncelik seviyeleri nelerdir?')">
                        Alarm Yonetimi
                    </button>
                    <button class="btn btn-secondary" style="font-size: 12px; padding: 8px 12px;"
                        onclick="SmartFactory.ragQuickQuery('OPC UA nedir? MQTT ile farki nedir?')">
                        OPC UA vs MQTT
                    </button>
                </div>

                <!-- Cevap Alani -->
                <div id="rag-answer-panel" style="display: none;">
                    <div style="background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 8px; padding: 20px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                            <h4 style="color: var(--accent-cyan); font-size: 14px;">Cevap</h4>
                            <span id="rag-confidence" style="font-size: 11px; padding: 2px 8px; border-radius: 10px; background: var(--bg-tertiary); color: var(--text-secondary);"></span>
                        </div>
                        <div id="rag-answer" style="color: var(--text-primary); font-size: 14px; line-height: 1.8; white-space: pre-wrap;"></div>
                        <div id="rag-sources" style="margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--border-color);"></div>
                    </div>
                </div>
                <div id="rag-loading" style="display: none; text-align: center; padding: 20px; color: var(--text-secondary);">
                    <div class="pulse">Dokumanlar aranip analiz ediliyor...</div>
                </div>
            </div>

            <!-- Alarm Analizi -->
            <div class="control-card">
                <h3>&#9888; Sensor Deger Analizi</h3>
                <p style="color: var(--text-secondary); font-size: 13px; margin-bottom: 16px;">
                    Sensor degerini girin, standartlara gore analiz yapilsin.
                </p>
                <div style="display: grid; gap: 12px;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px;">
                        <div>
                            <label style="font-size: 12px; color: var(--text-secondary); display: block; margin-bottom: 4px;">Makine Kodu</label>
                            <input type="text" id="rag-alarm-machine" value="MX100" style="width: 100%; padding: 10px; background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 6px; color: var(--text-primary); font-size: 13px;">
                        </div>
                        <div>
                            <label style="font-size: 12px; color: var(--text-secondary); display: block; margin-bottom: 4px;">Sensor Tipi</label>
                            <select id="rag-alarm-sensor" style="width: 100%; padding: 10px; background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 6px; color: var(--text-primary); font-size: 13px;">
                                <option value="vibration">Titresim (Vibration)</option>
                                <option value="temperature">Sicaklik (Temperature)</option>
                                <option value="current">Akim (Current)</option>
                                <option value="pressure">Basinc (Pressure)</option>
                            </select>
                        </div>
                    </div>
                    <div style="display: grid; grid-template-columns: 2fr 1fr; gap: 12px;">
                        <div>
                            <label style="font-size: 12px; color: var(--text-secondary); display: block; margin-bottom: 4px;">Deger</label>
                            <input type="number" step="0.1" id="rag-alarm-value" value="6.2" style="width: 100%; padding: 10px; background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 6px; color: var(--text-primary); font-size: 13px;">
                        </div>
                        <div>
                            <label style="font-size: 12px; color: var(--text-secondary); display: block; margin-bottom: 4px;">Birim</label>
                            <input type="text" id="rag-alarm-unit" value="mm/s" style="width: 100%; padding: 10px; background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 6px; color: var(--text-primary); font-size: 13px;">
                        </div>
                    </div>
                    <button class="btn btn-primary" onclick="SmartFactory.ragAnalyzeAlarm()" style="width: 100%;">
                        <span>&#128300;</span> Analiz Et
                    </button>
                </div>
                <div id="rag-alarm-result" style="margin-top: 16px;"></div>
            </div>

            <!-- Dokuman Yonetimi -->
            <div class="control-card">
                <h3>&#128196; Dokuman Yonetimi</h3>
                <p style="color: var(--text-secondary); font-size: 13px; margin-bottom: 16px;">
                    PDF veya TXT dokumanlarini yukleyerek bilgi bankasini genisletin.
                </p>
                <div style="display: grid; gap: 12px;">
                    <div style="border: 2px dashed var(--border-color); border-radius: 8px; padding: 24px; text-align: center; cursor: pointer;"
                         onclick="document.getElementById('rag-file-input').click()">
                        <div style="font-size: 24px; margin-bottom: 8px;">&#128206;</div>
                        <div style="color: var(--text-secondary); font-size: 13px;">PDF veya TXT dosya yukle</div>
                        <input type="file" id="rag-file-input" accept=".pdf,.txt,.md" style="display: none;" onchange="SmartFactory.ragUploadFile(this)">
                    </div>
                    <button class="btn btn-secondary" onclick="SmartFactory.ragSaveBuiltin()" style="width: 100%;">
                        <span>&#128190;</span> Yerlesik Dokumanlari Kaydet
                    </button>
                </div>

                <!-- RAG Durumu -->
                <div style="margin-top: 16px; padding-top: 12px; border-top: 1px solid var(--border-color);">
                    <h4 style="font-size: 13px; color: var(--text-secondary); margin-bottom: 8px;">Bilgi Bankasi</h4>
                    <div class="status-list" id="rag-status-list">
                        <div class="status-row">
                            <span class="label">Durum</span>
                            <span class="value" id="rag-status-state">Yukleniyor...</span>
                        </div>
                        <div class="status-row">
                            <span class="label">Toplam Chunk</span>
                            <span class="value" id="rag-status-chunks">-</span>
                        </div>
                        <div class="status-row">
                            <span class="label">Yuklu Dokumanlar</span>
                            <span class="value" id="rag-status-docs">-</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- ===== ERP/MES TABS ===== -->

    <!-- ERP Dashboard -->
    <div class="tab-content" id="tab-erp-dashboard">
        <div class="erp-kpi-grid" id="erp-kpi-grid">
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#127919;</div>
                <h4>Plan Gerceklesme</h4>
                <div class="kpi-value good" id="erp-kpi-fulfillment">--%</div>
                <div class="kpi-subtitle">Hedef: >= %95</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#9202;</div>
                <h4>Ort. Gecikme</h4>
                <div class="kpi-value" id="erp-kpi-delay">-- saat</div>
                <div class="kpi-subtitle">Hedef: <= 2 saat</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#128295;</div>
                <h4>Hurda Orani</h4>
                <div class="kpi-value" id="erp-kpi-scrap">--%</div>
                <div class="kpi-subtitle">Hedef: <= %2</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#128230;</div>
                <h4>Toplam Siparis</h4>
                <div class="kpi-value" id="erp-kpi-orders">--</div>
                <div class="kpi-subtitle" id="erp-kpi-produced">Uretilen: --</div>
            </div>
        </div>

        <div class="erp-charts-grid">
            <div class="chart-panel">
                <div class="panel-header">
                    <h3>Feature Importance (Ozellik Onemi)</h3>
                </div>
                <div class="chart-container">
                    <canvas id="erp-chart-importance" class="chart-canvas"></canvas>
                </div>
            </div>
            <div class="chart-panel">
                <div class="panel-header">
                    <h3>Sicaklik - Hata Olasiligi Egrisi</h3>
                </div>
                <div class="chart-container">
                    <canvas id="erp-chart-tempcurve" class="chart-canvas"></canvas>
                </div>
            </div>
        </div>
    </div>

    <!-- ERP Orders -->
    <div class="tab-content" id="tab-erp-orders">
        <div class="orders-table-wrapper">
            <div class="orders-table-header">
                <h3>Birlesik Siparisler (ERP + MES)</h3>
                <span class="alarm-count" id="erp-order-count">0</span>
            </div>
            <div class="orders-scroll">
                <table class="orders-table">
                    <thead>
                        <tr>
                            <th>Siparis No</th>
                            <th>Planlanan</th>
                            <th>Uretilen</th>
                            <th>Hata</th>
                            <th>Gerceklesme</th>
                            <th>Gecikme (saat)</th>
                            <th>Hurda Orani</th>
                        </tr>
                    </thead>
                    <tbody id="erp-orders-body">
                        <tr><td colspan="7" style="text-align:center; color: var(--text-muted); padding: 40px;">Yukleniyor...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- ERP Predict -->
    <div class="tab-content" id="tab-erp-predict">
        <div class="erp-predict-layout">
            <div class="erp-form-card">
                <h3>Hata Tahmin Parametreleri</h3>
                <div class="form-group">
                    <label>Sicaklik (C)</label>
                    <input type="number" id="erp-pred-temp" value="85" min="50" max="120" step="0.5">
                </div>
                <div class="form-group">
                    <label>Hat Hizi (birim/dk)</label>
                    <input type="number" id="erp-pred-speed" value="90" min="50" max="150" step="1">
                </div>
                <div class="form-group">
                    <label>Vardiya</label>
                    <select id="erp-pred-shift">
                        <option value="Day">Gunduz</option>
                        <option value="Night">Gece</option>
                    </select>
                </div>
                <div class="form-group">
                    <label>Operator Deneyimi (yil)</label>
                    <input type="number" id="erp-pred-exp" value="5" min="0" max="30" step="1">
                </div>
                <div class="form-group">
                    <label>Makine Yasi (ay)</label>
                    <input type="number" id="erp-pred-age" value="24" min="0" max="120" step="1">
                </div>
                <button class="btn btn-primary" onclick="SmartFactory.erpPredict()" style="width:100%; margin-top:8px;">
                    Tahmin Yap
                </button>
            </div>

            <div class="predict-result-card">
                <h3>Tahmin Sonucu</h3>
                <div id="erp-predict-result">
                    <div style="text-align:center; color: var(--text-muted); padding: 60px 0;">
                        Parametre girin ve "Tahmin Yap" butonuna basin.
                    </div>
                </div>
                <div class="erp-charts-grid" style="margin-top: 20px;">
                    <div class="chart-panel" style="margin-bottom:0;">
                        <div class="panel-header"><h3>Feature Importance</h3></div>
                        <div class="chart-container"><canvas id="erp-chart-importance2" class="chart-canvas"></canvas></div>
                    </div>
                    <div class="chart-panel" style="margin-bottom:0;">
                        <div class="panel-header"><h3>Sicaklik Egrisi</h3></div>
                        <div class="chart-container"><canvas id="erp-chart-tempcurve2" class="chart-canvas"></canvas></div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- ERP OEE Detail -->
    <div class="tab-content" id="tab-erp-oee">
        <div class="erp-kpi-grid" style="grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));">
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#9881;</div>
                <h4>OEE</h4>
                <div class="kpi-value" id="oee-val-oee">--%</div>
                <div class="kpi-subtitle">Hedef: >= %85</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#9200;</div>
                <h4>Availability</h4>
                <div class="kpi-value" id="oee-val-avail">--%</div>
                <div class="kpi-subtitle">Hedef: >= %90</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#9889;</div>
                <h4>Performance</h4>
                <div class="kpi-value" id="oee-val-perf">--%</div>
                <div class="kpi-subtitle">Hedef: >= %95</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#10003;</div>
                <h4>Quality</h4>
                <div class="kpi-value" id="oee-val-qual">--%</div>
                <div class="kpi-subtitle">Hedef: >= %99</div>
            </div>
        </div>

        <div class="erp-charts-grid">
            <div class="erp-kpi-grid" style="grid-template-columns: 1fr 1fr;">
                <div class="erp-kpi-card">
                    <h4>MTTR (Ort. Tamir Suresi)</h4>
                    <div class="kpi-value" id="oee-val-mttr" style="color: var(--accent-yellow);">-- dk</div>
                    <div class="kpi-subtitle" id="oee-mttr-hours">-- saat</div>
                </div>
                <div class="erp-kpi-card">
                    <h4>MTBF (Arizalar Arasi Sure)</h4>
                    <div class="kpi-value" id="oee-val-mtbf" style="color: var(--accent-cyan);">-- dk</div>
                    <div class="kpi-subtitle" id="oee-mtbf-hours">-- saat</div>
                </div>
            </div>
            <div class="erp-kpi-grid" style="grid-template-columns: 1fr 1fr;">
                <div class="erp-kpi-card">
                    <h4>Ariza Sayisi</h4>
                    <div class="kpi-value" id="oee-val-failures" style="color: var(--accent-red);">--</div>
                    <div class="kpi-subtitle">Planlanmamis duruslar</div>
                </div>
                <div class="erp-kpi-card">
                    <h4>Toplam Tamir Suresi</h4>
                    <div class="kpi-value" id="oee-val-totalrepair" style="color: var(--accent-orange);">-- dk</div>
                    <div class="kpi-subtitle">Kumulatif</div>
                </div>
            </div>
        </div>

        <div class="chart-panel">
            <div class="panel-header">
                <h3>Haftalik OEE Trendi</h3>
            </div>
            <div class="chart-container" style="height: 300px;">
                <canvas id="oee-chart-weekly" class="chart-canvas"></canvas>
            </div>
        </div>

        <div class="erp-kpi-grid" style="margin-top: 20px; grid-template-columns: 1fr 1fr 1fr;">
            <div class="erp-kpi-card">
                <h4>Planlanan Sure</h4>
                <div class="kpi-value" id="oee-val-planned" style="font-size: 24px;">-- dk</div>
            </div>
            <div class="erp-kpi-card">
                <h4>Calisma Suresi</h4>
                <div class="kpi-value" id="oee-val-operating" style="font-size: 24px;">-- dk</div>
            </div>
            <div class="erp-kpi-card">
                <h4>Planlanmamis Durus</h4>
                <div class="kpi-value" id="oee-val-unplanned" style="font-size: 24px; color: var(--accent-red);">-- dk</div>
            </div>
        </div>
    </div>

    <!-- ERP Downtime Analysis -->
    <div class="tab-content" id="tab-erp-downtime">
        <div class="erp-charts-grid">
            <div class="chart-panel">
                <div class="panel-header">
                    <h3>Durus Pareto Analizi</h3>
                </div>
                <div class="chart-container" style="height: 350px;">
                    <canvas id="downtime-chart-pareto" class="chart-canvas"></canvas>
                </div>
            </div>
            <div class="orders-table-wrapper">
                <div class="orders-table-header">
                    <h3>Durus Sebepleri Detay</h3>
                </div>
                <div class="orders-scroll" style="max-height: 350px;">
                    <table class="orders-table">
                        <thead>
                            <tr>
                                <th>Kod</th>
                                <th>Sebep</th>
                                <th>Kategori</th>
                                <th>Sure (dk)</th>
                                <th>Adet</th>
                                <th>Oran</th>
                                <th>Kumulatif</th>
                            </tr>
                        </thead>
                        <tbody id="downtime-table-body">
                            <tr><td colspan="7" style="text-align:center; color: var(--text-muted); padding: 40px;">Yukleniyor...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- ERP Supplier Performance -->
    <div class="tab-content" id="tab-erp-suppliers">
        <div class="erp-kpi-grid" style="grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));">
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#128666;</div>
                <h4>Zamaninda Teslimat</h4>
                <div class="kpi-value" id="sup-val-ontime">--%</div>
                <div class="kpi-subtitle">Hedef: >= %95</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#128197;</div>
                <h4>Ort. Gecikme</h4>
                <div class="kpi-value" id="sup-val-delay">-- gun</div>
                <div class="kpi-subtitle">Hedef: <= 2 gun</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#128230;</div>
                <h4>Toplam Teslimat</h4>
                <div class="kpi-value" id="sup-val-total">--</div>
                <div class="kpi-subtitle" id="sup-val-late">Geciken: --</div>
            </div>
            <div class="erp-kpi-card">
                <div class="kpi-icon">&#127981;</div>
                <h4>Tedarikci Sayisi</h4>
                <div class="kpi-value" id="sup-val-count">--</div>
                <div class="kpi-subtitle">Aktif tedarikci</div>
            </div>
        </div>

        <div class="orders-table-wrapper">
            <div class="orders-table-header">
                <h3>Tedarikci Performans Tablosu</h3>
            </div>
            <div class="orders-scroll" style="max-height: 400px;">
                <table class="orders-table">
                    <thead>
                        <tr>
                            <th>Tedarikci</th>
                            <th>Ulke</th>
                            <th>Siparis</th>
                            <th>Zamaninda %</th>
                            <th>Ort. Gecikme</th>
                            <th>Kalite Skoru</th>
                        </tr>
                    </thead>
                    <tbody id="supplier-table-body">
                        <tr><td colspan="6" style="text-align:center; color: var(--text-muted); padding: 40px;">Yukleniyor...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- ERP Analytics -->
    <div class="tab-content" id="tab-erp-analytics">
        <div class="erp-analytics-grid">
            <div class="analytics-card">
                <h3>Model Bilgisi</h3>
                <div id="erp-model-info">
                    <div class="finding-item">
                        <h4>Random Forest Classifier</h4>
                        <p>300 agac, max derinlik: 12</p>
                    </div>
                </div>
                <h3 style="margin-top:20px;">ML Bulgulari</h3>
                <div id="erp-findings">
                    <div style="text-align:center; color: var(--text-muted); padding: 20px;">Yukleniyor...</div>
                </div>
            </div>
            <div class="analytics-card">
                <h3>Oneriler</h3>
                <ul class="recommendation-list" id="erp-recommendations">
                    <li>Yukleniyor...</li>
                </ul>
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
        async post(url, body) {
            try {
                const opts = { method: 'POST' };
                if (body) {
                    opts.headers = { 'Content-Type': 'application/json' };
                    opts.body = JSON.stringify(body);
                }
                const res = await fetch(url, opts);
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
        setupErpTabs();
        updateClock();
        setInterval(updateClock, 1000);

        // Initial load
        refreshAll();
        ragUpdateStatus();

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

    // ─── RAG Functions ─────────────────────────────────────

    async function ragQuery() {
        const question = document.getElementById('rag-question')?.value;
        if (!question) return;

        const answerPanel = document.getElementById('rag-answer-panel');
        const loading = document.getElementById('rag-loading');
        if (answerPanel) answerPanel.style.display = 'none';
        if (loading) loading.style.display = 'block';

        try {
            const res = await fetch('/api/rag/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question, top_k: 5 })
            });
            const data = await res.json();

            if (loading) loading.style.display = 'none';
            if (answerPanel) answerPanel.style.display = 'block';

            const answerEl = document.getElementById('rag-answer');
            const confidenceEl = document.getElementById('rag-confidence');
            const sourcesEl = document.getElementById('rag-sources');

            if (answerEl) answerEl.textContent = data.answer || 'Cevap alinamadi';
            if (confidenceEl) {
                const conf = Math.round((data.confidence || 0) * 100);
                confidenceEl.textContent = 'Guven: ' + conf + '%';
                confidenceEl.style.color = conf > 70 ? 'var(--accent-green)' : conf > 40 ? 'var(--accent-yellow)' : 'var(--accent-red)';
            }
            if (sourcesEl && data.sources && data.sources.length > 0) {
                sourcesEl.innerHTML = '<div style="font-size: 12px; color: var(--text-muted); margin-bottom: 6px;">Kaynaklar:</div>' +
                    data.sources.map(s =>
                        '<div style="font-size: 12px; color: var(--text-secondary); padding: 4px 0;">' +
                        '<span style="color: var(--accent-cyan);">' + s.source + '</span>' +
                        (s.page ? ' (s.' + s.page + ')' : '') +
                        ' - Eslesme: ' + Math.round(s.score * 100) + '%' +
                        '</div>'
                    ).join('');
            }
        } catch (e) {
            if (loading) loading.style.display = 'none';
            if (answerPanel) {
                answerPanel.style.display = 'block';
                document.getElementById('rag-answer').textContent = 'Hata: ' + e.message;
            }
        }
    }

    function ragQuickQuery(question) {
        const input = document.getElementById('rag-question');
        if (input) input.value = question;
        ragQuery();
    }

    async function ragAnalyzeAlarm() {
        const machine_code = document.getElementById('rag-alarm-machine')?.value || 'MX100';
        const sensor_type = document.getElementById('rag-alarm-sensor')?.value || 'vibration';
        const value = parseFloat(document.getElementById('rag-alarm-value')?.value || '0');
        const unit = document.getElementById('rag-alarm-unit')?.value || '';

        const resultDiv = document.getElementById('rag-alarm-result');
        if (resultDiv) resultDiv.innerHTML = '<div class="pulse" style="color: var(--text-secondary); text-align: center; padding: 12px;">Analiz ediliyor...</div>';

        try {
            const res = await fetch('/api/rag/analyze/alarm', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ machine_code, sensor_type, value, unit })
            });
            const data = await res.json();

            const statusColors = { normal: 'var(--accent-green)', warning: 'var(--accent-yellow)', critical: 'var(--accent-red)' };
            const statusColor = statusColors[data.status] || 'var(--text-secondary)';

            resultDiv.innerHTML = `
                <div style="background: var(--bg-primary); border: 1px solid var(--border-color); border-radius: 8px; padding: 16px;">
                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 12px;">
                        <div style="width: 12px; height: 12px; border-radius: 50%; background: ${statusColor}; box-shadow: 0 0 8px ${statusColor};"></div>
                        <span style="font-size: 16px; font-weight: 700; color: ${statusColor}; text-transform: uppercase;">${data.status || 'unknown'}</span>
                    </div>
                    ${data.standard_reference ? '<div style="font-size: 12px; color: var(--text-secondary); margin-bottom: 4px;"><b>Standart:</b> ' + data.standard_reference + '</div>' : ''}
                    ${data.limit_info ? '<div style="font-size: 12px; color: var(--text-secondary); margin-bottom: 4px;"><b>Limitler:</b> ' + data.limit_info + '</div>' : ''}
                    ${data.oee_impact ? '<div style="font-size: 12px; color: var(--text-secondary); margin-bottom: 4px;"><b>OEE Etkisi:</b> ' + data.oee_impact + '</div>' : ''}
                    ${data.root_cause ? '<div style="font-size: 12px; color: var(--text-secondary); margin-bottom: 4px;"><b>Olasi Neden:</b> ' + data.root_cause + '</div>' : ''}
                    ${data.action ? '<div style="font-size: 12px; color: var(--accent-cyan); margin-top: 8px;"><b>Aksiyon:</b> ' + data.action + '</div>' : ''}
                    ${data.explanation ? '<div style="font-size: 13px; color: var(--text-primary); margin-top: 8px; line-height: 1.6;">' + data.explanation + '</div>' : ''}
                </div>
            `;
        } catch (e) {
            resultDiv.innerHTML = '<div style="color: var(--accent-red); padding: 12px;">Hata: ' + e.message + '</div>';
        }
    }

    async function ragUploadFile(input) {
        if (!input.files || !input.files[0]) return;
        const file = input.files[0];
        const formData = new FormData();
        formData.append('file', file);

        try {
            const res = await fetch('/api/rag/documents/upload', {
                method: 'POST',
                body: formData
            });
            const data = await res.json();
            alert('Dokuman yuklendi: ' + file.name + ' (' + (data.chunks || 0) + ' chunk)');
            ragUpdateStatus();
        } catch (e) {
            alert('Yukleme hatasi: ' + e.message);
        }
    }

    async function ragSaveBuiltin() {
        try {
            await fetch('/api/rag/knowledge/save-builtin', { method: 'POST' });
            alert('Yerlesik dokumanlar rag_docs/builtin/ dizinine kaydedildi.');
        } catch (e) {
            alert('Hata: ' + e.message);
        }
    }

    async function ragUpdateStatus() {
        try {
            const res = await fetch('/api/rag/status');
            const data = await res.json();

            const stateEl = document.getElementById('rag-status-state');
            const chunksEl = document.getElementById('rag-status-chunks');
            const docsEl = document.getElementById('rag-status-docs');

            if (stateEl) stateEl.textContent = data.status === 'active' ? 'Aktif' : 'Pasif';
            if (chunksEl) chunksEl.textContent = data.vector_store?.total_chunks || '0';
            if (docsEl) docsEl.textContent = data.total_documents || '0';
        } catch (e) {
            // RAG henuz hazir degil
        }
    }

    // ─── ERP/MES Functions ─────────────────────────────────

    const erpState = {
        currentMode: 'iot',
        kpis: null,
        orders: [],
        analytics: null,
        featureImportance: null,
        temperatureCurve: null
    };

    function switchMode(mode) {
        erpState.currentMode = mode;

        // Toggle mode buttons
        document.querySelectorAll('.mode-btn').forEach(b => {
            b.classList.toggle('active', b.dataset.mode === mode);
        });

        // Toggle nav bars
        const navIot = document.getElementById('nav-iot');
        const navErp = document.getElementById('nav-erp');
        if (mode === 'erp') {
            navIot.classList.add('hidden');
            navErp.classList.add('active');
        } else {
            navIot.classList.remove('hidden');
            navErp.classList.remove('active');
        }

        // Hide all tab contents
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

        // Activate correct tab set
        if (mode === 'erp') {
            // Reset IoT tab active states
            document.querySelectorAll('#nav-iot .nav-tab').forEach(t => t.classList.remove('active'));
            document.querySelector('#nav-iot .nav-tab').classList.add('active');

            // Set ERP first tab active
            document.querySelectorAll('#nav-erp .nav-tab').forEach(t => t.classList.remove('active'));
            document.querySelector('#nav-erp .nav-tab').classList.add('active');
            document.getElementById('tab-erp-dashboard').classList.add('active');

            // Load ERP data
            erpRefreshAll();
        } else {
            // Reset ERP tab active states
            document.querySelectorAll('#nav-erp .nav-tab').forEach(t => t.classList.remove('active'));
            document.querySelector('#nav-erp .nav-tab').classList.add('active');

            // Set IoT first tab active
            document.querySelectorAll('#nav-iot .nav-tab').forEach(t => t.classList.remove('active'));
            document.querySelector('#nav-iot .nav-tab').classList.add('active');
            document.getElementById('tab-dashboard').classList.add('active');
        }
    }

    function setupErpTabs() {
        document.querySelectorAll('#nav-erp .nav-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('#nav-erp .nav-tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                tab.classList.add('active');
                const tabId = 'tab-' + tab.dataset.tab;
                document.getElementById(tabId)?.classList.add('active');

                // Redraw charts when switching to tabs with charts
                if (tab.dataset.tab === 'erp-dashboard' || tab.dataset.tab === 'erp-predict') {
                    setTimeout(() => {
                        drawFeatureImportanceChart();
                        drawTemperatureCurveChart();
                    }, 100);
                }
            });
        });
    }

    async function erpLoadKPIs() {
        const data = await API.get('/api/erp/kpi/summary');
        if (!data) return;
        erpState.kpis = data;

        const fulfEl = document.getElementById('erp-kpi-fulfillment');
        const delayEl = document.getElementById('erp-kpi-delay');
        const scrapEl = document.getElementById('erp-kpi-scrap');
        const ordersEl = document.getElementById('erp-kpi-orders');
        const producedEl = document.getElementById('erp-kpi-produced');

        if (fulfEl) {
            fulfEl.textContent = data.plan_fulfillment_mean + '%';
            fulfEl.className = 'kpi-value ' + (data.plan_fulfillment_mean >= 95 ? 'good' : data.plan_fulfillment_mean >= 85 ? 'warning' : 'bad');
        }
        if (delayEl) {
            delayEl.textContent = data.delay_hours_mean + ' saat';
            delayEl.className = 'kpi-value ' + (data.delay_hours_mean <= 2 ? 'good' : data.delay_hours_mean <= 5 ? 'warning' : 'bad');
        }
        if (scrapEl) {
            scrapEl.textContent = data.scrap_rate_mean + '%';
            scrapEl.className = 'kpi-value ' + (data.scrap_rate_mean <= 2 ? 'good' : data.scrap_rate_mean <= 5 ? 'warning' : 'bad');
        }
        if (ordersEl) ordersEl.textContent = data.total_orders;
        if (producedEl) producedEl.textContent = 'Uretilen: ' + data.total_produced.toLocaleString('tr-TR');
    }

    async function erpLoadOrders() {
        const data = await API.get('/api/erp/orders');
        if (!data) return;
        erpState.orders = data;

        const countEl = document.getElementById('erp-order-count');
        if (countEl) countEl.textContent = data.length;

        const tbody = document.getElementById('erp-orders-body');
        if (!tbody) return;

        tbody.innerHTML = data.map(o => {
            const fulfClass = o.plan_fulfillment >= 95 ? 'good' : o.plan_fulfillment >= 85 ? 'warning' : 'bad';
            const delayClass = o.delay_hours <= 0 ? 'good' : o.delay_hours <= 2 ? 'warning' : 'bad';
            const scrapClass = o.scrap_rate <= 2 ? 'good' : o.scrap_rate <= 5 ? 'warning' : 'bad';
            return '<tr>' +
                '<td>' + o.order_id + '</td>' +
                '<td>' + o.planned_qty + '</td>' +
                '<td>' + o.produced_qty + '</td>' +
                '<td>' + o.defect_qty + '</td>' +
                '<td><span class="badge ' + fulfClass + '">' + o.plan_fulfillment + '%</span></td>' +
                '<td><span class="badge ' + delayClass + '">' + o.delay_hours + '</span></td>' +
                '<td><span class="badge ' + scrapClass + '">' + o.scrap_rate + '%</span></td>' +
                '</tr>';
        }).join('');
    }

    async function erpLoadCharts() {
        const [importance, curve] = await Promise.all([
            API.get('/api/erp/predict/feature-importance'),
            API.get('/api/erp/predict/temperature-curve')
        ]);
        if (importance) erpState.featureImportance = importance;
        if (curve) erpState.temperatureCurve = curve;
        drawFeatureImportanceChart();
        drawTemperatureCurveChart();
    }

    function drawFeatureImportanceChart() {
        const data = erpState.featureImportance;
        if (!data || !data.length) return;

        const canvasIds = ['erp-chart-importance', 'erp-chart-importance2'];
        canvasIds.forEach(id => {
            const canvas = document.getElementById(id);
            if (!canvas || !canvas.offsetParent) return;
            const ctx = canvas.getContext('2d');
            const W = canvas.parentElement.clientWidth;
            const H = canvas.parentElement.clientHeight || 250;
            canvas.width = W;
            canvas.height = H;

            ctx.clearRect(0, 0, W, H);

            const barH = Math.min(30, (H - 40) / data.length - 8);
            const maxVal = Math.max(...data.map(d => d.importance));
            const labelW = 140;
            const barMaxW = W - labelW - 80;

            data.forEach((item, i) => {
                const y = 20 + i * (barH + 8);
                const barW = (item.importance / maxVal) * barMaxW;

                // Label
                ctx.fillStyle = '#8892a0';
                ctx.font = '12px Segoe UI';
                ctx.textAlign = 'right';
                ctx.textBaseline = 'middle';
                ctx.fillText(item.feature, labelW - 10, y + barH / 2);

                // Bar
                const grad = ctx.createLinearGradient(labelW, 0, labelW + barW, 0);
                grad.addColorStop(0, '#1e90ff');
                grad.addColorStop(1, '#00d4ff');
                ctx.fillStyle = grad;
                ctx.beginPath();
                ctx.roundRect(labelW, y, barW, barH, 4);
                ctx.fill();

                // Value
                ctx.fillStyle = '#f0f4f8';
                ctx.textAlign = 'left';
                ctx.fillText((item.importance * 100).toFixed(1) + '%', labelW + barW + 8, y + barH / 2);
            });
        });
    }

    function drawTemperatureCurveChart() {
        const data = erpState.temperatureCurve;
        if (!data || !data.length) return;

        const canvasIds = ['erp-chart-tempcurve', 'erp-chart-tempcurve2'];
        canvasIds.forEach(id => {
            const canvas = document.getElementById(id);
            if (!canvas || !canvas.offsetParent) return;
            const ctx = canvas.getContext('2d');
            const W = canvas.parentElement.clientWidth;
            const H = canvas.parentElement.clientHeight || 250;
            canvas.width = W;
            canvas.height = H;

            ctx.clearRect(0, 0, W, H);

            const pad = { top: 20, right: 30, bottom: 40, left: 50 };
            const chartW = W - pad.left - pad.right;
            const chartH = H - pad.top - pad.bottom;

            const temps = data.map(d => d.temperature);
            const probs = data.map(d => d.defect_probability);
            const minT = Math.min(...temps);
            const maxT = Math.max(...temps);
            const maxP = Math.max(...probs, 0.5);

            // Grid
            ctx.strokeStyle = '#2a3a4a';
            ctx.lineWidth = 0.5;
            for (let i = 0; i <= 4; i++) {
                const y = pad.top + (chartH / 4) * i;
                ctx.beginPath();
                ctx.moveTo(pad.left, y);
                ctx.lineTo(W - pad.right, y);
                ctx.stroke();

                ctx.fillStyle = '#5a6a7a';
                ctx.font = '11px Consolas';
                ctx.textAlign = 'right';
                ctx.fillText((maxP * (1 - i / 4) * 100).toFixed(0) + '%', pad.left - 8, y + 4);
            }

            // X labels
            ctx.fillStyle = '#5a6a7a';
            ctx.textAlign = 'center';
            for (let i = 0; i < temps.length; i += 4) {
                const x = pad.left + ((temps[i] - minT) / (maxT - minT)) * chartW;
                ctx.fillText(temps[i].toFixed(0) + 'C', x, H - 8);
            }

            // Line
            ctx.beginPath();
            ctx.strokeStyle = '#ff8c00';
            ctx.lineWidth = 2.5;
            ctx.shadowColor = 'rgba(255, 140, 0, 0.4)';
            ctx.shadowBlur = 8;
            data.forEach((d, i) => {
                const x = pad.left + ((d.temperature - minT) / (maxT - minT)) * chartW;
                const y = pad.top + chartH - (d.defect_probability / maxP) * chartH;
                if (i === 0) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
            });
            ctx.stroke();
            ctx.shadowBlur = 0;

            // Points
            data.forEach(d => {
                const x = pad.left + ((d.temperature - minT) / (maxT - minT)) * chartW;
                const y = pad.top + chartH - (d.defect_probability / maxP) * chartH;
                ctx.beginPath();
                ctx.arc(x, y, 3, 0, Math.PI * 2);
                ctx.fillStyle = '#ff8c00';
                ctx.fill();
            });

            // Axis labels
            ctx.fillStyle = '#8892a0';
            ctx.font = '12px Segoe UI';
            ctx.textAlign = 'center';
            ctx.fillText('Sicaklik (C)', W / 2, H - 2);
        });
    }

    async function erpPredict() {
        const temp = parseFloat(document.getElementById('erp-pred-temp')?.value || '85');
        const speed = parseFloat(document.getElementById('erp-pred-speed')?.value || '90');
        const shift = document.getElementById('erp-pred-shift')?.value || 'Day';
        const exp = parseFloat(document.getElementById('erp-pred-exp')?.value || '5');
        const age = parseFloat(document.getElementById('erp-pred-age')?.value || '24');

        const resultDiv = document.getElementById('erp-predict-result');
        if (resultDiv) resultDiv.innerHTML = '<div style="text-align:center; color: var(--text-secondary); padding: 40px;" class="pulse">Tahmin hesaplaniyor...</div>';

        const data = await API.post('/api/erp/predict', {
            temperature: temp,
            line_speed: speed,
            shift: shift,
            operator_experience: exp,
            machine_age: age
        });

        if (!data || !resultDiv) return;

        const prob = (data.defect_probability * 100).toFixed(1);
        const conf = (data.confidence * 100).toFixed(1);
        const isDefect = data.predicted_defect;
        const riskLevel = getRiskLevel(data.defect_probability);
        const riskText = getRiskText(riskLevel);
        const riskColors = { low: 'var(--accent-green)', medium: 'var(--accent-yellow)', high: 'var(--accent-orange)', critical: 'var(--accent-red)' };
        const riskColor = riskColors[riskLevel];

        resultDiv.innerHTML = `
            <div class="predict-gauge">
                <div style="font-size: 48px; font-weight: 700; color: ${riskColor};">${prob}%</div>
                <div style="font-size: 14px; color: var(--text-secondary);">Hata Olasiligi</div>
            </div>
            <div class="predict-prob-bar">
                <div class="predict-prob-fill" style="width: ${prob}%; background: linear-gradient(90deg, var(--accent-green), var(--accent-yellow), var(--accent-red));"></div>
            </div>
            <div class="predict-detail-grid">
                <div class="predict-detail-item">
                    <div class="label">Risk Seviyesi</div>
                    <div class="value" style="color: ${riskColor};">${riskText}</div>
                </div>
                <div class="predict-detail-item">
                    <div class="label">Guven</div>
                    <div class="value" style="color: var(--accent-cyan);">${conf}%</div>
                </div>
                <div class="predict-detail-item">
                    <div class="label">Tahmin</div>
                    <div class="value" style="color: ${isDefect ? 'var(--accent-red)' : 'var(--accent-green)'};">${isDefect ? 'HATALI' : 'NORMAL'}</div>
                </div>
                <div class="predict-detail-item">
                    <div class="label">Model</div>
                    <div class="value" style="font-size: 13px; color: var(--text-secondary);">RandomForest</div>
                </div>
            </div>
        `;

        // Refresh charts
        erpLoadCharts();
    }

    async function erpLoadAnalytics() {
        const data = await API.get('/api/erp/analytics');
        if (!data) return;
        erpState.analytics = data;

        // Model info
        const modelEl = document.getElementById('erp-model-info');
        if (modelEl && data.model_info) {
            modelEl.innerHTML = `
                <div class="finding-item">
                    <h4>${data.model_info.name}</h4>
                    <p>${data.model_info.trees} agac, max derinlik: ${data.model_info.max_depth} | Durum: ${data.model_info.status}</p>
                </div>
            `;
        }

        // Findings
        const findingsEl = document.getElementById('erp-findings');
        if (findingsEl && data.findings) {
            findingsEl.innerHTML = data.findings.map(f =>
                '<div class="finding-item ' + f.severity + '">' +
                '<h4>' + f.title + '</h4>' +
                '<p>' + f.description + '</p>' +
                '</div>'
            ).join('');
        }

        // Recommendations
        const recEl = document.getElementById('erp-recommendations');
        if (recEl && data.recommendations) {
            recEl.innerHTML = data.recommendations.map(r =>
                '<li>' + r + '</li>'
            ).join('');
        }
    }

    // ─── KPI Automation (OEE, Downtime, Supplier) ──────────

    async function erpLoadOEEDetail() {
        const [oee, mttr, trends] = await Promise.all([
            API.get('/api/erp/oee/detail'),
            API.get('/api/erp/oee/mttr-mtbf'),
            API.get('/api/erp/oee/weekly-trends')
        ]);

        if (oee) {
            const setVal = (id, val, target) => {
                const el = document.getElementById(id);
                if (!el) return;
                el.textContent = val;
                if (target !== undefined) {
                    const num = parseFloat(val);
                    el.className = 'kpi-value ' + (num >= target ? 'good' : num >= target * 0.9 ? 'warning' : 'bad');
                }
            };
            setVal('oee-val-oee', oee.oee_pct + '%', 85);
            setVal('oee-val-avail', oee.availability_pct + '%', 90);
            setVal('oee-val-perf', oee.performance_pct + '%', 95);
            setVal('oee-val-qual', oee.quality_pct + '%', 99);

            const el = (id, txt) => { const e = document.getElementById(id); if (e) e.textContent = txt; };
            el('oee-val-planned', Math.round(oee.planned_time_min) + ' dk');
            el('oee-val-operating', Math.round(oee.operating_time_min) + ' dk');
            el('oee-val-unplanned', Math.round(oee.unplanned_downtime_min) + ' dk');
        }

        if (mttr) {
            const el = (id, txt) => { const e = document.getElementById(id); if (e) e.textContent = txt; };
            el('oee-val-mttr', mttr.mttr_min + ' dk');
            el('oee-mttr-hours', mttr.mttr_hours + ' saat');
            el('oee-val-mtbf', mttr.mtbf_min + ' dk');
            el('oee-mtbf-hours', mttr.mtbf_hours + ' saat');
            el('oee-val-failures', mttr.failure_count);
            el('oee-val-totalrepair', mttr.total_repair_time_min + ' dk');
        }

        if (trends && trends.length) {
            drawWeeklyOEEChart(trends);
        }
    }

    function drawWeeklyOEEChart(trends) {
        const canvas = document.getElementById('oee-chart-weekly');
        if (!canvas || !canvas.offsetParent) return;
        const ctx = canvas.getContext('2d');
        const W = canvas.parentElement.clientWidth;
        const H = canvas.parentElement.clientHeight || 280;
        canvas.width = W;
        canvas.height = H;
        ctx.clearRect(0, 0, W, H);

        const pad = { top: 30, right: 30, bottom: 50, left: 50 };
        const cW = W - pad.left - pad.right;
        const cH = H - pad.top - pad.bottom;

        // Grid
        ctx.strokeStyle = '#2a3a4a';
        ctx.lineWidth = 0.5;
        ctx.fillStyle = '#5a6a7a';
        ctx.font = '11px Consolas';
        ctx.textAlign = 'right';
        for (let i = 0; i <= 5; i++) {
            const y = pad.top + (cH / 5) * i;
            const val = 100 - i * 20;
            ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
            ctx.fillText(val + '%', pad.left - 8, y + 4);
        }

        // Target line at 85%
        const targetY = pad.top + cH * (1 - 85 / 100);
        ctx.strokeStyle = 'rgba(255, 59, 59, 0.5)';
        ctx.setLineDash([5, 5]);
        ctx.beginPath(); ctx.moveTo(pad.left, targetY); ctx.lineTo(W - pad.right, targetY); ctx.stroke();
        ctx.setLineDash([]);
        ctx.fillStyle = '#ff3b3b';
        ctx.textAlign = 'left';
        ctx.fillText('Hedef %85', W - pad.right + 4, targetY + 4);

        const barW = Math.min(40, (cW / trends.length) - 8);
        const colors = { oee: '#1e90ff', availability: '#00ff88', performance: '#ffc107', quality: '#00d4ff' };
        const metrics = ['availability_pct', 'performance_pct', 'quality_pct', 'oee_pct'];
        const metricColors = [colors.availability, colors.performance, colors.quality, colors.oee];

        // X labels + OEE bars
        ctx.textAlign = 'center';
        trends.forEach((t, i) => {
            const x = pad.left + (i + 0.5) * (cW / trends.length);

            // OEE bar
            const barH = (t.oee_pct / 100) * cH;
            const grad = ctx.createLinearGradient(0, pad.top + cH - barH, 0, pad.top + cH);
            grad.addColorStop(0, '#1e90ff');
            grad.addColorStop(1, '#0a4a8f');
            ctx.fillStyle = grad;
            ctx.beginPath();
            ctx.roundRect(x - barW / 2, pad.top + cH - barH, barW, barH, 3);
            ctx.fill();

            // Value on top
            ctx.fillStyle = '#f0f4f8';
            ctx.font = '11px Segoe UI';
            ctx.fillText(t.oee_pct + '%', x, pad.top + cH - barH - 6);

            // X label
            ctx.fillStyle = '#5a6a7a';
            ctx.fillText(t.week_label, x, H - 10);
        });

        // Legend
        ctx.font = '11px Segoe UI';
        ctx.textAlign = 'left';
        const legendItems = [['OEE', '#1e90ff'], ['Hedef', '#ff3b3b']];
        legendItems.forEach(([label, color], i) => {
            const lx = pad.left + i * 80;
            ctx.fillStyle = color;
            ctx.fillRect(lx, 6, 12, 12);
            ctx.fillStyle = '#8892a0';
            ctx.fillText(label, lx + 16, 16);
        });
    }

    async function erpLoadDowntime() {
        const data = await API.get('/api/erp/downtime/pareto');
        if (!data || !data.length) return;

        // Table
        const tbody = document.getElementById('downtime-table-body');
        if (tbody) {
            tbody.innerHTML = data.map(d => {
                const prioClass = d.priority === 'High' ? 'bad' : d.priority === 'Medium' ? 'warning' : 'good';
                return '<tr>' +
                    '<td>' + d.reason_code + '</td>' +
                    '<td>' + d.reason_name + '</td>' +
                    '<td><span class="badge ' + prioClass + '">' + d.category + '</span></td>' +
                    '<td>' + d.total_duration_min + '</td>' +
                    '<td>' + d.occurrence_count + '</td>' +
                    '<td>' + d.duration_pct + '%</td>' +
                    '<td>' + d.cumulative_pct + '%</td>' +
                    '</tr>';
            }).join('');
        }

        // Pareto chart
        drawParetoChart(data);
    }

    function drawParetoChart(data) {
        const canvas = document.getElementById('downtime-chart-pareto');
        if (!canvas || !canvas.offsetParent) return;
        const ctx = canvas.getContext('2d');
        const W = canvas.parentElement.clientWidth;
        const H = canvas.parentElement.clientHeight || 320;
        canvas.width = W;
        canvas.height = H;
        ctx.clearRect(0, 0, W, H);

        const pad = { top: 20, right: 50, bottom: 60, left: 50 };
        const cW = W - pad.left - pad.right;
        const cH = H - pad.top - pad.bottom;
        const maxDur = Math.max(...data.map(d => d.total_duration_min));

        // Y axis (duration)
        ctx.fillStyle = '#5a6a7a';
        ctx.font = '11px Consolas';
        ctx.textAlign = 'right';
        for (let i = 0; i <= 4; i++) {
            const y = pad.top + (cH / 4) * i;
            const val = Math.round(maxDur * (1 - i / 4));
            ctx.beginPath(); ctx.strokeStyle = '#2a3a4a'; ctx.lineWidth = 0.5;
            ctx.moveTo(pad.left, y); ctx.lineTo(W - pad.right, y); ctx.stroke();
            ctx.fillText(val + ' dk', pad.left - 8, y + 4);
        }

        // Right axis (cumulative %)
        ctx.textAlign = 'left';
        for (let i = 0; i <= 4; i++) {
            const y = pad.top + (cH / 4) * i;
            ctx.fillText((100 - i * 25) + '%', W - pad.right + 8, y + 4);
        }

        const barW = Math.min(50, (cW / data.length) - 12);
        const barColors = ['#ff3b3b', '#ff8c00', '#ffc107', '#1e90ff', '#00d4ff', '#00ff88', '#8892a0', '#5a6a7a', '#3a4a5a', '#2a3a4a'];

        // Bars
        data.forEach((d, i) => {
            const x = pad.left + (i + 0.5) * (cW / data.length);
            const barH = (d.total_duration_min / maxDur) * cH;

            ctx.fillStyle = barColors[i % barColors.length];
            ctx.beginPath();
            ctx.roundRect(x - barW / 2, pad.top + cH - barH, barW, barH, 3);
            ctx.fill();

            // Value on bar
            ctx.fillStyle = '#f0f4f8';
            ctx.font = '10px Segoe UI';
            ctx.textAlign = 'center';
            ctx.fillText(d.total_duration_min + 'dk', x, pad.top + cH - barH - 6);

            // X label (rotated)
            ctx.save();
            ctx.translate(x, H - 5);
            ctx.rotate(-Math.PI / 6);
            ctx.fillStyle = '#8892a0';
            ctx.font = '10px Segoe UI';
            ctx.textAlign = 'right';
            ctx.fillText(d.reason_name, 0, 0);
            ctx.restore();
        });

        // Cumulative line
        ctx.beginPath();
        ctx.strokeStyle = '#00ff88';
        ctx.lineWidth = 2;
        data.forEach((d, i) => {
            const x = pad.left + (i + 0.5) * (cW / data.length);
            const y = pad.top + cH - (d.cumulative_pct / 100) * cH;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        // Cumulative dots
        data.forEach((d, i) => {
            const x = pad.left + (i + 0.5) * (cW / data.length);
            const y = pad.top + cH - (d.cumulative_pct / 100) * cH;
            ctx.beginPath(); ctx.arc(x, y, 4, 0, Math.PI * 2);
            ctx.fillStyle = '#00ff88'; ctx.fill();
        });
    }

    async function erpLoadSuppliers() {
        const data = await API.get('/api/erp/suppliers');
        if (!data || data.error) return;

        const el = (id, txt) => { const e = document.getElementById(id); if (e) e.textContent = txt; };
        const setKpi = (id, val, target, invert) => {
            const e = document.getElementById(id);
            if (!e) return;
            e.textContent = val;
            const num = parseFloat(val);
            if (invert) {
                e.className = 'kpi-value ' + (num <= target ? 'good' : num <= target * 2 ? 'warning' : 'bad');
            } else {
                e.className = 'kpi-value ' + (num >= target ? 'good' : num >= target * 0.9 ? 'warning' : 'bad');
            }
        };

        setKpi('sup-val-ontime', data.overall_on_time_pct + '%', 95);
        setKpi('sup-val-delay', data.avg_delay_days + ' gun', 2, true);
        el('sup-val-total', data.total_deliveries);
        el('sup-val-late', 'Geciken: ' + data.late_deliveries);
        el('sup-val-count', data.supplier_count);

        // Supplier table
        const tbody = document.getElementById('supplier-table-body');
        if (tbody && data.suppliers) {
            tbody.innerHTML = data.suppliers.map(s => {
                const otClass = s.on_time_pct >= 80 ? 'good' : s.on_time_pct >= 50 ? 'warning' : 'bad';
                const qClass = s.avg_quality_score >= 97 ? 'good' : s.avg_quality_score >= 95 ? 'warning' : 'bad';
                const dClass = s.avg_delay_days <= 0 ? 'good' : s.avg_delay_days <= 2 ? 'warning' : 'bad';
                return '<tr>' +
                    '<td><strong>' + s.supplier_name + '</strong><br><span style="font-size:11px;color:var(--text-muted);">' + s.supplier_id + '</span></td>' +
                    '<td>' + s.country + '</td>' +
                    '<td>' + s.order_count + '</td>' +
                    '<td><span class="badge ' + otClass + '">' + s.on_time_pct + '%</span></td>' +
                    '<td><span class="badge ' + dClass + '">' + s.avg_delay_days + ' gun</span></td>' +
                    '<td><span class="badge ' + qClass + '">' + s.avg_quality_score + '</span></td>' +
                    '</tr>';
            }).join('');
        }
    }

    async function erpRefreshAll() {
        await Promise.all([
            erpLoadKPIs(),
            erpLoadOrders(),
            erpLoadCharts(),
            erpLoadAnalytics(),
            erpLoadOEEDetail(),
            erpLoadDowntime(),
            erpLoadSuppliers()
        ]);
    }

    return {
        init,
        startSystem,
        ackAlarm,
        refreshAll,
        ragQuery,
        ragQuickQuery,
        ragAnalyzeAlarm,
        ragUploadFile,
        ragSaveBuiltin,
        switchMode,
        erpPredict,
        erpRefreshAll
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
