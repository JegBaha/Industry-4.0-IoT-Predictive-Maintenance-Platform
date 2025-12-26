"""Simple web UI to start simulation and consumer with one click."""
import logging
from threading import Thread
from typing import Optional
from collections import deque
import json
import paho.mqtt.client as mqtt

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from config import mqtt as mqtt_cfg
from mosquitto_runner import ensure_broker_running
from mqtt_simulator import publish_stream
from ingest_consumer import consume_forever

log = logging.getLogger(__name__)

app = FastAPI(title="Smart Factory Control Panel")

sim_thread: Optional[Thread] = None
consumer_thread: Optional[Thread] = None
listener_thread: Optional[Thread] = None
messages_buffer: deque[str] = deque(maxlen=50)


def _start_sim():
    global sim_thread
    if sim_thread and sim_thread.is_alive():
        return

    ensure_broker_running()

    def run_sim():
        try:
            machines = [("MX100", "Line1"), ("MX200", "Line2")]
            publish_stream(machines)
        except Exception as exc:  # pragma: no cover
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
        except Exception as exc:  # pragma: no cover
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
        except Exception as exc:  # pragma: no cover
            log.exception("Listener error: %s", exc)

    def run_listener():
        client = mqtt.Client(client_id="ui-listener")
        if mqtt_cfg.username and mqtt_cfg.password:
            client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)
        client.on_message = on_message
        client.connect(mqtt_cfg.host, mqtt_cfg.port, keepalive=60)
        client.subscribe(f"{mqtt_cfg.base_topic}/#")
        client.loop_forever()

    listener_thread = Thread(target=run_listener, daemon=True)
    listener_thread.start()
    log.info("Listener thread started")


@app.get("/", response_class=HTMLResponse)
def index():
    return """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Smart Factory Control Panel</title>
  <style>
    body { font-family: Arial, sans-serif; background: #0b1526; color: #f2f4f8; padding: 32px; }
    h1 { margin-bottom: 8px; }
    button { padding: 12px 18px; margin-right: 12px; margin-top: 12px; border: none; border-radius: 6px; cursor: pointer; background: #1f7aec; color: white; font-weight: 600; }
    button.secondary { background: #354a67; }
    #status { margin-top: 24px; padding: 12px; background: #111a2c; border-radius: 8px; white-space: pre-line; }
  </style>
</head>
<body>
  <h1>Smart Factory Control Panel</h1>
  <p>Industry 4.0 IoT Predictive Maintenance Platform</p>
  <div>
    <button onclick="startAll()">Başlat (Broker + Sim + Consumer)</button>
    <button class="secondary" onclick="refreshStatus()">Durum Yenile</button>
  </div>
  <div id="status">Durum yükleniyor...</div>
  <h3>Son mesajlar (MQTT)</h3>
  <pre id="messages" style="background:#111a2c;padding:12px;border-radius:8px;min-height:140px;max-height:240px;overflow:auto;"></pre>
  <script>
    async function startAll() {
      await fetch('/actions/start-all', {method: 'POST'});
      await refreshStatus();
    }
    async function refreshStatus() {
      const res = await fetch('/status');
      const data = await res.json();
      document.getElementById('status').innerText =
        `Broker: ${data.broker}\nSimülasyon: ${data.simulation}\nConsumer: ${data.consumer}`;
    }
    async function refreshMessages() {
      const res = await fetch('/messages');
      const data = await res.json();
      document.getElementById('messages').innerText = data.messages.join('\\n');
    }
    refreshStatus();
    refreshMessages();
    setInterval(refreshStatus, 3000);
    setInterval(refreshMessages, 2000);
  </script>
</body>
</html>
    """


@app.post("/actions/start-all")
def start_all():
    _start_sim()
    _start_consumer()
    _start_listener()
    return JSONResponse({"ok": True})


@app.get("/status")
def status():
    broker = "running"  # we check on demand inside ensure_broker_running when starting
    sim = "running" if sim_thread and sim_thread.is_alive() else "stopped"
    consumer = "running" if consumer_thread and consumer_thread.is_alive() else "stopped"
    return {"broker": broker, "simulation": sim, "consumer": consumer}


@app.get("/messages")
def messages():
    return {"messages": list(messages_buffer)}


if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    uvicorn.run("ui:app", host="0.0.0.0", port=8000, reload=False)
