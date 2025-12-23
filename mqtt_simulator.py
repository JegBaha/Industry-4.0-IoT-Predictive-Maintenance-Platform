"""MQTT publisher using synthetic generator."""
import json
import logging
import time
import paho.mqtt.client as mqtt

from config import mqtt as mqtt_cfg
from data_generator import generate_stream

log = logging.getLogger(__name__)


def _topic(line: str, machine_code: str) -> str:
    return f"{mqtt_cfg.base_topic}/{line}/{machine_code}"


def publish_stream(machines: list[tuple[str, str]], client_id: str = "simulator"):
    client = mqtt.Client(client_id=client_id)
    if mqtt_cfg.username and mqtt_cfg.password:
        client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)
    log.info("Connecting to MQTT broker %s:%s", mqtt_cfg.host, mqtt_cfg.port)
    client.connect(mqtt_cfg.host, mqtt_cfg.port, keepalive=60)
    log.info("Connected. Publishing telemetry every 0.5s to base topic '%s'", mqtt_cfg.base_topic)
    stream = generate_stream(machines)
    for reading in stream:
        payload = json.dumps(reading.__dict__)
        topic = _topic(reading.line, reading.machine_code)
        client.publish(topic, payload, qos=mqtt_cfg.qos)
        log.debug("PUB %s %s", topic, payload)
        time.sleep(0.5)
