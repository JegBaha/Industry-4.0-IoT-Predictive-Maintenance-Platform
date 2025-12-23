"""MQTT consumer inserting into DB."""
import json
import paho.mqtt.client as mqtt
from datetime import datetime

from config import mqtt as mqtt_cfg
from db import insert_batch

INSERT_SQL = """
INSERT INTO fact_sensor_readings (
    machine_id, time_id, shift_id, temperature, vibration, current, throughput, failure_flag
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def _lookup_ids(reading: dict) -> tuple[int, int, int]:
    # Placeholder: in a real system we would query dim tables. Use 1 for now.
    return 1, 1, 1


def consume_once(batch_size: int = 50):
    rows = []

    def on_message(_client, _userdata, msg):
        nonlocal rows
        reading = json.loads(msg.payload.decode())
        machine_id, time_id, shift_id = _lookup_ids(reading)
        rows.append(
            (
                machine_id,
                time_id,
                shift_id,
                reading.get("temperature"),
                reading.get("vibration"),
                reading.get("current"),
                reading.get("throughput"),
                reading.get("failure_flag"),
            )
        )
        if len(rows) >= batch_size:
            insert_batch(INSERT_SQL, rows)
            rows = []

    client = mqtt.Client(client_id="consumer")
    if mqtt_cfg.username and mqtt_cfg.password:
        client.username_pw_set(mqtt_cfg.username, mqtt_cfg.password)
    client.on_message = on_message
    client.connect(mqtt_cfg.host, mqtt_cfg.port, keepalive=60)
    client.subscribe(f"{mqtt_cfg.base_topic}/#")
    client.loop_start()
    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass
    finally:
        if rows:
            insert_batch(INSERT_SQL, rows)
        client.loop_stop()
