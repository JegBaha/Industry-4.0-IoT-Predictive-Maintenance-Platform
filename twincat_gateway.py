"""TwinCAT → MQTT Gateway.

Reads sensor data from a Beckhoff TwinCAT PLC via ADS (pyads)
and publishes it to MQTT in the format SmartFact expects.

Usage:
    python twincat_gateway.py --config twincat_gateway_config.json
"""

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass

import pyads
import paho.mqtt.client as mqtt

from observability import mqtt_publish_total, setup_logging

setup_logging(json_format=True)
log = logging.getLogger("twincat_gateway")

# ADS ↔ pyads type mapping for sensor fields
ADS_TYPES = {
    "temperature": pyads.PLCTYPE_LREAL,
    "vibration":   pyads.PLCTYPE_LREAL,
    "current":     pyads.PLCTYPE_LREAL,
    "throughput":  pyads.PLCTYPE_DINT,
    "failure_flag": pyads.PLCTYPE_BOOL,
}


@dataclass
class MachineMapping:
    machine_code: str
    line: str
    tags: dict[str, str]  # smartfact_field -> PLC tag name


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


class TwinCATGateway:
    def __init__(self, config: dict):
        tc = config["twincat"]
        self.ams_net_id: str = tc["ams_net_id"]
        self.ams_port: int = tc.get("ams_port", pyads.PORT_TC3PLC1)
        self.plc_ip: str | None = tc.get("plc_ip")

        mq = config.get("mqtt", {})
        self.mqtt_host: str = mq.get("host", "localhost")
        self.mqtt_port: int = mq.get("port", 1883)
        self.mqtt_base_topic: str = mq.get("base_topic", "factory")
        self.mqtt_qos: int = mq.get("qos", 1)
        self.mqtt_username: str | None = mq.get("username")
        self.mqtt_password: str | None = mq.get("password")

        self.poll_interval: float = config.get("poll_interval_sec", 0.5)

        self.machines: list[MachineMapping] = [
            MachineMapping(
                machine_code=m["machine_code"],
                line=m["line"],
                tags=m["tags"],
            )
            for m in config["machines"]
        ]

        self.plc: pyads.Connection | None = None
        self.mqtt_client: mqtt.Client | None = None
        self._running = True

    # ── PLC Connection ───────────────────────────────────────────
    def connect_plc(self) -> None:
        """Open ADS connection to TwinCAT, with retry."""
        while self._running:
            try:
                if self.plc_ip:
                    self.plc = pyads.Connection(
                        self.ams_net_id, self.ams_port, self.plc_ip
                    )
                else:
                    self.plc = pyads.Connection(self.ams_net_id, self.ams_port)
                self.plc.open()
                state = self.plc.read_state()
                log.info(
                    "Connected to PLC %s (state: %s, device: %s)",
                    self.ams_net_id, state[0], state[1],
                )
                return
            except Exception as exc:
                log.warning("PLC connection failed: %s — retrying in 5s", exc)
                time.sleep(5)

    # ── MQTT Connection ──────────────────────────────────────────
    def connect_mqtt(self) -> None:
        self.mqtt_client = mqtt.Client(client_id="twincat-gateway")
        if self.mqtt_username and self.mqtt_password:
            self.mqtt_client.username_pw_set(self.mqtt_username, self.mqtt_password)
        self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, keepalive=60)
        self.mqtt_client.loop_start()
        log.info("MQTT connected to %s:%s", self.mqtt_host, self.mqtt_port)

    # ── Read & Publish ───────────────────────────────────────────
    def read_and_publish(self) -> None:
        """Read all machine tags from PLC and publish to MQTT."""
        for machine in self.machines:
            try:
                payload = self._read_machine(machine)
                topic = f"{self.mqtt_base_topic}/{machine.line}/{machine.machine_code}"
                msg = json.dumps(payload)
                self.mqtt_client.publish(topic, msg, qos=self.mqtt_qos)
                mqtt_publish_total.labels(machine=machine.machine_code).inc()
                log.debug("PUB %s %s", topic, msg)
            except pyads.ADSError as exc:
                log.error(
                    "ADS read error for %s: %s", machine.machine_code, exc
                )
            except Exception as exc:
                log.error(
                    "Error processing %s: %s", machine.machine_code, exc
                )

    def _read_machine(self, machine: MachineMapping) -> dict:
        """Read a single machine's sensor tags and return SmartFact payload."""
        data: dict = {
            "machine_code": machine.machine_code,
            "line": machine.line,
            "timestamp": time.time(),
        }
        for field, tag_name in machine.tags.items():
            ads_type = ADS_TYPES.get(field, pyads.PLCTYPE_LREAL)
            value = self.plc.read_by_name(tag_name, ads_type)
            # Round floats for consistency with simulator
            if isinstance(value, float):
                value = round(value, 3)
            data[field] = value
        return data

    # ── Main Loop ────────────────────────────────────────────────
    def run(self) -> None:
        """Start gateway: connect to PLC + MQTT, then poll in a loop."""
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        self.connect_plc()
        self.connect_mqtt()

        log.info(
            "Gateway running — polling %d machine(s) every %.1fs",
            len(self.machines), self.poll_interval,
        )

        while self._running:
            try:
                self.read_and_publish()
            except Exception as exc:
                log.error("Poll cycle error: %s — reconnecting PLC", exc)
                self.connect_plc()
            time.sleep(self.poll_interval)

        self._cleanup()

    def _shutdown(self, signum, frame):
        log.info("Shutdown signal received, stopping...")
        self._running = False

    def _cleanup(self):
        if self.plc:
            try:
                self.plc.close()
            except Exception:
                pass
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        log.info("Gateway stopped.")


def main():
    parser = argparse.ArgumentParser(description="TwinCAT → MQTT Gateway")
    parser.add_argument(
        "--config", "-c",
        default="twincat_gateway_config.json",
        help="Path to gateway config JSON (default: twincat_gateway_config.json)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    gateway = TwinCATGateway(config)
    gateway.run()


if __name__ == "__main__":
    main()
