"""Orchestrator stub."""
import logging
from mosquitto_runner import ensure_broker_running
from mqtt_simulator import publish_stream


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.info("Starting Smart Factory simulator (MQTT publisher)")
    ensure_broker_running()
    machines = [("MX100", "Line1"), ("MX200", "Line2")]
    try:
        publish_stream(machines)
    except KeyboardInterrupt:
        logging.info("Stopped by user")


if __name__ == "__main__":
    main()
