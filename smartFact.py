"""Orchestrator stub."""
import logging
from mosquitto_runner import ensure_broker_running
from mqtt_simulator import publish_stream
from ingest_consumer import consume_forever
from threading import Thread
from observability import setup_logging


def main():
    setup_logging(json_format=True)
    logging.info("Starting Smart Factory simulator (MQTT publisher + consumer)")
    ensure_broker_running()
    machines = [("MX100", "Line1"), ("MX200", "Line2")]

    consumer_thread = Thread(target=consume_forever, daemon=True)
    consumer_thread.start()

    try:
        publish_stream(machines)
    except KeyboardInterrupt:
        logging.info("Stopped by user")


if __name__ == "__main__":
    main()
