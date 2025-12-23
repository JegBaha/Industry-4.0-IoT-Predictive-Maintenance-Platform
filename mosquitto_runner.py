"""Utility to ensure Mosquitto broker is running locally (Windows-friendly)."""
import os
import socket
import subprocess
import logging
from pathlib import Path

from config import mqtt as mqtt_cfg

log = logging.getLogger(__name__)


def _is_port_open(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0


def _find_mosquitto_exe() -> Path | None:
    env_path = os.getenv("MOSQUITTO_PATH")
    candidates = []
    if env_path:
        candidates.append(Path(env_path))
    candidates.append(Path(r"C:\Program Files\mosquitto\mosquitto.exe"))

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def ensure_broker_running() -> None:
    """Check if broker is up; if not, try to start mosquitto.exe in foreground."""
    if _is_port_open(mqtt_cfg.host, mqtt_cfg.port):
        log.info("Mosquitto already running on %s:%s", mqtt_cfg.host, mqtt_cfg.port)
        return

    exe = _find_mosquitto_exe()
    if not exe:
        log.error("mosquitto.exe bulunamadı. MOSQUITTO_PATH env ile tam yolu belirtin.")
        return

    log.info("Starting mosquitto broker from %s on port %s", exe, mqtt_cfg.port)
    # Start with verbose logging; no config file to keep it simple.
    subprocess.Popen(
        [str(exe), "-v", "-p", str(mqtt_cfg.port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_CONSOLE,  # separate console so it keeps running
    )
    # Small wait and re-check
    for _ in range(5):
        if _is_port_open(mqtt_cfg.host, mqtt_cfg.port):
            log.info("Mosquitto started and listening on %s:%s", mqtt_cfg.host, mqtt_cfg.port)
            return
    log.error("Mosquitto başlatılamadı; port hâlâ kapalı.")
