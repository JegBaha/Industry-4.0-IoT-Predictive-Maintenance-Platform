"""Observability module — Prometheus metrics, structured logging, health checks."""

import json
import logging
import socket
import time
import uuid
from contextvars import ContextVar

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

# MQTT
mqtt_messages_received = Counter(
    "smartfact_mqtt_messages_total",
    "MQTT messages received",
    ["machine"],
)
mqtt_publish_total = Counter(
    "smartfact_mqtt_publish_total",
    "MQTT messages published",
    ["machine"],
)

# DB
db_insert_latency = Histogram(
    "smartfact_db_insert_seconds",
    "DB batch insert latency",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0],
)
db_insert_rows = Counter(
    "smartfact_db_rows_inserted_total",
    "Total rows inserted to DB",
)

# Alarms
alarm_total = Counter(
    "smartfact_alarms_total",
    "Total alarms generated",
    ["machine", "sensor", "severity"],
)
alarm_active = Gauge(
    "smartfact_alarms_active",
    "Currently unacknowledged alarms",
)

# Model inference
inference_latency = Histogram(
    "smartfact_inference_seconds",
    "Model inference latency",
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
)

# PackML
packml_transitions = Counter(
    "smartfact_packml_transitions_total",
    "PackML state transitions",
    ["machine", "from_state", "to_state"],
)
packml_current_state = Gauge(
    "smartfact_packml_state",
    "PackML current state (encoded)",
    ["machine"],
)

# Digital Twin
twin_deviation_count = Counter(
    "smartfact_twin_deviations_total",
    "Digital twin deviation alerts",
    ["machine", "sensor", "severity"],
)

# SPC
spc_violation_total = Counter(
    "smartfact_spc_violations_total",
    "SPC Nelson rule violations",
    ["machine", "sensor", "rule"],
)

# Energy
energy_kwh_total = Counter(
    "smartfact_energy_kwh_total",
    "Total energy consumed (kWh)",
    ["machine"],
)

# Edge
edge_buffer_size = Gauge(
    "smartfact_edge_buffer_messages",
    "Messages in edge buffer",
)
edge_rule_triggers = Counter(
    "smartfact_edge_rule_triggers_total",
    "Edge rule engine triggers",
    ["rule_id"],
)

# System
uptime_seconds = Gauge("smartfact_uptime_seconds", "Application uptime")

# ============================================================================
# STRUCTURED LOGGING
# ============================================================================

correlation_id: ContextVar[str] = ContextVar("correlation_id", default="")


def new_correlation_id() -> str:
    """Generate and set a new correlation ID for the current context."""
    cid = uuid.uuid4().hex[:8]
    correlation_id.set(cid)
    return cid


class JSONFormatter(logging.Formatter):
    """JSON log formatter with correlation ID support."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        cid = correlation_id.get("")
        if cid:
            entry["correlation_id"] = cid
        # Optional extra fields
        for attr in ("machine", "duration_ms", "rows", "topic"):
            val = getattr(record, attr, None)
            if val is not None:
                entry[attr] = val
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(json_format: bool = True, level: int = logging.INFO) -> None:
    """Configure root logger. Call once at application startup."""
    root = logging.getLogger()
    root.setLevel(level)

    # Remove existing handlers to avoid duplicates
    root.handlers.clear()

    handler = logging.StreamHandler()
    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
    root.addHandler(handler)

    # Quiet noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("chromadb").setLevel(logging.WARNING)
    logging.getLogger("sentence_transformers").setLevel(logging.WARNING)


# ============================================================================
# HEALTH CHECKS
# ============================================================================


def check_mqtt_broker(host: str, port: int) -> dict:
    """Check MQTT broker reachability via TCP socket."""
    try:
        sock = socket.create_connection((host, port), timeout=3)
        sock.close()
        return {"status": "up", "host": host, "port": port}
    except (OSError, ConnectionRefusedError) as exc:
        return {"status": "down", "host": host, "port": port, "error": str(exc)}


def check_postgres(uri: str) -> dict:
    """Check PostgreSQL connectivity with SELECT 1."""
    try:
        import psycopg2

        t0 = time.time()
        conn = psycopg2.connect(uri, connect_timeout=3)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        latency_ms = round((time.time() - t0) * 1000, 1)
        return {"status": "up", "latency_ms": latency_ms}
    except Exception as exc:
        return {"status": "down", "error": str(exc)}


def check_vector_store() -> dict:
    """Check ChromaDB / vector store health."""
    try:
        from rag.rag_api import get_knowledge_base

        kb = get_knowledge_base()
        stats = kb.vector_store.get_stats()
        return {"status": "up", **stats}
    except Exception as exc:
        return {"status": "down", "error": str(exc)}


def run_health_checks(mqtt_host: str, mqtt_port: int, db_uri: str) -> dict:
    """Run all health checks and return aggregated result."""
    checks = {
        "mqtt_broker": check_mqtt_broker(mqtt_host, mqtt_port),
        "postgres": check_postgres(db_uri),
        "vector_store": check_vector_store(),
    }
    all_up = all(c["status"] == "up" for c in checks.values())
    return {
        "status": "healthy" if all_up else "degraded",
        "checks": checks,
    }
