"""Edge Computing — Store-and-forward buffer and local rule engine.

Provides offline-capable message buffering using SQLite and a low-latency
rule engine for critical alarm decisions at the edge (<10ms target).
"""
import json
import logging
import sqlite3
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable

from config import edge_cfg

log = logging.getLogger(__name__)


class StoreAndForward:
    """SQLite-backed local buffer for messages when cloud/DB is unavailable.

    Messages are stored with topic and payload, then forwarded in FIFO order
    when connectivity is restored.
    """

    def __init__(self, db_path: str = None, max_size: int = None):
        self.db_path = db_path or edge_cfg.local_buffer_path
        self.max_size = max_size or edge_cfg.max_buffer_size
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS message_buffer (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    topic TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_buffer_ts ON message_buffer(created_at)")
            conn.commit()
            conn.close()
        except Exception:
            log.exception("Failed to initialize edge buffer DB")

    def store(self, topic: str, payload: str):
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                conn.execute(
                    "INSERT INTO message_buffer (topic, payload, created_at) VALUES (?, ?, ?)",
                    (topic, payload, time.time())
                )
                conn.commit()

                # Enforce max size — delete oldest
                count = conn.execute("SELECT COUNT(*) FROM message_buffer").fetchone()[0]
                if count > self.max_size:
                    excess = count - self.max_size
                    conn.execute(
                        "DELETE FROM message_buffer WHERE id IN "
                        "(SELECT id FROM message_buffer ORDER BY created_at ASC LIMIT ?)",
                        (excess,)
                    )
                    conn.commit()
                conn.close()
            except Exception:
                log.exception("Edge buffer store failed")

    def forward(self, batch_size: int = 100) -> list[tuple[int, str, str]]:
        """Retrieve oldest messages for forwarding. Returns [(id, topic, payload)]."""
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                rows = conn.execute(
                    "SELECT id, topic, payload FROM message_buffer ORDER BY created_at ASC LIMIT ?",
                    (batch_size,)
                ).fetchall()
                conn.close()
                return rows
            except Exception:
                log.exception("Edge buffer forward failed")
                return []

    def delete_forwarded(self, ids: list[int]):
        if not ids:
            return
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                placeholders = ",".join("?" for _ in ids)
                conn.execute(f"DELETE FROM message_buffer WHERE id IN ({placeholders})", ids)
                conn.commit()
                conn.close()
            except Exception:
                log.exception("Edge buffer delete failed")

    def get_stats(self) -> dict:
        try:
            conn = sqlite3.connect(self.db_path)
            count = conn.execute("SELECT COUNT(*) FROM message_buffer").fetchone()[0]
            oldest = conn.execute("SELECT MIN(created_at) FROM message_buffer").fetchone()[0]
            newest = conn.execute("SELECT MAX(created_at) FROM message_buffer").fetchone()[0]
            conn.close()
            return {
                "buffered_count": count,
                "oldest_ts": oldest,
                "newest_ts": newest,
                "max_size": self.max_size,
                "db_path": self.db_path,
            }
        except Exception:
            return {"buffered_count": 0, "error": "DB unavailable"}

    def cleanup(self, max_age_hours: int = 24) -> int:
        cutoff = time.time() - max_age_hours * 3600
        with self._lock:
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.execute(
                    "DELETE FROM message_buffer WHERE created_at < ?", (cutoff,)
                )
                deleted = cursor.rowcount
                conn.commit()
                conn.close()
                return deleted
            except Exception:
                return 0


@dataclass
class EdgeRule:
    """A simple rule for the edge rule engine."""
    rule_id: str
    sensor: str
    operator: str  # '>', '<', '>=', '<=', '=='
    threshold: float
    action: str  # 'alarm', 'packml_hold', 'mqtt_publish', 'log'
    enabled: bool = True
    trigger_count: int = 0
    last_trigger: float = 0.0

    def evaluate(self, value: float) -> bool:
        ops = {
            '>': lambda v, t: v > t,
            '<': lambda v, t: v < t,
            '>=': lambda v, t: v >= t,
            '<=': lambda v, t: v <= t,
            '==': lambda v, t: abs(v - t) < 0.001,
        }
        fn = ops.get(self.operator)
        if fn is None:
            return False
        return fn(value, self.threshold)

    def to_dict(self) -> dict:
        return {
            "rule_id": self.rule_id,
            "sensor": self.sensor,
            "operator": self.operator,
            "threshold": self.threshold,
            "action": self.action,
            "enabled": self.enabled,
            "trigger_count": self.trigger_count,
            "last_trigger": self.last_trigger,
        }


class EdgeRuleEngine:
    """Low-latency rule evaluation for critical decisions at the edge.

    Target: <10ms per evaluation cycle for all rules.
    """

    def __init__(self):
        self._rules: dict[str, EdgeRule] = {}
        self._action_handlers: dict[str, Callable] = {}
        self._init_default_rules()

    def _init_default_rules(self):
        defaults = [
            EdgeRule("R001", "temperature", ">", 70.0, "alarm"),
            EdgeRule("R002", "vibration", ">", 0.7, "alarm"),
            EdgeRule("R003", "current", ">", 15.0, "alarm"),
            EdgeRule("R004", "temperature", ">", 80.0, "packml_hold"),
            EdgeRule("R005", "vibration", ">", 1.0, "packml_hold"),
        ]
        for r in defaults:
            self._rules[r.rule_id] = r

    def add_rule(self, rule_id: str, sensor: str, operator: str,
                 threshold: float, action: str) -> EdgeRule:
        rule = EdgeRule(rule_id=rule_id, sensor=sensor, operator=operator,
                        threshold=threshold, action=action)
        self._rules[rule_id] = rule
        return rule

    def remove_rule(self, rule_id: str) -> bool:
        return self._rules.pop(rule_id, None) is not None

    def evaluate(self, data: dict) -> list[dict]:
        """Evaluate all rules against incoming sensor data.

        Args:
            data: dict with sensor keys (temperature, vibration, current, etc.)

        Returns:
            List of triggered rule results
        """
        triggered = []
        for rule in self._rules.values():
            if not rule.enabled:
                continue
            value = data.get(rule.sensor)
            if value is None:
                continue
            if rule.evaluate(value):
                rule.trigger_count += 1
                rule.last_trigger = time.time()
                triggered.append({
                    "rule_id": rule.rule_id,
                    "sensor": rule.sensor,
                    "value": value,
                    "threshold": rule.threshold,
                    "action": rule.action,
                })
                # Execute action handler if registered
                handler = self._action_handlers.get(rule.action)
                if handler:
                    try:
                        handler(rule, value, data)
                    except Exception:
                        log.exception(f"Edge rule action handler failed: {rule.rule_id}")

        return triggered

    def register_handler(self, action: str, handler: Callable):
        self._action_handlers[action] = handler

    def get_rules(self) -> list[dict]:
        return [r.to_dict() for r in self._rules.values()]

    def get_stats(self) -> dict:
        return {
            "total_rules": len(self._rules),
            "active_rules": sum(1 for r in self._rules.values() if r.enabled),
            "total_triggers": sum(r.trigger_count for r in self._rules.values()),
        }
