"""Digital Twin Engine — compares real sensor data against physics model predictions.

Tracks deviations, calculates health scores, and generates early warnings
when actual values deviate significantly from expected behavior.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from .physics_model import PhysicsModel, DEFAULT_MODELS

log = logging.getLogger(__name__)


@dataclass
class DeviationRecord:
    sensor: str
    expected: float
    actual: float
    deviation_pct: float
    severity: str  # normal, warning, anomaly
    timestamp: float = field(default_factory=time.time)


@dataclass
class MachineDigitalTwin:
    """Digital twin for a single machine."""
    machine_code: str
    model: PhysicsModel
    warning_pct: float = 10.0
    anomaly_pct: float = 15.0
    _history: deque = field(default_factory=lambda: deque(maxlen=200))
    _deviations: deque = field(default_factory=lambda: deque(maxlen=100))
    _comparison_history: deque = field(default_factory=lambda: deque(maxlen=200))
    _health_score: float = 100.0
    _runtime_start: float = field(default_factory=time.time)
    _last_update: float = 0.0

    def update(self, actual: dict) -> dict:
        """Compare actual sensor reading against model predictions.

        Args:
            actual: dict with keys temperature, vibration, current, throughput

        Returns:
            Deviation report for all sensors
        """
        runtime_min = (time.time() - self._runtime_start) / 60.0
        throughput = actual.get("throughput", 0)

        expected = {
            "temperature": round(self.model.expected_temperature(throughput, runtime_min), 2),
            "vibration": round(self.model.expected_vibration(throughput), 4),
            "current": round(self.model.expected_current(throughput, actual.get("temperature", 22)), 2),
        }

        report = {"machine_code": self.machine_code, "deviations": [], "timestamp": time.time()}
        total_deviation = 0.0
        sensor_count = 0

        for sensor in ["temperature", "vibration", "current"]:
            act_val = actual.get(sensor)
            exp_val = expected.get(sensor)
            if act_val is None or exp_val is None or exp_val == 0:
                continue

            deviation_pct = abs(act_val - exp_val) / abs(exp_val) * 100
            severity = "normal"
            if deviation_pct > self.anomaly_pct:
                severity = "anomaly"
            elif deviation_pct > self.warning_pct:
                severity = "warning"

            record = DeviationRecord(
                sensor=sensor,
                expected=exp_val,
                actual=act_val,
                deviation_pct=round(deviation_pct, 2),
                severity=severity,
            )
            if severity != "normal":
                self._deviations.append(record)

            report["deviations"].append({
                "sensor": sensor,
                "expected": exp_val,
                "actual": act_val,
                "deviation_pct": round(deviation_pct, 2),
                "severity": severity,
            })
            total_deviation += deviation_pct
            sensor_count += 1

        # Update comparison history
        self._comparison_history.append({
            "timestamp": time.time(),
            "expected": expected,
            "actual": {k: actual.get(k) for k in ["temperature", "vibration", "current"]},
        })

        # Recalculate health score (exponential moving average)
        if sensor_count > 0:
            avg_deviation = total_deviation / sensor_count
            # Health = 100 when 0% deviation, 0 when >30% deviation
            instant_health = max(0, 100 - avg_deviation * (100 / 30))
            alpha = 0.1  # Smoothing factor
            self._health_score = alpha * instant_health + (1 - alpha) * self._health_score

        report["health_score"] = round(self._health_score, 1)
        self._last_update = time.time()
        return report

    def get_health_score(self) -> float:
        return round(self._health_score, 1)

    def get_comparison(self, limit: int = 50) -> list[dict]:
        return list(self._comparison_history)[-limit:]

    def get_active_deviations(self) -> list[dict]:
        """Get recent non-normal deviations (last 60 seconds)."""
        cutoff = time.time() - 60
        return [
            {
                "sensor": d.sensor,
                "expected": d.expected,
                "actual": d.actual,
                "deviation_pct": d.deviation_pct,
                "severity": d.severity,
                "timestamp": d.timestamp,
            }
            for d in self._deviations
            if d.timestamp > cutoff
        ]

    def calibrate(self):
        """Reset runtime and health score (after maintenance)."""
        self._runtime_start = time.time()
        self._health_score = 100.0
        self._deviations.clear()
        log.info(f"[{self.machine_code}] Digital twin calibrated")

    def to_dict(self) -> dict:
        return {
            "machine_code": self.machine_code,
            "health_score": self.get_health_score(),
            "active_deviations": self.get_active_deviations(),
            "last_update": self._last_update,
            "runtime_minutes": round((time.time() - self._runtime_start) / 60, 1),
        }


class TwinEngine:
    """Manages digital twins for all machines."""

    def __init__(self, warning_pct: float = 10.0, anomaly_pct: float = 15.0):
        self.warning_pct = warning_pct
        self.anomaly_pct = anomaly_pct
        self._twins: dict[str, MachineDigitalTwin] = {}

    def _get_twin(self, machine_code: str) -> MachineDigitalTwin:
        if machine_code not in self._twins:
            model = DEFAULT_MODELS.get(machine_code, PhysicsModel(machine_code=machine_code))
            self._twins[machine_code] = MachineDigitalTwin(
                machine_code=machine_code,
                model=model,
                warning_pct=self.warning_pct,
                anomaly_pct=self.anomaly_pct,
            )
        return self._twins[machine_code]

    def update(self, machine_code: str, actual: dict) -> dict:
        twin = self._get_twin(machine_code)
        return twin.update(actual)

    def get_status(self, machine_code: str) -> dict:
        return self._get_twin(machine_code).to_dict()

    def get_all_status(self) -> dict:
        return {code: twin.to_dict() for code, twin in self._twins.items()}

    def get_comparison(self, machine_code: str, limit: int = 50) -> list[dict]:
        return self._get_twin(machine_code).get_comparison(limit)

    def get_health_score(self, machine_code: str) -> float:
        return self._get_twin(machine_code).get_health_score()

    def calibrate(self, machine_code: str):
        self._get_twin(machine_code).calibrate()
