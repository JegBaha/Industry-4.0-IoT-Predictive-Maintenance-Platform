"""Energy Monitoring service (ISO 50001).

Calculates power (kW), energy (kWh), kWh/unit, and CO2 emissions
from existing current sensor data. No additional hardware required.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field

from config import energy_cfg

log = logging.getLogger(__name__)


@dataclass
class EnergyReading:
    machine_code: str
    power_kw: float
    power_factor: float
    energy_kwh: float  # Incremental for this interval
    voltage: float
    current: float
    timestamp: float = field(default_factory=time.time)


@dataclass
class MachineEnergy:
    machine_code: str
    _readings: deque = field(default_factory=lambda: deque(maxlen=500))
    _shift_kwh: float = 0.0
    _total_kwh: float = 0.0
    _total_parts: int = 0
    _shift_start: float = field(default_factory=time.time)
    _last_timestamp: float = 0.0

    def add_reading(self, current: float, throughput: int = 0, timestamp: float = None):
        ts = timestamp or time.time()
        voltage = energy_cfg.nominal_voltage
        pf = 0.85  # Typical industrial power factor
        power_kw = voltage * current * pf * 1.732 / 1000  # 3-phase power

        # Energy = power * time
        dt_hours = (ts - self._last_timestamp) / 3600 if self._last_timestamp > 0 else 0
        dt_hours = min(dt_hours, 1.0)  # Cap at 1 hour (safety)
        energy_kwh = power_kw * dt_hours

        reading = EnergyReading(
            machine_code=self.machine_code,
            power_kw=round(power_kw, 3),
            power_factor=pf,
            energy_kwh=round(energy_kwh, 4),
            voltage=voltage,
            current=current,
            timestamp=ts,
        )
        self._readings.append(reading)
        self._shift_kwh += energy_kwh
        self._total_kwh += energy_kwh
        self._total_parts += throughput
        self._last_timestamp = ts

    @property
    def current_power_kw(self) -> float:
        if not self._readings:
            return 0
        return self._readings[-1].power_kw

    @property
    def kwh_per_part(self) -> float:
        if self._total_parts <= 0:
            return 0
        return round(self._total_kwh / self._total_parts, 4)

    @property
    def shift_kwh(self) -> float:
        return round(self._shift_kwh, 2)

    @property
    def co2_kg(self) -> float:
        return round(self._total_kwh * energy_cfg.co2_factor, 2)

    def get_trend(self, limit: int = 100) -> list[dict]:
        readings = list(self._readings)[-limit:]
        return [
            {
                "timestamp": r.timestamp,
                "power_kw": r.power_kw,
                "energy_kwh": r.energy_kwh,
                "current": r.current,
            }
            for r in readings
        ]

    def to_dict(self) -> dict:
        return {
            "machine_code": self.machine_code,
            "current_power_kw": self.current_power_kw,
            "shift_kwh": self.shift_kwh,
            "total_kwh": round(self._total_kwh, 2),
            "total_parts": self._total_parts,
            "kwh_per_part": self.kwh_per_part,
            "co2_kg": self.co2_kg,
            "target_kwh_per_unit": energy_cfg.target_kwh_per_unit,
            "on_target": self.kwh_per_part <= energy_cfg.target_kwh_per_unit if self._total_parts > 0 else True,
        }

    def reset_shift(self):
        self._shift_kwh = 0.0
        self._shift_start = time.time()


class EnergyService:
    """Manages energy monitoring for all machines."""

    def __init__(self):
        self._machines: dict[str, MachineEnergy] = {}

    def _get(self, machine_code: str) -> MachineEnergy:
        if machine_code not in self._machines:
            self._machines[machine_code] = MachineEnergy(machine_code=machine_code)
        return self._machines[machine_code]

    def feed(self, machine_code: str, current: float, throughput: int = 0, timestamp: float = None):
        self._get(machine_code).add_reading(current, throughput, timestamp)

    def get_realtime(self) -> dict:
        return {code: m.to_dict() for code, m in self._machines.items()}

    def get_summary(self) -> dict:
        total_kwh = sum(m._total_kwh for m in self._machines.values())
        total_parts = sum(m._total_parts for m in self._machines.values())
        return {
            "total_kwh": round(total_kwh, 2),
            "total_parts": total_parts,
            "avg_kwh_per_part": round(total_kwh / total_parts, 4) if total_parts > 0 else 0,
            "total_co2_kg": round(total_kwh * energy_cfg.co2_factor, 2),
            "machines": {code: m.to_dict() for code, m in self._machines.items()},
        }

    def get_trend(self, machine_code: str, limit: int = 100) -> list[dict]:
        return self._get(machine_code).get_trend(limit)

    def get_shift_report(self) -> dict:
        return {
            "machines": {code: {
                "shift_kwh": m.shift_kwh,
                "current_power_kw": m.current_power_kw,
            } for code, m in self._machines.items()},
            "total_shift_kwh": round(sum(m._shift_kwh for m in self._machines.values()), 2),
        }
