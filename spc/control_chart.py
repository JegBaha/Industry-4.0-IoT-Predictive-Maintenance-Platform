"""SPC Control Charts — X-bar, R chart, and process capability (Cp, Cpk).

Implements real-time statistical process control with subgroup-based
control charts and Nelson rules for out-of-control detection.
"""
import math
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

# A2, D3, D4 constants for X-bar and R charts (subgroup sizes 2-10)
SPC_CONSTANTS = {
    2:  {"A2": 1.880, "D3": 0.000, "D4": 3.267, "d2": 1.128},
    3:  {"A2": 1.023, "D3": 0.000, "D4": 2.575, "d2": 1.693},
    4:  {"A2": 0.729, "D3": 0.000, "D4": 2.282, "d2": 2.059},
    5:  {"A2": 0.577, "D3": 0.000, "D4": 2.115, "d2": 2.326},
    6:  {"A2": 0.483, "D3": 0.000, "D4": 2.004, "d2": 2.534},
    7:  {"A2": 0.419, "D3": 0.076, "D4": 1.924, "d2": 2.704},
    8:  {"A2": 0.373, "D3": 0.136, "D4": 1.864, "d2": 2.847},
    9:  {"A2": 0.337, "D3": 0.184, "D4": 1.816, "d2": 2.970},
    10: {"A2": 0.308, "D3": 0.223, "D4": 1.777, "d2": 3.078},
}


@dataclass
class Subgroup:
    values: list[float]
    mean: float
    range: float
    timestamp: float = 0.0


class ControlChart:
    """X-bar and R control chart with automatic limit calculation."""

    def __init__(self, subgroup_size: int = 5, max_subgroups: int = 50):
        self.n = subgroup_size
        self._subgroups: deque[Subgroup] = deque(maxlen=max_subgroups)
        self._sample_buffer: list[float] = []
        self._constants = SPC_CONSTANTS.get(subgroup_size, SPC_CONSTANTS[5])

    def add_sample(self, value: float, timestamp: float = 0.0):
        """Add a single sample. Forms subgroups automatically."""
        self._sample_buffer.append(value)
        if len(self._sample_buffer) >= self.n:
            values = self._sample_buffer[:self.n]
            self._sample_buffer = self._sample_buffer[self.n:]
            sg = Subgroup(
                values=values,
                mean=sum(values) / len(values),
                range=max(values) - min(values),
                timestamp=timestamp,
            )
            self._subgroups.append(sg)

    @property
    def grand_mean(self) -> float:
        if not self._subgroups:
            return 0
        return sum(sg.mean for sg in self._subgroups) / len(self._subgroups)

    @property
    def mean_range(self) -> float:
        if not self._subgroups:
            return 0
        return sum(sg.range for sg in self._subgroups) / len(self._subgroups)

    @property
    def sigma_estimate(self) -> float:
        d2 = self._constants["d2"]
        return self.mean_range / d2 if d2 > 0 else 0

    def get_xbar_limits(self) -> dict:
        xbar = self.grand_mean
        r_bar = self.mean_range
        A2 = self._constants["A2"]
        return {
            "cl": round(xbar, 4),
            "ucl": round(xbar + A2 * r_bar, 4),
            "lcl": round(xbar - A2 * r_bar, 4),
        }

    def get_r_limits(self) -> dict:
        r_bar = self.mean_range
        D3 = self._constants["D3"]
        D4 = self._constants["D4"]
        return {
            "cl": round(r_bar, 4),
            "ucl": round(D4 * r_bar, 4),
            "lcl": round(D3 * r_bar, 4),
        }

    def get_xbar_data(self) -> dict:
        limits = self.get_xbar_limits()
        return {
            "values": [round(sg.mean, 4) for sg in self._subgroups],
            "timestamps": [sg.timestamp for sg in self._subgroups],
            **limits,
            "subgroup_count": len(self._subgroups),
        }

    def get_r_data(self) -> dict:
        limits = self.get_r_limits()
        return {
            "values": [round(sg.range, 4) for sg in self._subgroups],
            "timestamps": [sg.timestamp for sg in self._subgroups],
            **limits,
            "subgroup_count": len(self._subgroups),
        }


class ProcessCapability:
    """Cp, Cpk, Pp, Ppk calculations."""

    @staticmethod
    def calculate(values: list[float], usl: float, lsl: float) -> dict:
        if len(values) < 2 or usl <= lsl:
            return {"cp": 0, "cpk": 0, "pp": 0, "ppk": 0, "mean": 0, "sigma": 0}

        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        sigma = math.sqrt(variance) if variance > 0 else 0.0001

        cp = (usl - lsl) / (6 * sigma) if sigma > 0 else 0
        cpu = (usl - mean) / (3 * sigma) if sigma > 0 else 0
        cpl = (mean - lsl) / (3 * sigma) if sigma > 0 else 0
        cpk = min(cpu, cpl)

        # Pp, Ppk use overall standard deviation (same as Cp/Cpk for single dataset)
        pp = cp
        ppk = cpk

        return {
            "cp": round(cp, 3),
            "cpk": round(cpk, 3),
            "pp": round(pp, 3),
            "ppk": round(ppk, 3),
            "mean": round(mean, 4),
            "sigma": round(sigma, 4),
            "usl": usl,
            "lsl": lsl,
            "sample_size": len(values),
        }


class NelsonRules:
    """Nelson rules for detecting out-of-control conditions."""

    @staticmethod
    def check_all(values: list[float], mean: float, sigma: float) -> list[dict]:
        violations = []
        if sigma <= 0 or len(values) < 2:
            return violations

        # Rule 1: Single point beyond 3-sigma
        for i, v in enumerate(values):
            if abs(v - mean) > 3 * sigma:
                violations.append({"rule": 1, "index": i, "value": round(v, 4),
                                    "description": "3-sigma ihlali"})

        # Rule 2: 9 consecutive points on same side of mean
        if len(values) >= 9:
            for i in range(len(values) - 8):
                window = values[i:i + 9]
                if all(v > mean for v in window) or all(v < mean for v in window):
                    violations.append({"rule": 2, "index": i + 8,
                                        "description": "9 ardisik nokta ayni tarafta"})
                    break

        # Rule 3: 6 consecutive increasing or decreasing
        if len(values) >= 6:
            for i in range(len(values) - 5):
                window = values[i:i + 6]
                diffs = [window[j + 1] - window[j] for j in range(5)]
                if all(d > 0 for d in diffs) or all(d < 0 for d in diffs):
                    violations.append({"rule": 3, "index": i + 5,
                                        "description": "6 ardisik artan/azalan"})
                    break

        # Rule 4: 14 consecutive alternating up/down
        if len(values) >= 14:
            for i in range(len(values) - 13):
                window = values[i:i + 14]
                diffs = [window[j + 1] - window[j] for j in range(13)]
                alternating = all(
                    (diffs[j] > 0 and diffs[j + 1] < 0) or (diffs[j] < 0 and diffs[j + 1] > 0)
                    for j in range(12)
                )
                if alternating:
                    violations.append({"rule": 4, "index": i + 13,
                                        "description": "14 ardisik alternatif"})
                    break

        # Rule 5: 2 of 3 beyond 2-sigma (same side)
        if len(values) >= 3:
            for i in range(len(values) - 2):
                window = values[i:i + 3]
                above = sum(1 for v in window if v > mean + 2 * sigma)
                below = sum(1 for v in window if v < mean - 2 * sigma)
                if above >= 2 or below >= 2:
                    violations.append({"rule": 5, "index": i + 2,
                                        "description": "3'ten 2'si 2-sigma otesinde"})
                    break

        return violations
