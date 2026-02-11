"""FFT-based vibration analysis and bearing diagnosis.

Provides frequency-domain analysis of vibration signals for condition monitoring,
ISO 10816 zone classification, bearing defect frequency detection, and RUL estimation.
"""
import math
import logging
import time
import random
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)


class FFTAnalyzer:
    """Computes FFT on vibration time-domain signals."""

    def __init__(self, sample_rate: int = 1000):
        self.sample_rate = sample_rate

    def analyze(self, signal: list[float]) -> dict:
        """Compute FFT spectrum from time-domain signal.

        Uses a simple DFT implementation (no numpy dependency required).
        For production, replace with numpy.fft.rfft.
        """
        n = len(signal)
        if n == 0:
            return {"frequencies": [], "amplitudes": [], "rms": 0, "peak_freq": 0}

        # RMS of time domain signal
        rms = math.sqrt(sum(x ** 2 for x in signal) / n)

        # Simple DFT (for small N) or use numpy if available
        try:
            import numpy as np
            spectrum = np.fft.rfft(signal)
            amplitudes = (2.0 / n) * np.abs(spectrum[:n // 2])
            frequencies = np.fft.rfftfreq(n, 1.0 / self.sample_rate)[:n // 2]
            amplitudes = amplitudes.tolist()
            frequencies = frequencies.tolist()
        except ImportError:
            # Fallback: simplified DFT for first 64 frequency bins
            num_bins = min(n // 2, 64)
            frequencies = []
            amplitudes = []
            for k in range(num_bins):
                freq = k * self.sample_rate / n
                re = sum(signal[i] * math.cos(2 * math.pi * k * i / n) for i in range(n))
                im = sum(signal[i] * math.sin(2 * math.pi * k * i / n) for i in range(n))
                amp = 2.0 / n * math.sqrt(re ** 2 + im ** 2)
                frequencies.append(round(freq, 2))
                amplitudes.append(round(amp, 6))

        peak_idx = amplitudes.index(max(amplitudes)) if amplitudes else 0
        peak_freq = frequencies[peak_idx] if frequencies else 0

        return {
            "frequencies": [round(f, 2) for f in frequencies[:128]],
            "amplitudes": [round(a, 6) for a in amplitudes[:128]],
            "rms": round(rms, 4),
            "peak_freq": round(peak_freq, 2),
            "sample_rate": self.sample_rate,
            "samples": n,
        }


@dataclass
class BearingConfig:
    """Bearing geometry for defect frequency calculation."""
    bearing_model: str = "6205"
    n_balls: int = 9
    ball_diameter: float = 7.94  # mm
    pitch_diameter: float = 38.5  # mm
    contact_angle: float = 0.0  # degrees
    rpm: float = 1500.0

    def bpfo(self) -> float:
        cos_a = math.cos(math.radians(self.contact_angle))
        return (self.n_balls / 2) * (self.rpm / 60) * (1 - self.ball_diameter / self.pitch_diameter * cos_a)

    def bpfi(self) -> float:
        cos_a = math.cos(math.radians(self.contact_angle))
        return (self.n_balls / 2) * (self.rpm / 60) * (1 + self.ball_diameter / self.pitch_diameter * cos_a)

    def bsf(self) -> float:
        cos_a = math.cos(math.radians(self.contact_angle))
        return (self.pitch_diameter / (2 * self.ball_diameter)) * (self.rpm / 60) * (
            1 - (self.ball_diameter / self.pitch_diameter * cos_a) ** 2
        )

    def ftf(self) -> float:
        cos_a = math.cos(math.radians(self.contact_angle))
        return (self.rpm / 60) / 2 * (1 - self.ball_diameter / self.pitch_diameter * cos_a)

    def defect_frequencies(self) -> dict:
        return {
            "BPFO": round(self.bpfo(), 2),
            "BPFI": round(self.bpfi(), 2),
            "BSF": round(self.bsf(), 2),
            "FTF": round(self.ftf(), 2),
        }


# ISO 10816 zone thresholds (mm/s RMS) for Group 1 machines (small machines)
ISO10816_ZONES = {
    "Group1": {"A": 0.71, "B": 1.80, "C": 4.50, "D": float("inf")},
    "Group2": {"A": 1.12, "B": 2.80, "C": 7.10, "D": float("inf")},
    "Group3": {"A": 1.80, "B": 4.50, "C": 11.2, "D": float("inf")},
    "Group4": {"A": 2.80, "B": 7.10, "C": 18.0, "D": float("inf")},
}


def classify_iso10816(rms_velocity: float, machine_group: str = "Group1") -> dict:
    zones = ISO10816_ZONES.get(machine_group, ISO10816_ZONES["Group1"])
    zone = "D"
    if rms_velocity <= zones["A"]:
        zone = "A"
    elif rms_velocity <= zones["B"]:
        zone = "B"
    elif rms_velocity <= zones["C"]:
        zone = "C"

    zone_labels = {"A": "Iyi", "B": "Kabul Edilebilir", "C": "Uyari", "D": "Tehlikeli"}
    return {
        "zone": zone,
        "label": zone_labels[zone],
        "rms_velocity": round(rms_velocity, 4),
        "machine_group": machine_group,
        "thresholds": zones,
    }


class RULEstimator:
    """Simple trend-based Remaining Useful Life estimation.

    Uses exponential degradation model: vibration increases exponentially
    as bearing/component approaches failure.
    """

    def __init__(self, failure_threshold: float = 7.1, model: str = "exponential"):
        self.failure_threshold = failure_threshold
        self.model = model
        self._history: dict[str, deque] = {}  # machine -> [(timestamp, rms)]

    def add_reading(self, machine_code: str, rms: float, timestamp: float = None):
        if machine_code not in self._history:
            self._history[machine_code] = deque(maxlen=500)
        self._history[machine_code].append((timestamp or time.time(), rms))

    def estimate(self, machine_code: str) -> dict:
        history = self._history.get(machine_code, [])
        if len(history) < 10:
            return {"rul_hours": -1, "confidence": 0, "message": "Yeterli veri yok (min 10 okuma)"}

        readings = list(history)
        timestamps = [r[0] for r in readings]
        values = [r[1] for r in readings]

        # Simple linear regression on log(values) for exponential model
        n = len(values)
        t_start = timestamps[0]
        t_hours = [(t - t_start) / 3600 for t in timestamps]

        # Filter out zero/negative values for log
        valid = [(t, v) for t, v in zip(t_hours, values) if v > 0]
        if len(valid) < 5:
            return {"rul_hours": -1, "confidence": 0, "message": "Yetersiz gecerli veri"}

        t_vals = [v[0] for v in valid]
        log_vals = [math.log(v[1]) for v in valid]

        # Linear regression: log(v) = a + b*t
        n_v = len(t_vals)
        sum_t = sum(t_vals)
        sum_lv = sum(log_vals)
        sum_t2 = sum(t ** 2 for t in t_vals)
        sum_tlv = sum(t * lv for t, lv in zip(t_vals, log_vals))

        denom = n_v * sum_t2 - sum_t ** 2
        if abs(denom) < 1e-10:
            return {"rul_hours": -1, "confidence": 0, "message": "Sabit sinyal"}

        b = (n_v * sum_tlv - sum_t * sum_lv) / denom
        a = (sum_lv - b * sum_t) / n_v

        # Current value and time
        current_t = t_vals[-1]
        current_v = values[-1]

        if b <= 0:
            return {"rul_hours": 999, "confidence": 0.3,
                    "trend": "stable", "message": "Bozulma trendi yok"}

        # Time to reach failure threshold
        log_threshold = math.log(self.failure_threshold)
        t_failure = (log_threshold - a) / b
        rul = max(0, t_failure - current_t)

        # R-squared for confidence
        mean_lv = sum_lv / n_v
        ss_tot = sum((lv - mean_lv) ** 2 for lv in log_vals)
        ss_res = sum((lv - (a + b * t)) ** 2 for t, lv in zip(t_vals, log_vals))
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        confidence = max(0, min(1, r_squared))

        return {
            "rul_hours": round(rul, 1),
            "confidence": round(confidence, 3),
            "trend_slope": round(b, 6),
            "current_rms": round(current_v, 4),
            "failure_threshold": self.failure_threshold,
            "trend": "degrading" if b > 0.001 else "stable",
        }


class VibrationSimulator:
    """Generates simulated vibration waveforms for testing."""

    def __init__(self, sample_rate: int = 1000, samples: int = 1024):
        self.sample_rate = sample_rate
        self.samples = samples

    def generate(self, bearing: BearingConfig, defect: str = None,
                 defect_amplitude: float = 0.1) -> list[float]:
        """Generate a synthetic vibration signal.

        Args:
            bearing: Bearing configuration for frequency calculation
            defect: Optional defect type ('outer', 'inner', 'ball', 'cage')
            defect_amplitude: Amplitude of the defect component
        """
        dt = 1.0 / self.sample_rate
        rpm_freq = bearing.rpm / 60.0

        signal = []
        defect_freq = 0
        if defect == "outer":
            defect_freq = bearing.bpfo()
        elif defect == "inner":
            defect_freq = bearing.bpfi()
        elif defect == "ball":
            defect_freq = bearing.bsf()
        elif defect == "cage":
            defect_freq = bearing.ftf()

        for i in range(self.samples):
            t = i * dt
            # Base vibration: fundamental rotation frequency + harmonics
            v = 0.1 * math.sin(2 * math.pi * rpm_freq * t)
            v += 0.05 * math.sin(2 * math.pi * 2 * rpm_freq * t)  # 2nd harmonic
            v += 0.02 * math.sin(2 * math.pi * 3 * rpm_freq * t)  # 3rd harmonic

            # Add defect signature
            if defect_freq > 0:
                v += defect_amplitude * math.sin(2 * math.pi * defect_freq * t)
                v += defect_amplitude * 0.5 * math.sin(2 * math.pi * 2 * defect_freq * t)

            # Noise
            v += random.gauss(0, 0.015)
            signal.append(v)

        return signal


# Default bearing configs per machine
DEFAULT_BEARINGS = {
    "MX100": BearingConfig(bearing_model="6205", n_balls=9, ball_diameter=7.94,
                           pitch_diameter=38.5, contact_angle=0, rpm=1500),
    "MX200": BearingConfig(bearing_model="6208", n_balls=9, ball_diameter=11.91,
                           pitch_diameter=54.99, contact_angle=0, rpm=1800),
}
