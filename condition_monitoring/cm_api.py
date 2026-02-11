"""FastAPI router for Condition Monitoring endpoints."""
import logging
import time
from fastapi import APIRouter

from config import cm_cfg
from .fft_analysis import (
    FFTAnalyzer, BearingConfig, VibrationSimulator,
    RULEstimator, classify_iso10816, DEFAULT_BEARINGS,
)

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/cm", tags=["ConditionMonitoring"])

# Singletons
_fft = FFTAnalyzer(sample_rate=cm_cfg.sample_rate)
_simulator = VibrationSimulator(sample_rate=cm_cfg.sample_rate, samples=cm_cfg.fft_size)
_rul = RULEstimator(failure_threshold=7.1, model=cm_cfg.rul_model)
_latest_spectrums: dict[str, dict] = {}


def feed_vibration(machine_code: str, rms: float, timestamp: float = None):
    """Called from main listener to feed vibration data."""
    _rul.add_reading(machine_code, rms, timestamp or time.time())


@router.get("/{machine_code}/spectrum")
def get_spectrum(machine_code: str, defect: str = None):
    """Get FFT spectrum for a machine (simulated waveform)."""
    bearing = DEFAULT_BEARINGS.get(machine_code, DEFAULT_BEARINGS["MX100"])
    signal = _simulator.generate(bearing, defect=defect)
    spectrum = _fft.analyze(signal)
    spectrum["bearing_frequencies"] = bearing.defect_frequencies()
    _latest_spectrums[machine_code] = spectrum
    return {"machine_code": machine_code, **spectrum}


@router.get("/{machine_code}/iso10816")
def get_iso10816(machine_code: str, machine_group: str = "Group1"):
    """Get ISO 10816 zone classification for current vibration level."""
    spectrum = _latest_spectrums.get(machine_code)
    rms = spectrum["rms"] if spectrum else 0.3
    classification = classify_iso10816(rms, machine_group)
    return {"machine_code": machine_code, **classification}


@router.get("/{machine_code}/bearing")
def get_bearing_diagnosis(machine_code: str):
    """Get bearing defect frequency analysis."""
    bearing = DEFAULT_BEARINGS.get(machine_code, DEFAULT_BEARINGS["MX100"])
    freqs = bearing.defect_frequencies()

    # Check latest spectrum for peaks near defect frequencies
    spectrum = _latest_spectrums.get(machine_code)
    diagnosis = {"healthy": True, "defects": []}

    if spectrum and spectrum.get("frequencies"):
        frequencies = spectrum["frequencies"]
        amplitudes = spectrum["amplitudes"]
        for defect_name, defect_freq in freqs.items():
            # Find nearest frequency bin
            closest_idx = min(range(len(frequencies)),
                              key=lambda i: abs(frequencies[i] - defect_freq),
                              default=None)
            if closest_idx is not None:
                amp = amplitudes[closest_idx]
                # Threshold: amplitude > 0.05 suggests defect
                if amp > 0.05:
                    diagnosis["healthy"] = False
                    diagnosis["defects"].append({
                        "type": defect_name,
                        "frequency": defect_freq,
                        "amplitude": round(amp, 4),
                        "severity": "high" if amp > 0.15 else "medium" if amp > 0.08 else "low",
                    })

    return {
        "machine_code": machine_code,
        "bearing_model": bearing.bearing_model,
        "defect_frequencies": freqs,
        "rpm": bearing.rpm,
        **diagnosis,
    }


@router.get("/{machine_code}/rul")
def get_rul(machine_code: str):
    """Get Remaining Useful Life estimate."""
    return {"machine_code": machine_code, **_rul.estimate(machine_code)}


@router.get("/{machine_code}/trend")
def get_vibration_trend(machine_code: str, limit: int = 100):
    """Get vibration RMS trend data."""
    history = list(_rul._history.get(machine_code, []))[-limit:]
    return {
        "machine_code": machine_code,
        "trend": [{"timestamp": t, "rms": round(v, 4)} for t, v in history],
    }
