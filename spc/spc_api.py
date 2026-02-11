"""FastAPI router for SPC — Statistical Process Control."""
import logging
import time
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from config import spc_cfg
from .control_chart import ControlChart, ProcessCapability, NelsonRules

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/spc", tags=["SPC"])

# Per-machine, per-sensor control charts
_charts: dict[str, ControlChart] = {}

# Spec limits per sensor type
SPEC_LIMITS = {
    "temperature": {"usl": 70.0, "lsl": 20.0},
    "vibration": {"usl": 0.7, "lsl": 0.0},
    "current": {"usl": 15.0, "lsl": 0.0},
}


def _key(machine: str, sensor: str) -> str:
    return f"{machine}:{sensor}"


def get_chart(machine: str, sensor: str) -> ControlChart:
    k = _key(machine, sensor)
    if k not in _charts:
        _charts[k] = ControlChart(
            subgroup_size=spc_cfg.subgroup_size,
            max_subgroups=spc_cfg.history_subgroups,
        )
    return _charts[k]


def feed_sample(machine: str, sensor: str, value: float, timestamp: float = 0):
    """Called by the main listener to feed sensor data into SPC charts."""
    chart = get_chart(machine, sensor)
    chart.add_sample(value, timestamp or time.time())


@router.get("/chart/{machine_code}/{sensor}")
def get_chart_data(machine_code: str, sensor: str):
    chart = get_chart(machine_code, sensor)
    return {
        "machine_code": machine_code,
        "sensor": sensor,
        "xbar": chart.get_xbar_data(),
        "r_chart": chart.get_r_data(),
    }


@router.get("/capability/{machine_code}/{sensor}")
def get_capability(machine_code: str, sensor: str):
    chart = get_chart(machine_code, sensor)
    # Collect all individual values from subgroups
    all_values = []
    for sg in chart._subgroups:
        all_values.extend(sg.values)

    limits = SPEC_LIMITS.get(sensor, {"usl": 100, "lsl": 0})
    if not all_values:
        return {"machine_code": machine_code, "sensor": sensor,
                "message": "Yeterli veri yok", "cp": 0, "cpk": 0}

    result = ProcessCapability.calculate(all_values, limits["usl"], limits["lsl"])
    return {"machine_code": machine_code, "sensor": sensor, **result}


@router.get("/violations/{machine_code}")
def get_violations(machine_code: str):
    violations = {}
    for sensor in ["temperature", "vibration", "current"]:
        chart = get_chart(machine_code, sensor)
        xbar_data = chart.get_xbar_data()
        values = xbar_data["values"]
        if len(values) >= 2:
            mean = xbar_data["cl"]
            sigma = chart.sigma_estimate
            v = NelsonRules.check_all(values, mean, sigma)
            if v:
                violations[sensor] = v
    return {"machine_code": machine_code, "violations": violations}


class SPCConfigRequest(BaseModel):
    subgroup_size: Optional[int] = None
    usl: Optional[float] = None
    lsl: Optional[float] = None
    sensor: Optional[str] = None


@router.post("/configure")
def configure_spc(req: SPCConfigRequest):
    if req.sensor and req.usl is not None and req.lsl is not None:
        SPEC_LIMITS[req.sensor] = {"usl": req.usl, "lsl": req.lsl}
    return {"ok": True, "spec_limits": SPEC_LIMITS}
