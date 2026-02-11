"""FastAPI router for Digital Twin endpoints."""
import logging
from fastapi import APIRouter

from config import twin_cfg
from .twin_engine import TwinEngine

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/twin", tags=["DigitalTwin"])

# Singleton
_engine: TwinEngine | None = None


def get_engine() -> TwinEngine:
    global _engine
    if _engine is None:
        _engine = TwinEngine(
            warning_pct=twin_cfg.deviation_warning_pct,
            anomaly_pct=twin_cfg.deviation_anomaly_pct,
        )
    return _engine


@router.get("/status")
def twin_status_all():
    return {"machines": get_engine().get_all_status()}


@router.get("/{machine_code}/status")
def twin_status(machine_code: str):
    return get_engine().get_status(machine_code)


@router.get("/{machine_code}/comparison")
def twin_comparison(machine_code: str, limit: int = 50):
    return {"machine_code": machine_code, "comparison": get_engine().get_comparison(machine_code, limit)}


@router.get("/{machine_code}/deviations")
def twin_deviations(machine_code: str):
    e = get_engine()
    twin = e._get_twin(machine_code)
    return {"machine_code": machine_code, "deviations": twin.get_active_deviations()}


@router.get("/{machine_code}/health-score")
def twin_health(machine_code: str):
    return {"machine_code": machine_code, "health_score": get_engine().get_health_score(machine_code)}


@router.post("/calibrate/{machine_code}")
def twin_calibrate(machine_code: str):
    get_engine().calibrate(machine_code)
    return {"ok": True, "machine_code": machine_code}
