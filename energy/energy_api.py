"""FastAPI router for Energy Monitoring (ISO 50001)."""
import logging
from fastapi import APIRouter

from .energy_service import EnergyService

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/energy", tags=["Energy"])

_service: EnergyService | None = None


def get_service() -> EnergyService:
    global _service
    if _service is None:
        _service = EnergyService()
    return _service


@router.get("/realtime")
def energy_realtime():
    return {"machines": get_service().get_realtime()}


@router.get("/summary")
def energy_summary():
    return get_service().get_summary()


@router.get("/per-part")
def energy_per_part():
    machines = get_service().get_realtime()
    return {
        "per_part": {code: m["kwh_per_part"] for code, m in machines.items()}
    }


@router.get("/shift-report")
def energy_shift_report():
    return get_service().get_shift_report()


@router.get("/trends/{machine_code}")
def energy_trends(machine_code: str, limit: int = 100):
    return {"machine_code": machine_code, "trend": get_service().get_trend(machine_code, limit)}
