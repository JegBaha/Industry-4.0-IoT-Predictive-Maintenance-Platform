"""FastAPI router for Traceability endpoints."""
import logging
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel

from .trace_service import TraceService

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/trace", tags=["Traceability"])

_service: TraceService | None = None


def get_service() -> TraceService:
    global _service
    if _service is None:
        _service = TraceService()
    return _service


@router.get("/parts")
def search_parts(wo_number: str = None, machine_code: str = None,
                 batch_number: str = None, limit: int = 50):
    parts = get_service().search(wo_number, machine_code, batch_number, limit)
    return {"parts": [p.to_dict() for p in parts]}


@router.get("/parts/{dmc_code}")
def get_part(dmc_code: str):
    part = get_service().get_part(dmc_code)
    if not part:
        return {"error": "Part not found"}
    return part.to_dict()


@router.get("/reverse/{dmc_code}")
def reverse_trace(dmc_code: str):
    return get_service().reverse_trace(dmc_code)


class PartEventRequest(BaseModel):
    event_type: str
    detail: dict = {}


@router.post("/parts/{dmc_code}/event")
def add_event(dmc_code: str, req: PartEventRequest):
    ok = get_service().add_event(dmc_code, req.event_type, req.detail)
    return {"ok": ok}


@router.get("/batch/{batch_number}")
def batch_report(batch_number: str):
    return get_service().get_batch_report(batch_number)


@router.get("/stats")
def trace_stats():
    return get_service().get_stats()
