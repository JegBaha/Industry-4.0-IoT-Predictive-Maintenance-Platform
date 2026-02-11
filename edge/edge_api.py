"""FastAPI router for Edge Computing endpoints."""
import logging
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional

from config import edge_cfg
from .store_forward import StoreAndForward, EdgeRuleEngine

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/edge", tags=["Edge"])

_buffer: StoreAndForward | None = None
_rule_engine: EdgeRuleEngine | None = None


def get_buffer() -> StoreAndForward:
    global _buffer
    if _buffer is None:
        _buffer = StoreAndForward()
    return _buffer


def get_rule_engine() -> EdgeRuleEngine:
    global _rule_engine
    if _rule_engine is None:
        _rule_engine = EdgeRuleEngine()
    return _rule_engine


@router.get("/status")
def edge_status():
    return {
        "mode": edge_cfg.mode,
        "buffer": get_buffer().get_stats(),
        "rules": get_rule_engine().get_stats(),
    }


@router.get("/rules")
def list_rules():
    return {"rules": get_rule_engine().get_rules()}


class AddRuleRequest(BaseModel):
    rule_id: str
    sensor: str
    operator: str
    threshold: float
    action: str = "alarm"


@router.post("/rules")
def add_rule(req: AddRuleRequest):
    rule = get_rule_engine().add_rule(req.rule_id, req.sensor, req.operator,
                                      req.threshold, req.action)
    return {"ok": True, "rule": rule.to_dict()}


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    ok = get_rule_engine().remove_rule(rule_id)
    return {"ok": ok}


@router.post("/sync")
def force_sync():
    """Force forward buffered messages."""
    messages = get_buffer().forward(batch_size=500)
    count = len(messages)
    if messages:
        ids = [m[0] for m in messages]
        get_buffer().delete_forwarded(ids)
    return {"ok": True, "forwarded": count}


@router.get("/buffer/stats")
def buffer_stats():
    return get_buffer().get_stats()


@router.post("/buffer/cleanup")
def buffer_cleanup(max_age_hours: int = 24):
    deleted = get_buffer().cleanup(max_age_hours)
    return {"ok": True, "deleted": deleted}
