"""FastAPI router for MES Work Orders and Recipe Management."""
import logging
from typing import Optional
from fastapi import APIRouter
from pydantic import BaseModel

from .work_order import WorkOrderService
from .recipe import RecipeService

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/mes", tags=["MES"])

# Singletons
_wo_service: WorkOrderService | None = None
_recipe_service: RecipeService | None = None


def get_wo_service() -> WorkOrderService:
    global _wo_service
    if _wo_service is None:
        _wo_service = WorkOrderService()
    return _wo_service


def get_recipe_service() -> RecipeService:
    global _recipe_service
    if _recipe_service is None:
        _recipe_service = RecipeService()
    return _recipe_service


# ── Request Models ────────────────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    product_code: str
    machine_code: str
    target_qty: int
    recipe_id: Optional[int] = None


class ScrapRequest(BaseModel):
    qty: int
    reason: str = ""
    operator: str = "system"


class CreateRecipeRequest(BaseModel):
    recipe_code: str
    product_code: str
    description: str = ""
    parameters: list[dict] = []


class UpdateRecipeRequest(BaseModel):
    parameters: list[dict]
    changed_by: str = "system"


# ── Work Order Endpoints ─────────────────────────────────────────────────

@router.get("/orders")
def list_orders(status: str = None, machine_code: str = None):
    orders = get_wo_service().get_all(status, machine_code)
    return {"orders": [o.to_dict() for o in orders]}


@router.get("/orders/{wo_number}")
def get_order(wo_number: str):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"error": "Work order not found"}
    data = wo.to_dict()
    data["events"] = [
        {"event_type": e.event_type, "detail": e.detail, "operator": e.operator, "timestamp": e.timestamp}
        for e in wo.events[-50:]
    ]
    return data


@router.post("/orders")
def create_order(req: CreateOrderRequest):
    wo = get_wo_service().create(req.product_code, req.machine_code, req.target_qty, req.recipe_id)
    return {"ok": True, "order": wo.to_dict()}


@router.post("/orders/{wo_number}/start")
def start_order(wo_number: str, operator: str = "system"):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"ok": False, "error": "Not found"}
    ok = wo.start(operator)
    return {"ok": ok, "status": wo.status}


@router.post("/orders/{wo_number}/pause")
def pause_order(wo_number: str, operator: str = "system"):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"ok": False, "error": "Not found"}
    ok = wo.pause(operator)
    return {"ok": ok, "status": wo.status}


@router.post("/orders/{wo_number}/complete")
def complete_order(wo_number: str, operator: str = "system"):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"ok": False, "error": "Not found"}
    ok = wo.complete(operator)
    return {"ok": ok, "status": wo.status}


@router.post("/orders/{wo_number}/scrap")
def report_scrap(wo_number: str, req: ScrapRequest):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"ok": False, "error": "Not found"}
    wo.report_scrap(req.qty, req.reason, req.operator)
    return {"ok": True, "scrap_qty": wo.scrap_qty}


@router.get("/orders/{wo_number}/oee")
def order_oee(wo_number: str):
    wo = get_wo_service().get(wo_number)
    if not wo:
        return {"error": "Not found"}
    return {"wo_number": wo_number, **wo.oee}


@router.get("/shift-report")
def shift_report():
    return get_wo_service().get_shift_report()


# ── Recipe Endpoints ──────────────────────────────────────────────────────

@router.get("/recipes")
def list_recipes(status: str = None):
    recipes = get_recipe_service().get_all(status)
    return {"recipes": [r.to_dict() for r in recipes]}


@router.get("/recipes/{recipe_code}")
def get_recipe(recipe_code: str):
    r = get_recipe_service().get(recipe_code)
    if not r:
        return {"error": "Recipe not found"}
    return r.to_dict()


@router.post("/recipes")
def create_recipe(req: CreateRecipeRequest):
    r = get_recipe_service().create(req.recipe_code, req.product_code, req.description, req.parameters)
    return {"ok": True, "recipe": r.to_dict()}


@router.put("/recipes/{recipe_code}")
def update_recipe(recipe_code: str, req: UpdateRecipeRequest):
    r = get_recipe_service().update(recipe_code, req.parameters, req.changed_by)
    if not r:
        return {"ok": False, "error": "Recipe not found"}
    return {"ok": True, "recipe": r.to_dict()}


@router.post("/recipes/{recipe_code}/activate")
def activate_recipe(recipe_code: str):
    ok = get_recipe_service().activate(recipe_code)
    return {"ok": ok}


@router.post("/recipes/{recipe_code}/apply/{machine_code}")
def apply_recipe(recipe_code: str, machine_code: str):
    return get_recipe_service().apply_to_machine(recipe_code, machine_code)


@router.get("/recipes/{recipe_code}/versions")
def recipe_versions(recipe_code: str):
    return {"versions": get_recipe_service().get_versions(recipe_code)}


@router.get("/recipes/{recipe_code}/audit")
def recipe_audit(recipe_code: str):
    return {"audit": get_recipe_service().get_audit_trail(recipe_code)}
