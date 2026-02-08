"""FastAPI router for ERP/MES endpoints."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .kpi_service import get_summary, get_trends
from .data_service import get_unified, refresh_cache
from .ml_service import erp_ml
from . import kpi_automation as kpi_auto
from .kpi_automation import _to_native

router = APIRouter(prefix="/api/erp", tags=["ERP/MES"])


class PredictRequest(BaseModel):
    temperature: float
    line_speed: float
    shift: str
    operator_experience: float
    machine_age: float


@router.get("/kpi/summary")
def kpi_summary():
    return get_summary()


@router.get("/kpi/trends")
def kpi_trends():
    return get_trends()


@router.get("/kpi/refresh")
def kpi_refresh():
    refresh_cache()
    return get_summary()


@router.get("/orders")
def orders_list():
    df = get_unified()
    records = []
    for _, row in df.iterrows():
        records.append({
            "order_id": row["order_id"],
            "planned_qty": int(row["planned_qty"]),
            "produced_qty": int(row["produced_qty"]),
            "defect_qty": int(row["defect_qty"]),
            "plan_fulfillment": round(float(row["plan_fulfillment"] * 100), 1),
            "delay_hours": round(float(row["delay_hours"]), 2),
            "scrap_rate": round(float(row["scrap_rate"] * 100), 2),
            "planned_start": str(row["planned_start"]),
            "planned_end": str(row["planned_end"]),
            "start_time": str(row["start_time"]),
            "end_time": str(row["end_time"]),
        })
    return records


@router.get("/orders/{order_id}")
def order_detail(order_id: str):
    df = get_unified()
    row = df[df["order_id"] == order_id]
    if row.empty:
        raise HTTPException(status_code=404, detail="Order not found")
    r = row.iloc[0]
    return {
        "order_id": r["order_id"],
        "planned_qty": int(r["planned_qty"]),
        "produced_qty": int(r["produced_qty"]),
        "defect_qty": int(r["defect_qty"]),
        "plan_fulfillment": round(float(r["plan_fulfillment"] * 100), 1),
        "delay_hours": round(float(r["delay_hours"]), 2),
        "scrap_rate": round(float(r["scrap_rate"] * 100), 2),
        "planned_start": str(r["planned_start"]),
        "planned_end": str(r["planned_end"]),
        "start_time": str(r["start_time"]),
        "end_time": str(r["end_time"]),
    }


@router.post("/predict")
def predict(req: PredictRequest):
    result = erp_ml.predict(
        temperature=req.temperature,
        line_speed=req.line_speed,
        shift=req.shift,
        operator_experience=req.operator_experience,
        machine_age=req.machine_age,
    )
    return result


@router.get("/predict/feature-importance")
def feature_importance():
    return erp_ml.get_feature_importance()


@router.get("/predict/temperature-curve")
def temperature_curve():
    return erp_ml.get_temperature_curve()


@router.get("/analytics")
def analytics():
    return erp_ml.get_analytics()


# ─── KPI Automation Endpoints ─────────────────────────────

@router.get("/oee/detail")
def oee_detail(line: str = "ALL"):
    return _to_native(kpi_auto.get_oee_detail(line))


@router.get("/oee/mttr-mtbf")
def mttr_mtbf(line: str = "ALL"):
    return _to_native(kpi_auto.get_mttr_mtbf(line))


@router.get("/oee/weekly-trends")
def oee_weekly_trends():
    return _to_native(kpi_auto.get_weekly_trends())


@router.get("/downtime/pareto")
def downtime_pareto(top_n: int = 10):
    return _to_native(kpi_auto.get_downtime_pareto(top_n))


@router.get("/suppliers")
def supplier_performance():
    return _to_native(kpi_auto.get_supplier_performance())
