"""KPI Automation Service - OEE Detay, MTBF, Duruş Pareto, Tedarikçi Performansı.

Ported from KpiAutomation/src/pipeline.py KPICalculator class.
"""
import json
import logging
from pathlib import Path
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"


def _to_native(obj):
    """Convert numpy types to native Python for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_native(v) for v in obj]
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    return obj

_cache: dict = {
    "orders": None,
    "downtime": None,
    "suppliers": None,
    "config": None,
}

REASON_CODE_MAP = {
    "RC-001": {"name": "Mekanik Ariza", "category": "Ekipman", "priority": "High"},
    "RC-002": {"name": "Elektrik Sorunu", "category": "Ekipman", "priority": "High"},
    "RC-003": {"name": "Malzeme Eksikligi", "category": "Tedarik", "priority": "Medium"},
    "RC-004": {"name": "Kalite Kontrol", "category": "Planli", "priority": "Low"},
    "RC-005": {"name": "Sensor Arizasi", "category": "Ekipman", "priority": "High"},
    "RC-006": {"name": "Takim Degisimi", "category": "Planli", "priority": "Low"},
    "RC-007": {"name": "PLC Hatasi", "category": "Ekipman", "priority": "High"},
    "RC-008": {"name": "Onleyici Bakim", "category": "Planli", "priority": "Low"},
    "RC-009": {"name": "Hidrolik Kacak", "category": "Ekipman", "priority": "High"},
    "UNKNOWN": {"name": "Bilinmeyen", "category": "Diger", "priority": "Medium"},
}

OEE_TARGETS = {
    "availability": 90.0,
    "performance": 95.0,
    "quality": 99.0,
    "oee": 85.0,
}

IDEAL_CYCLE_TIMES = {
    "PRD-1001": 12, "PRD-1002": 15, "PRD-1003": 10, "PRD-1004": 14,
    "PRD-2001": 18, "PRD-2002": 16, "PRD-2003": 20, "default": 15,
}


def _load_config() -> dict:
    if _cache["config"] is not None:
        return _cache["config"]
    cfg_path = DATA_DIR / "kpi_config.json"
    if cfg_path.exists():
        with open(cfg_path, "r", encoding="utf-8") as f:
            _cache["config"] = json.load(f)
    else:
        _cache["config"] = {}
    return _cache["config"]


def _load_orders() -> pd.DataFrame:
    if _cache["orders"] is not None:
        return _cache["orders"]
    path = DATA_DIR / "SAP_Export_Orders.csv"
    if not path.exists():
        logger.warning("SAP_Export_Orders.csv not found")
        return pd.DataFrame()
    df = pd.read_csv(path)
    # Clean
    for col in ["Order_Date", "Due_Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ["Quantity_Produced", "Quantity_Good", "Quantity_Ordered"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    if "Cycle_Time_Sec" in df.columns:
        df = df[df["Cycle_Time_Sec"] > 0]
    df = df.drop_duplicates(subset=["Order_ID"], keep="first")
    _cache["orders"] = df
    logger.info(f"Loaded {len(df)} orders")
    return df


def _load_downtime() -> pd.DataFrame:
    if _cache["downtime"] is not None:
        return _cache["downtime"]
    path = DATA_DIR / "SAP_Export_Downtime.csv"
    if not path.exists():
        logger.warning("SAP_Export_Downtime.csv not found")
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["Start_Time", "End_Time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    if "Duration_Min" in df.columns:
        df = df[df["Duration_Min"] > 0]
    df = df.drop_duplicates(subset=["Downtime_ID"], keep="first")
    _cache["downtime"] = df
    logger.info(f"Loaded {len(df)} downtime records")
    return df


def _load_suppliers() -> pd.DataFrame:
    if _cache["suppliers"] is not None:
        return _cache["suppliers"]
    path = DATA_DIR / "Suppliers.csv"
    if not path.exists():
        logger.warning("Suppliers.csv not found")
        return pd.DataFrame()
    df = pd.read_csv(path)
    for col in ["Order_Date", "Expected_Date", "Actual_Delivery_Date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # Calculate delay
    if "Actual_Delivery_Date" in df.columns and "Expected_Date" in df.columns:
        df["Delay_Days"] = (df["Actual_Delivery_Date"] - df["Expected_Date"]).dt.days
        df["On_Time"] = df["Delay_Days"] <= 0
    _cache["suppliers"] = df
    logger.info(f"Loaded {len(df)} supplier records")
    return df


# ─── OEE Detail ───────────────────────────────────────────

def get_oee_detail(line: str = "ALL") -> dict:
    orders = _load_orders()
    downtime = _load_downtime()

    if orders.empty:
        return {"error": "No orders data"}

    if line and line != "ALL" and "Line" in orders.columns:
        orders = orders[orders["Line"] == line]
        if "Line" in downtime.columns:
            downtime = downtime[downtime["Line"] == line]

    # Planned time
    planned_time = orders["Planned_Time_Min"].sum() if "Planned_Time_Min" in orders.columns else 0

    # Unplanned downtime only
    if "Failure_Type" in downtime.columns:
        unplanned = downtime[downtime["Failure_Type"] == "Breakdown"]["Duration_Min"].sum()
        planned_dt = downtime[downtime["Failure_Type"] == "Planned"]["Duration_Min"].sum()
    else:
        unplanned = downtime["Duration_Min"].sum() if "Duration_Min" in downtime.columns else 0
        planned_dt = 0

    operating_time = max(planned_time - unplanned, 0)

    # Availability
    availability = operating_time / planned_time if planned_time > 0 else 0

    # Performance
    total_count = int(orders["Quantity_Produced"].sum()) if "Quantity_Produced" in orders.columns else 0
    ideal_time = 0
    for _, row in orders.iterrows():
        product = row.get("Product_Code", "")
        cycle = IDEAL_CYCLE_TIMES.get(product, IDEAL_CYCLE_TIMES["default"])
        qty = row.get("Quantity_Produced", 0) or 0
        ideal_time += (cycle * qty) / 60

    performance = ideal_time / operating_time if operating_time > 0 else 0
    performance = min(performance, 1.0)

    # Quality
    good_count = int(orders["Quantity_Good"].sum()) if "Quantity_Good" in orders.columns else 0
    quality = good_count / total_count if total_count > 0 else 0

    # OEE
    oee = availability * performance * quality

    return {
        "planned_time_min": round(planned_time, 1),
        "operating_time_min": round(operating_time, 1),
        "unplanned_downtime_min": round(unplanned, 1),
        "planned_downtime_min": round(planned_dt, 1),
        "total_count": total_count,
        "good_count": good_count,
        "defect_count": total_count - good_count,
        "availability_pct": round(availability * 100, 2),
        "performance_pct": round(performance * 100, 2),
        "quality_pct": round(quality * 100, 2),
        "oee_pct": round(oee * 100, 2),
        "targets": OEE_TARGETS,
        "lines_available": sorted(orders["Line"].unique().tolist()) if "Line" in orders.columns else [],
    }


# ─── MTTR / MTBF ──────────────────────────────────────────

def get_mttr_mtbf(line: str = "ALL") -> dict:
    downtime = _load_downtime()

    if downtime.empty:
        return {"mttr_min": 0, "mtbf_min": 0, "failure_count": 0}

    df = downtime.copy()
    if line and line != "ALL" and "Line" in df.columns:
        df = df[df["Line"] == line]

    if "Failure_Type" in df.columns:
        breakdowns = df[df["Failure_Type"] == "Breakdown"]
    else:
        breakdowns = df

    if breakdowns.empty:
        return {"mttr_min": 0, "mtbf_min": 0, "failure_count": 0}

    total_repair = breakdowns["Repair_Time_Min"].sum() if "Repair_Time_Min" in breakdowns.columns else 0
    failure_count = len(breakdowns)
    mttr = total_repair / failure_count if failure_count > 0 else 0

    # MTBF
    if "Start_Time" in breakdowns.columns:
        date_range = (breakdowns["Start_Time"].max() - breakdowns["Start_Time"].min()).total_seconds() / 60
        operating_time = max(date_range, 480)
    else:
        operating_time = 480 * failure_count

    mtbf = operating_time / failure_count if failure_count > 0 else 0

    return {
        "mttr_min": round(mttr, 2),
        "mtbf_min": round(mtbf, 2),
        "failure_count": failure_count,
        "total_repair_time_min": round(total_repair, 2),
        "mttr_hours": round(mttr / 60, 2),
        "mtbf_hours": round(mtbf / 60, 2),
    }


# ─── Downtime Pareto ──────────────────────────────────────

def get_downtime_pareto(top_n: int = 10) -> list[dict]:
    downtime = _load_downtime()

    if downtime.empty or "Reason_Code" not in downtime.columns:
        return []

    stats = downtime.groupby("Reason_Code").agg(
        total_duration=("Duration_Min", "sum"),
        occurrence_count=("Downtime_ID", "count"),
    ).reset_index()

    total = stats["total_duration"].sum()
    stats["duration_pct"] = (stats["total_duration"] / total * 100).round(2)
    stats = stats.sort_values("total_duration", ascending=False)
    stats["cumulative_pct"] = stats["duration_pct"].cumsum().round(2)

    results = []
    for _, row in stats.head(top_n).iterrows():
        code = row["Reason_Code"]
        info = REASON_CODE_MAP.get(code, REASON_CODE_MAP["UNKNOWN"])
        results.append({
            "reason_code": code,
            "reason_name": info["name"],
            "category": info["category"],
            "priority": info["priority"],
            "total_duration_min": round(float(row["total_duration"]), 1),
            "occurrence_count": int(row["occurrence_count"]),
            "duration_pct": float(row["duration_pct"]),
            "cumulative_pct": float(row["cumulative_pct"]),
        })
    return results


# ─── Supplier Performance ─────────────────────────────────

def get_supplier_performance() -> dict:
    suppliers = _load_suppliers()

    if suppliers.empty:
        return {"error": "No supplier data"}

    on_time_count = int(suppliers["On_Time"].sum()) if "On_Time" in suppliers.columns else 0
    total = len(suppliers)
    on_time_pct = round(on_time_count / total * 100, 2) if total > 0 else 0

    late = suppliers[suppliers["Delay_Days"] > 0] if "Delay_Days" in suppliers.columns else pd.DataFrame()
    avg_delay = round(float(late["Delay_Days"].mean()), 2) if not late.empty else 0

    # Per-supplier stats
    supplier_stats = suppliers.groupby("Supplier_ID").agg(
        supplier_name=("Supplier_Name", "first"),
        on_time_rate=("On_Time", "mean"),
        avg_delay=("Delay_Days", "mean"),
        avg_quality=("Quality_Score", "mean"),
        order_count=("PO_Number", "count"),
        country=("Country", "first"),
    ).reset_index()

    supplier_stats["on_time_pct"] = (supplier_stats["on_time_rate"] * 100).round(1)

    supplier_list = []
    for _, row in supplier_stats.sort_values("on_time_pct", ascending=False).iterrows():
        supplier_list.append({
            "supplier_id": row["Supplier_ID"],
            "supplier_name": row["supplier_name"],
            "on_time_pct": float(row["on_time_pct"]),
            "avg_delay_days": round(float(row["avg_delay"]), 2),
            "avg_quality_score": round(float(row["avg_quality"]), 1),
            "order_count": int(row["order_count"]),
            "country": row["country"],
        })

    return {
        "overall_on_time_pct": on_time_pct,
        "avg_delay_days": avg_delay,
        "total_deliveries": total,
        "on_time_deliveries": on_time_count,
        "late_deliveries": total - on_time_count,
        "supplier_count": len(supplier_list),
        "suppliers": supplier_list,
        "targets": {"on_time_pct": 95.0, "quality_min": 97.0, "max_delay_days": 2},
    }


# ─── Weekly OEE Trends ────────────────────────────────────

def get_weekly_trends() -> list[dict]:
    orders = _load_orders()
    downtime = _load_downtime()

    if orders.empty or "Order_Date" not in orders.columns:
        return []

    orders = orders.copy()
    orders["Week"] = orders["Order_Date"].dt.isocalendar().week.astype(int)
    orders["Year"] = orders["Order_Date"].dt.isocalendar().year.astype(int)

    trends = []
    for (year, week), grp in orders.groupby(["Year", "Week"]):
        week_start = grp["Order_Date"].min()
        week_end = grp["Order_Date"].max()

        # Filter downtime for this week
        if not downtime.empty and "Start_Time" in downtime.columns:
            wk_dt = downtime[(downtime["Start_Time"] >= week_start) & (downtime["Start_Time"] <= week_end)]
        else:
            wk_dt = pd.DataFrame()

        planned = grp["Planned_Time_Min"].sum() if "Planned_Time_Min" in grp.columns else 0
        unplanned_dt = 0
        if not wk_dt.empty and "Failure_Type" in wk_dt.columns:
            unplanned_dt = wk_dt[wk_dt["Failure_Type"] == "Breakdown"]["Duration_Min"].sum()
        elif not wk_dt.empty:
            unplanned_dt = wk_dt["Duration_Min"].sum()

        op_time = max(planned - unplanned_dt, 0)
        avail = op_time / planned if planned > 0 else 0

        total_produced = int(grp["Quantity_Produced"].sum()) if "Quantity_Produced" in grp.columns else 0
        good = int(grp["Quantity_Good"].sum()) if "Quantity_Good" in grp.columns else 0

        ideal_t = 0
        for _, r in grp.iterrows():
            cyc = IDEAL_CYCLE_TIMES.get(r.get("Product_Code", ""), IDEAL_CYCLE_TIMES["default"])
            ideal_t += (cyc * (r.get("Quantity_Produced", 0) or 0)) / 60
        perf = min(ideal_t / op_time, 1.0) if op_time > 0 else 0
        qual = good / total_produced if total_produced > 0 else 0
        oee = avail * perf * qual

        trends.append({
            "year": int(year),
            "week": int(week),
            "week_label": f"H{int(week):02d}",
            "week_start": week_start.strftime("%Y-%m-%d"),
            "orders_count": len(grp),
            "total_produced": total_produced,
            "availability_pct": round(avail * 100, 1),
            "performance_pct": round(perf * 100, 1),
            "quality_pct": round(qual * 100, 1),
            "oee_pct": round(oee * 100, 1),
        })

    return sorted(trends, key=lambda x: (x["year"], x["week"]))
