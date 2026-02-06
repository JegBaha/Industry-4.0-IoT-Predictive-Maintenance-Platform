"""KPI hesaplama servisi - Plan fulfillment, delay, scrap rate."""
from .data_service import get_unified


def get_summary() -> dict:
    df = get_unified()
    total_orders = len(df)
    total_produced = int(df["produced_qty"].sum())
    total_planned = int(df["planned_qty"].sum())
    total_defects = int(df["defect_qty"].sum())

    return {
        "plan_fulfillment_mean": round(float(df["plan_fulfillment"].mean() * 100), 1),
        "delay_hours_mean": round(float(df["delay_hours"].mean()), 2),
        "scrap_rate_mean": round(float(df["scrap_rate"].mean() * 100), 2),
        "total_orders": total_orders,
        "total_produced": total_produced,
        "total_planned": total_planned,
        "total_defects": total_defects,
        "overall_fulfillment": round(total_produced / total_planned * 100, 1) if total_planned else 0,
    }


def get_trends() -> list[dict]:
    df = get_unified()
    df = df.copy()
    df["date"] = df["planned_end"].dt.date.astype(str)

    daily = df.groupby("date").agg(
        plan_fulfillment=("plan_fulfillment", "mean"),
        delay_hours=("delay_hours", "mean"),
        scrap_rate=("scrap_rate", "mean"),
        order_count=("order_id", "count"),
    ).reset_index()

    return [
        {
            "date": row["date"],
            "plan_fulfillment": round(float(row["plan_fulfillment"] * 100), 1),
            "delay_hours": round(float(row["delay_hours"]), 2),
            "scrap_rate": round(float(row["scrap_rate"] * 100), 2),
            "order_count": int(row["order_count"]),
        }
        for _, row in daily.iterrows()
    ]
