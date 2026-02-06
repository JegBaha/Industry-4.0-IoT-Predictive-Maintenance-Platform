"""MES-ERP veri yükleme, birleştirme ve cache servisi."""
from pathlib import Path
import pandas as pd
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"

_cache: dict = {"unified": None, "mes": None, "erp": None}


def load_mes(path: str | Path = DATA_DIR / "mes.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["end_time"] = pd.to_datetime(df["end_time"])
    return df


def load_erp(path: str | Path = DATA_DIR / "erp.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["planned_start"] = pd.to_datetime(df["planned_start"])
    df["planned_end"] = pd.to_datetime(df["planned_end"])
    return df


def build_unified() -> pd.DataFrame:
    if _cache["unified"] is not None:
        return _cache["unified"]

    mes = load_mes()
    erp = load_erp()
    _cache["mes"] = mes
    _cache["erp"] = erp

    unified = pd.merge(erp, mes, on="order_id", how="inner", suffixes=("_plan", "_actual"))
    unified["plan_fulfillment"] = unified["produced_qty"] / unified["planned_qty"]
    unified["delay_hours"] = (unified["end_time"] - unified["planned_end"]).dt.total_seconds() / 3600
    unified["scrap_rate"] = unified["defect_qty"] / unified["produced_qty"].replace(0, pd.NA)

    _cache["unified"] = unified
    logger.info(f"Unified table built: {len(unified)} rows")
    return unified


def get_unified() -> pd.DataFrame:
    return build_unified()


def refresh_cache():
    _cache["unified"] = None
    _cache["mes"] = None
    _cache["erp"] = None
    return build_unified()
