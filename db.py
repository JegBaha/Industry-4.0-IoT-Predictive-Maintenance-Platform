
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import execute_batch
from typing import Iterable, Sequence

from config import db as db_cfg


@contextmanager
def get_conn():
    conn = psycopg2.connect(db_cfg.uri)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def run_ddl(sql: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)


def insert_batch(sql: str, rows: Sequence[Sequence]) -> None:
    if not rows:
        return
    with get_conn() as conn, conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)


def fetch_all(sql: str) -> list[tuple]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def fetch_kpi_oee() -> list[dict]:
    """Fetch OEE KPIs from materialized view."""
    try:
        rows = fetch_all("SELECT machine_code, availability, performance, quality, oee FROM mv_kpi_oee")
        return [
            {
                "machine_code": r[0],
                "availability": float(r[1]) if r[1] else 0.0,
                "performance": float(r[2]) if r[2] else 0.0,
                "quality": float(r[3]) if r[3] else 0.0,
                "oee": float(r[4]) if r[4] else 0.0,
            }
            for r in rows
        ]
    except Exception:
        return []


def fetch_kpi_mttr() -> list[dict]:
    """Fetch MTTR KPIs from materialized view."""
    try:
        rows = fetch_all("SELECT machine_code, mttr_minutes FROM mv_mttr")
        return [
            {
                "machine_code": r[0],
                "mttr_minutes": float(r[1]) if r[1] else 0.0,
            }
            for r in rows
        ]
    except Exception:
        return []
