"""Database helpers."""
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
