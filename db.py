# alma_ops/db.py
import sqlite3
import json
from contextlib import contextmanager
from datetime import datetime

def get_db_connection(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@contextmanager
def db_transaction(conn):
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise

def db_fetch_one(conn, query, params=()):
    cur = conn.execute(query, params)
    return cur.fetchone()

def db_fetch_all(conn, query, params=()):
    cur = conn.execute(query, params)
    return cur.fetchall()

def db_execute(conn, query, params=(), commit=False):
    conn.execute(query, params)
    if commit:
        conn.commit()

# ---------------------
# MOUS getters
# ---------------------

def get_mous_record(conn, mous_id: str):
    return db_fetch_one(conn, "SELECT * FROM mous WHERE mous_id=?", (mous_id,))

def get_mous_download_url(conn, mous_id: str) -> str:
    row = db_fetch_one(conn, "SELECT download_url FROM mous WHERE mous_id=?", (mous_id,))
    return row["download_url"] if row else None

def get_mous_targets(conn, mous_id: str):
    return db_fetch_all(conn, "SELECT * FROM targets WHERE mous_id=?", (mous_id,))

def get_unique_target_names(conn, mous_id: str):
    rows = db_fetch_all(conn,
        "SELECT DISTINCT alma_source_name FROM targets WHERE mous_id=?",
        (mous_id,)
    )
    return [r["alma_source_name"] for r in rows]

def get_mous_asdms_from_targets(conn, mous_id: str):
    rows = db_fetch_all(conn,
        "SELECT DISTINCT asdm_uid FROM targets WHERE mous_id=?",
        (mous_id,)
    )
    return [r["asdm_uid"] for r in rows if r["asdm_uid"]]

def get_mous_expected_asdms(conn, mous_id: str) -> int:
    row = db_fetch_one(conn, "SELECT num_asdms FROM mous WHERE mous_id=?", (mous_id,))
    if row is None or row["num_asdms"] is None:
        return 0
    return int(row["num_asdms"])

# ---------------------
# MOUS update helpers
# ---------------------

def update_mous_split_state(
    conn,
    mous_id: str,
    split_status: str,
    split_products: list[str] | None,
    note: str
):
    with db_transaction(conn):
        conn.execute("""
            UPDATE mous
            SET split_status=?,
                split_date=?,
                split_products=?,
                split_notes=?
            WHERE mous_id=?
        """, (
            split_status,
            datetime.utcnow().isoformat(),
            json.dumps(split_products or []),
            note,
            mous_id
        ))
