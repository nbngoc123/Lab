"""Kết nối Postgres + upsert 'chỉ ghi khi khác' + đếm insert/update/delete."""
import hashlib
import json
import logging
from collections import defaultdict
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

from . import config

log = logging.getLogger("openliga.db")
SCHEMA_FILE = Path(__file__).with_name("schema.sql")


def connect(dbname=None):
    return psycopg2.connect(host=config.PG_HOST, port=config.PG_PORT, user=config.PG_USER,
                            password=config.PG_PASSWORD, dbname=dbname or config.PG_DB)


def init_db():
    """Tạo database nếu chưa có (kết nối DB 'postgres') rồi áp schema."""
    admin = connect("postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (config.PG_DB,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{config.PG_DB}"')
            log.info("Đã tạo database %s", config.PG_DB)
    admin.close()
    conn = connect()
    apply_schema(conn)
    return conn


def apply_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    conn.commit()


class Stats:
    """Đếm số dòng insert / update / delete theo bảng."""
    def __init__(self):
        self.d = defaultdict(lambda: {"ins": 0, "upd": 0, "del": 0})

    def add(self, table, ins=0, upd=0, dele=0):
        if not (ins or upd or dele):
            return
        x = self.d[table]; x["ins"] += ins; x["upd"] += upd; x["del"] += dele

    def total_changes(self):
        return sum(v["ins"] + v["upd"] + v["del"] for v in self.d.values())

    def __str__(self):
        if not self.d:
            return "(không có thay đổi)"
        return "\n".join(f"  {t:<20} +{v['ins']:<6} ~{v['upd']:<6} -{v['del']:<6}"
                         for t, v in sorted(self.d.items()))


def upsert(cur, stats, table, rows, pk, cols, nocompare=()):
    """INSERT ... ON CONFLICT DO UPDATE ... WHERE (cũ) IS DISTINCT FROM (mới).
    Nếu dữ liệu không đổi thì KHÔNG có UPDATE nào -> WAL im lặng."""
    if not rows:
        return
    pks = [pk] if isinstance(pk, str) else list(pk)
    non_pk = [c for c in cols if c not in pks]
    cmp_cols = [c for c in non_pk if c not in nocompare]
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
           f"ON CONFLICT ({', '.join(pks)}) DO UPDATE SET "
           + ", ".join(f"{c}=EXCLUDED.{c}" for c in non_pk))
    if cmp_cols:
        lhs = ", ".join(f"{table}.{c}" for c in cmp_cols)
        rhs = ", ".join(f"EXCLUDED.{c}" for c in cmp_cols)
        sql += f" WHERE ({lhs}) IS DISTINCT FROM ({rhs})"
    sql += " RETURNING (xmax = 0)"
    vals = [tuple(r[c] for c in cols) for r in rows]
    res = execute_values(cur, sql, vals, page_size=500, fetch=True)
    ins = sum(1 for (is_ins,) in res if is_ins)
    stats.add(table, ins=ins, upd=len(res) - ins)


def delete_orphans(cur, stats, table, key_col, parent_col, parent_ids, keep_ids):
    """Xoá các dòng con của parent_ids mà không còn nằm trong payload (API không có tombstone)."""
    if not parent_ids:
        return
    cur.execute(f"DELETE FROM {table} WHERE {parent_col} = ANY(%s::int[]) AND {key_col} <> ALL(%s::int[])",
                (list(parent_ids), list(keep_ids)))
    stats.add(table, dele=cur.rowcount)


# --- sync_state ---------------------------------------------------------------
def load_state(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT league_shortcut, season, group_order, last_change_raw, last_checked_at "
                    "FROM sync_state")
        return {(a, b, c): (raw, chk) for a, b, c, raw, chk in cur.fetchall()}


def set_state(conn, shortcut, season, group_order, raw=None, synced=True):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO sync_state (league_shortcut, season, group_order, last_change_raw,
                                       last_checked_at, last_synced_at)
               VALUES (%s,%s,%s,%s, now(), CASE WHEN %s THEN now() END)
               ON CONFLICT (league_shortcut, season, group_order) DO UPDATE SET
                 last_change_raw = COALESCE(EXCLUDED.last_change_raw, sync_state.last_change_raw),
                 last_checked_at = now(),
                 last_synced_at  = CASE WHEN %s THEN now() ELSE sync_state.last_synced_at END""",
            (shortcut, season, group_order, raw, synced, synced))
    conn.commit()


def log_raw(cur, endpoint, payload):
    """Ghi payload thô (chỉ khi payload đổi so với lần gần nhất của endpoint)."""
    if not config.RAW_LOG:
        return
    sha = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    cur.execute("SELECT payload_sha FROM raw_api_log WHERE endpoint=%s ORDER BY id DESC LIMIT 1", (endpoint,))
    row = cur.fetchone()
    if row and row[0] == sha:
        return
    cur.execute("INSERT INTO raw_api_log (endpoint, payload_sha, payload) VALUES (%s,%s,%s)",
                (endpoint, sha, Json(payload)))
