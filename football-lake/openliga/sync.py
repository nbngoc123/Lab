"""Backfill một lần + sync worker incremental."""
import logging
import time
from datetime import datetime, timezone

from . import config, db, loader
from .client import NotFound

log = logging.getLogger("openliga.sync")


# =============================================================================
# BACKFILL (2024 -> nay)
# =============================================================================
def backfill(conn, client, seasons=None, only=None, with_result_infos=True, force=False):
    """only: tập shortcut cần nạp (None = tất cả). force=False -> bỏ qua giải-mùa đã nạp (resume)."""
    seasons = seasons or config.SEASONS
    stats = db.Stats()
    log.info("Backfill mùa %s ...", seasons)
    league_rows = loader.load_reference(conn, client, seasons, stats)
    for season in seasons:                                   # đã refresh danh sách league -> worker khỏi làm lại ngay
        db.set_state(conn, "__leagues__", season, -2)
    if only:
        league_rows = {k: v for k, v in league_rows.items() if v["shortcut"] in only}

    state = db.load_state(conn)
    pairs = sorted({(v["shortcut"], v["season"]) for v in league_rows.values()},
                   key=lambda p: (p[1], p[0]))
    failures = []
    for i, (sc, season) in enumerate(pairs, 1):
        if not force and (sc, season, -1) in state:
            log.info("[%d/%d] %s/%d đã nạp -> bỏ qua", i, len(pairs), sc, season)
            continue
        try:
            n = loader.sync_league_season(conn, client, sc, season, stats)
            log.info("[%d/%d] %s/%d: %d trận", i, len(pairs), sc, season, n)
        except NotFound:
            log.warning("[%d/%d] %s/%d: 404", i, len(pairs), sc, season)
            failures.append((sc, season, "404"))
        except Exception as e:
            conn.rollback()
            log.error("[%d/%d] %s/%d lỗi: %s", i, len(pairs), sc, season, e)
            failures.append((sc, season, str(e)))

    if with_result_infos:
        for j, lid in enumerate(sorted(league_rows), 1):
            try:
                loader.load_result_infos(conn, client, lid, stats)
            except NotFound:
                pass
            except Exception as e:
                conn.rollback()
                failures.append((lid, "result_infos", str(e)))
            if j % 25 == 0:
                log.info("result_infos %d/%d", j, len(league_rows))

    log.info("Backfill xong. API requests: %d. Thay đổi:\n%s", client.requests_made, stats)
    if failures:
        log.warning("Có %d lỗi (chạy lại lệnh backfill để resume): %s", len(failures), failures[:10])
    return stats, failures


# =============================================================================
# SYNC WORKER
# =============================================================================
_HOT_SQL = """
SELECT l.shortcut, l.season, g.group_order,
       bool_or(NOT m.is_finished AND m.match_time_utc BETWEEN now() - interval '3 hours'
                                                          AND now() + interval '10 minutes') AS is_live
FROM matches m
JOIN groups  g ON g.group_id  = m.group_id
JOIN leagues l ON l.league_id = m.league_id
WHERE l.season = ANY(%s)
  AND ( (NOT m.is_finished AND m.match_time_utc BETWEEN now() - interval '14 days' AND now() + interval '2 days')
     OR (m.match_time_utc BETWEEN now() - interval '3 days' AND now() + interval '2 days') )
GROUP BY l.shortcut, l.season, g.group_order
"""


def _age(ts, now):
    return float("inf") if ts is None else (now - ts).total_seconds()


def plan_tick(conn, now=None):
    """Quyết định việc cần làm ở tick này, ưu tiên: nhóm live > nhóm hot > reconcile."""
    now = now or datetime.now(timezone.utc)
    state = db.load_state(conn)
    with conn.cursor() as cur:
        cur.execute(_HOT_SQL, (config.LIVE_SEASONS,))
        hot = cur.fetchall()
        cur.execute("SELECT DISTINCT shortcut, season FROM leagues WHERE season = ANY(%s)", (config.LIVE_SEASONS,))
        live_pairs = cur.fetchall()

    live_t, hot_t = [], []
    for sc, season, go, is_live in hot:
        chk = state.get((sc, season, go), (None, None))[1]
        interval = config.LIVE_INTERVAL if is_live else config.HOT_INTERVAL
        if _age(chk, now) >= interval:
            (live_t if is_live else hot_t).append(("group", sc, season, go))

    rec_t = []
    for season in config.LIVE_SEASONS:                       # refresh danh sách league (phát hiện league mới)
        if _age(state.get(("__leagues__", season, -2), (None, None))[1], now) >= config.RECONCILE_INTERVAL:
            rec_t.append(("leagues", "__leagues__", season, -2))
    for sc, season in sorted(live_pairs):                    # full reconcile mỗi RECONCILE_INTERVAL
        if _age(state.get((sc, season, -1), (None, None))[1], now) >= config.RECONCILE_INTERVAL:
            rec_t.append(("season", sc, season, -1))

    return (live_t + hot_t + rec_t)[: config.MAX_TASKS_PER_TICK]


def run_task(conn, client, task, stats):
    kind, sc, season, go = task
    if kind == "leagues":
        loader.load_reference(conn, client, [season], stats)
        db.set_state(conn, sc, season, go)
    elif kind == "season":
        n = loader.sync_league_season(conn, client, sc, season, stats)
        log.info("reconcile %s/%d: %d trận", sc, season, n)
    elif kind == "group":
        raw = client.last_change(sc, season, go)             # 1 request rẻ
        old = db.load_state(conn).get((sc, season, go), (None, None))[0]
        if raw is None or raw == old:
            db.set_state(conn, sc, season, go, synced=False)   # chỉ đánh dấu đã kiểm tra
            return
        n = loader.sync_group(conn, client, sc, season, go, raw, stats)
        log.info("group %s/%d/%d đổi (%s) -> %d trận", sc, season, go, raw, n)


def tick(conn, client):
    stats = db.Stats()
    tasks = plan_tick(conn)
    for t in tasks:
        try:
            run_task(conn, client, t, stats)
        except NotFound:
            db.set_state(conn, t[1], t[2], t[3], synced=False)
        except Exception as e:
            conn.rollback()
            log.error("task %s lỗi: %s", t, e)
    if stats.total_changes():
        log.info("tick: %d task, %d request, thay đổi:\n%s", len(tasks), client.requests_made, stats)
    else:
        log.debug("tick: %d task, không có thay đổi", len(tasks))
    return stats


def run_forever(conn, client):
    log.info("Sync worker chạy: live=%ss hot=%ss reconcile=%ss tick=%ss",
             config.LIVE_INTERVAL, config.HOT_INTERVAL, config.RECONCILE_INTERVAL, config.TICK_SECONDS)
    while True:
        try:
            tick(conn, client)
        except Exception as e:
            log.exception("tick lỗi: %s", e)
            try:
                conn.rollback()
            except Exception:
                conn = db.connect()
            time.sleep(60)
        time.sleep(config.TICK_SECONDS)
