"""python -m openliga.cli <init|backfill|sync|once|stats|verify|register>"""
import argparse
import logging
import sys

from . import config, db, sync
from .client import OpenLigaClient


def main(argv=None):
    p = argparse.ArgumentParser(prog="openliga")
    sp = p.add_subparsers(dest="cmd", required=True)
    sp.add_parser("init", help="tạo database + schema + publication")
    b = sp.add_parser("backfill", help="nạp 2024 -> nay (resume được)")
    b.add_argument("--seasons", help="vd 2024,2025")
    b.add_argument("--only", help="chỉ các shortcut, vd bl1,bl2")
    b.add_argument("--force", action="store_true", help="nạp lại cả giải-mùa đã có")
    b.add_argument("--no-result-infos", action="store_true")
    sp.add_parser("sync", help="worker chạy liên tục")
    sp.add_parser("once", help="chạy đúng 1 tick rồi thoát")
    sp.add_parser("stats", help="đếm số dòng từng bảng")
    v = sp.add_parser("verify", help="đối chiếu DB với API cho 1 giải-mùa")
    v.add_argument("shortcut"); v.add_argument("season", type=int)
    sp.add_parser("register", help="đăng ký Debezium connector")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    if a.cmd == "register":
        from . import register_connector
        return register_connector.main()
    if a.cmd == "init":
        db.init_db(); print("OK: schema + publication football_pub đã sẵn sàng"); return 0

    conn = db.connect()
    client = OpenLigaClient()
    if a.cmd == "backfill":
        seasons = [int(x) for x in a.seasons.split(",")] if a.seasons else None
        only = set(a.only.split(",")) if a.only else None
        _, fails = sync.backfill(conn, client, seasons, only, not a.no_result_infos, a.force)
        return 1 if fails else 0
    if a.cmd == "sync":
        sync.run_forever(conn, client)
    if a.cmd == "once":
        sync.tick(conn, client)
    if a.cmd == "stats":
        with conn.cursor() as cur:
            for t in ["sports", "result_types", "leagues", "league_result_infos", "teams", "locations",
                      "groups", "matches", "match_results", "goals", "sync_state"]:
                cur.execute(f"SELECT count(*) FROM {t}")
                print(f"{t:<22}{cur.fetchone()[0]:>8}")
    if a.cmd == "verify":
        return _verify(conn, client, a.shortcut, a.season)
    return 0


def _verify(conn, client, shortcut, season):
    payload = client.season_matches(shortcut, season) or []
    api_m = len(payload)
    api_g = sum(len(m.get("goals") or []) for m in payload)
    with conn.cursor() as cur:
        cur.execute("""SELECT count(*), (SELECT count(*) FROM goals g JOIN matches m2 USING(match_id)
                                         JOIN leagues l2 ON l2.league_id=m2.league_id
                                         WHERE l2.shortcut=%s AND l2.season=%s)
                       FROM matches m JOIN leagues l ON l.league_id=m.league_id
                       WHERE l.shortcut=%s AND l.season=%s""", (shortcut, season, shortcut, season))
        db_m, db_g = cur.fetchone()
    ok = (api_m, api_g) == (db_m, db_g)
    print(f"matches API={api_m} DB={db_m} | goals API={api_g} DB={db_g} -> {'OK' if ok else 'LỆCH'}")
    table = client.table(shortcut, season) if shortcut == "bl1" else None
    if table:
        with conn.cursor() as cur:
            cur.execute("""SELECT t.team_id, t.points FROM v_league_table t JOIN leagues l USING(league_id)
                           WHERE l.shortcut=%s AND l.season=%s""", (shortcut, season))
            mine = dict(cur.fetchall())
        bad = [(r["teamName"], r["points"], mine.get(r["teamInfoId"])) for r in table
               if mine.get(r["teamInfoId"]) != r["points"]]
        print("bảng xếp hạng khớp getbltable" if not bad else f"bảng xếp hạng lệch: {bad}")
        ok = ok and not bad
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
