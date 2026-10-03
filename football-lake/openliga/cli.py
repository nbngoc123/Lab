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
    cov = sp.add_parser("coverage", help="kiểm tra độ phủ: league rỗng, shortcut trùng, top scorer")
    cov.add_argument("--check-scorer", action="store_true", help="đối chiếu top scorer với getgoalgetters (thêm requests)")
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
    if a.cmd == "coverage":
        return _coverage(conn, client, getattr(a, 'check_scorer', False))
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


def _coverage(conn, client, check_scorer=False):
    """Báo cáo độ phủ: league rỗng, shortcut trùng, top scorer crosscheck."""
    issues = []
    with conn.cursor() as cur:
        # 1. League có trong DB nhưng không có trận nào
        cur.execute("""
            SELECT l.shortcut, l.season, l.name, l.league_id
            FROM leagues l
            WHERE NOT EXISTS (SELECT 1 FROM matches m WHERE m.league_id = l.league_id)
            ORDER BY l.season, l.shortcut
        """)
        empty = cur.fetchall()
        if empty:
            print(f"\n[CẢNH BÁO] {len(empty)} league có trong DB nhưng 0 trận (API trả rỗng hoặc chưa công bố lịch):")
            for sc, ssn, name, lid in empty:
                print(f"  {sc:<20} mùa={ssn}  '{name}'  (league_id={lid})")
        else:
            print("[OK] Mọi league trong DB đều có ít nhất 1 trận.")

        # 2. Shortcut trùng trong cùng 1 mùa
        cur.execute("""
            SELECT shortcut, season, count(*) AS cnt, array_agg(league_id) AS ids
            FROM leagues
            GROUP BY shortcut, season
            HAVING count(*) > 1
        """)
        dups = cur.fetchall()
        if dups:
            issues.append("shortcut trùng")
            print(f"\n[LỖI] {len(dups)} shortcut trùng trong cùng mùa (getmatchdata chỉ trả 1 league):")
            for sc, ssn, cnt, ids in dups:
                print(f"  shortcut='{sc}' mùa={ssn} -> {cnt} leagues: {ids}")
        else:
            print("[OK] Không có shortcut trùng trong cùng mùa.")

        # 3. Thống kê tổng quan
        cur.execute("""
            SELECT
                count(DISTINCT l.league_id) AS total_leagues,
                count(DISTINCT m.match_id)  AS total_matches,
                count(DISTINCT g.goal_id)   AS total_goals,
                count(DISTINCT t.team_id)   AS total_teams,
                count(DISTINCT l.season)    AS seasons
            FROM leagues l
            LEFT JOIN matches m USING(league_id)
            LEFT JOIN goals   g USING(match_id)
            LEFT JOIN teams   t ON t.team_id = m.team1_id
        """)
        row = cur.fetchone()
        print(f"\n[TỔNG QUAN] leagues={row[0]} | matches={row[1]} | goals={row[2]} | teams={row[3]} | seasons={row[4]}")

        # 4. Top 5 giải nhiều trận nhất
        cur.execute("""
            SELECT l.shortcut, l.season, l.name, count(m.match_id) AS n
            FROM leagues l JOIN matches m USING(league_id)
            GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10
        """)
        print("\n[TOP 10 GIẢI NHIỀU TRẬN NHẤT]:")
        for sc, ssn, name, n in cur.fetchall():
            print(f"  {sc:<12} {ssn}  {n:>5} trận  '{name}'")

    # 5. Crosscheck top scorer với API (tuỳ chọn)
    if check_scorer:
        print("\n[TOP SCORER CROSSCHECK] Đang kiểm tra bl1/2026...")
        from .client import NotFound
        try:
            api_scorers = client.goal_getters("bl1", 2026) or []
        except NotFound:
            api_scorers = []
        with conn.cursor() as cur:
            cur.execute("""
                SELECT g.scorer_id, g.scorer_name,
                       count(*) FILTER (WHERE NOT g.is_own_goal) AS goals
                FROM goals g
                JOIN matches m USING(match_id)
                JOIN leagues l USING(league_id)
                WHERE l.shortcut = 'bl1' AND l.season = 2026
                GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10
            """)
            db_scorers = cur.fetchall()
        # Map API: {goalGetterId: {name, goals}}
        api_map = {s["goalGetterID"]: s for s in api_scorers}
        print(f"  {'ID':<10} {'Tên':<25} DB_goals  API_goals  Match")
        ok_scorer = True
        for sid, sname, db_g in db_scorers:
            api_s = api_map.get(sid)
            api_g = api_s["goalCount"] if api_s else "?"
            match = "OK" if str(db_g) == str(api_g) else "LỆCH"
            if match != "OK":
                ok_scorer = False
                issues.append(f"top scorer lệch id={sid}")
            print(f"  {sid:<10} {sname:<25} {db_g:<9} {api_g:<10} {match}")
        if ok_scorer:
            print("  [OK] Top scorer khớp API.")

    print("\n[KẾT QUẢ]:", "PASS - không có vấn đề nghiêm trọng" if not issues else f"CÓ {len(issues)} VẤN ĐỀ: " + ", ".join(issues))
    return 1 if issues else 0


if __name__ == "__main__":
    sys.exit(main())
