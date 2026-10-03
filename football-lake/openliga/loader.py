"""Ghi dữ liệu vào Postgres. Mỗi payload (một giải/mùa hoặc một spieltag) = MỘT transaction
(match + results + goals commit cùng lúc) để event Debezium có thứ tự nhất quán."""
import logging

from . import config, db
from .transform import transform_matches

log = logging.getLogger("openliga.loader")

COLS = {
    "teams":     ["team_id", "team_name", "short_name", "icon_url"],
    "locations": ["location_id", "city", "stadium"],
    "groups":    ["group_id", "league_id", "group_order", "group_name"],
    "matches":   ["match_id", "league_id", "group_id", "team1_id", "team2_id", "team1_group_name",
                  "team2_group_name", "location_id", "match_time_utc", "match_time_local",
                  "time_zone_id", "is_finished", "viewers", "api_updated_at"],
    "match_results": ["result_id", "match_id", "result_name", "result_order", "result_type_id",
                      "result_type_kind", "description", "points_team1", "points_team2"],
    "goals":     ["goal_id", "match_id", "minute", "scorer_id", "scorer_name", "scoring_team_id",
                  "score_team1", "score_team2", "is_penalty", "is_own_goal", "is_overtime", "comment"],
}


def load_matches(conn, matches, stats, full_season=False, endpoint=None):
    """Upsert một payload match. Gọi commit ở ngoài.
    full_season=True: payload là TOÀN BỘ giải/mùa nên match biến mất khỏi payload sẽ bị xoá."""
    b = transform_matches(matches)
    with conn.cursor() as cur:
        if endpoint:
            db.log_raw(cur, endpoint, matches)

        # league "stub" chỉ để thoả FK khi league chưa có; không ghi đè dữ liệu league thật
        for lg in b["leagues"].values():
            cur.execute("""INSERT INTO leagues (league_id, shortcut, season, name)
                           VALUES (%s,%s,%s,%s) ON CONFLICT (league_id) DO NOTHING""",
                        (lg["league_id"], lg["shortcut"], lg["season"], lg["name"]))
            if cur.rowcount:
                stats.add("leagues", ins=cur.rowcount)

        up = lambda t, key, pk, nc=(): db.upsert(cur, stats, t, list(b[key].values()), pk, COLS[t], nc)
        up("teams", "teams", "team_id")
        up("locations", "locations", "location_id")
        up("groups", "groups", "group_id")
        up("matches", "matches", "match_id", nc=("api_updated_at",))
        up("match_results", "results", "result_id")
        up("goals", "goals", "goal_id")

        # DELETE: kết quả / bàn thắng không còn trong payload (VAR huỷ, sửa dữ liệu...)
        match_ids = list(b["matches"])
        db.delete_orphans(cur, stats, "goals", "goal_id", "match_id", match_ids, list(b["goals"]))
        db.delete_orphans(cur, stats, "match_results", "result_id", "match_id", match_ids, list(b["results"]))

        # DELETE trận biến mất: chỉ khi payload là cả giải/mùa và KHÔNG rỗng (tránh xoá nhầm khi API lỗi)
        if full_season and match_ids:
            cur.execute("DELETE FROM matches WHERE league_id = ANY(%s::int[]) AND match_id <> ALL(%s::int[])",
                        (list(b["leagues"]), match_ids))
            stats.add("matches", dele=cur.rowcount)
    return b


# --- dữ liệu tham chiếu -------------------------------------------------------
def load_reference(conn, client, seasons, stats):
    sports = client.sports() or []
    rtypes = client.result_types() or []
    leagues_by_season = {s: (client.leagues(s) or []) for s in seasons}

    sport_rows = {x["sportId"]: {"sport_id": x["sportId"], "sport_name": x.get("sportName") or ""} for x in sports}
    league_rows = {}
    for s, lst in leagues_by_season.items():
        for lg in lst:
            if not lg.get("leagueShortcut"):
                log.warning("Bỏ qua league không có shortcut: %s", lg.get("leagueId"))
                continue
            sp = lg.get("sport") or {}
            if sp.get("sportId") and sp["sportId"] not in sport_rows:
                sport_rows[sp["sportId"]] = {"sport_id": sp["sportId"], "sport_name": sp.get("sportName") or ""}
            league_rows[lg["leagueId"]] = {
                "league_id": lg["leagueId"], "shortcut": lg["leagueShortcut"],
                "season": int(lg.get("leagueSeason") or s), "name": lg.get("leagueName") or "",
                "sport_id": sp.get("sportId")}
    rt_rows = [{"result_type_id": x["id"], "name": x.get("name"), "kind": x.get("kind")} for x in rtypes]

    with conn.cursor() as cur:
        db.upsert(cur, stats, "sports", list(sport_rows.values()), "sport_id", ["sport_id", "sport_name"])
        db.upsert(cur, stats, "result_types", rt_rows, "result_type_id", ["result_type_id", "name", "kind"])
        db.upsert(cur, stats, "leagues", list(league_rows.values()), "league_id",
                  ["league_id", "shortcut", "season", "name", "sport_id"])
    conn.commit()
    return league_rows


def load_result_infos(conn, client, league_id, stats):
    payload = client.result_infos(league_id)
    items = payload if isinstance(payload, list) else ([payload] if payload else [])
    rows = []
    for x in items:
        g = x.get("globalResultInfo") or {}
        rows.append({"info_id": x["id"], "league_id": league_id, "name": x.get("name"),
                     "description": x.get("description"), "order_id": x.get("orderId"),
                     "result_type_id": g.get("id"), "result_type_kind": g.get("kind")})
    with conn.cursor() as cur:
        db.upsert(cur, stats, "league_result_infos", rows, "info_id",
                  ["info_id", "league_id", "name", "description", "order_id", "result_type_id", "result_type_kind"])
        db.delete_orphans(cur, stats, "league_result_infos", "info_id", "league_id", [league_id],
                          [r["info_id"] for r in rows])
    conn.commit()


# --- backfill một giải-mùa ----------------------------------------------------
def sync_league_season(conn, client, shortcut, season, stats):
    """Lấy TOÀN BỘ trận của (shortcut, season) -> 1 request, 1 transaction."""
    ep = f"/getmatchdata/{shortcut}/{season}"
    payload = client.season_matches(shortcut, season) or []
    try:
        load_matches(conn, payload, stats, full_season=True, endpoint=ep)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    db.set_state(conn, shortcut, season, -1)
    return len(payload)


def sync_group(conn, client, shortcut, season, group_order, last_change_raw, stats):
    ep = f"/getmatchdata/{shortcut}/{season}/{group_order}"
    payload = client.group_matches(shortcut, season, group_order) or []
    try:
        load_matches(conn, payload, stats, full_season=False, endpoint=ep)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    db.set_state(conn, shortcut, season, group_order, raw=last_change_raw)
    return len(payload)
