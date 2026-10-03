"""Chuyển payload JSON của OpenLigaDB -> các dict dòng cho từng bảng (dedupe theo PK)."""
import re
from datetime import datetime

_DT = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?$")


def parse_dt(s):
    """'2026-09-10T17:22:25.727' / '...Z' / '...T17:23:11.1' -> datetime (naive hoặc aware)."""
    if not s or not isinstance(s, str):
        return None
    m = _DT.match(s.strip())
    if not m:
        return None
    base, frac, tz = m.groups()
    frac = (frac or "0")[:6].ljust(6, "0")
    tz = "+00:00" if tz == "Z" else (tz or "")
    return datetime.fromisoformat(f"{base}.{frac}{tz}")


def _id(v):
    """ID hợp lệ (>0) hoặc None."""
    return v if isinstance(v, int) and v > 0 else None


def transform_matches(matches):
    out = {k: {} for k in ("leagues", "teams", "locations", "groups", "matches", "results", "goals")}
    for m in matches or []:
        mid = m["matchID"]
        lid = m["leagueId"]
        out["leagues"].setdefault(lid, {
            "league_id": lid, "shortcut": m.get("leagueShortcut") or "", "season": m.get("leagueSeason"),
            "name": m.get("leagueName") or "", "sport_id": None})

        t = {}
        for k in ("team1", "team2"):
            tm = m.get(k)
            t[k] = tm
            if tm and _id(tm.get("teamId")):
                out["teams"][tm["teamId"]] = {
                    "team_id": tm["teamId"], "team_name": tm.get("teamName") or "",
                    "short_name": tm.get("shortName"), "icon_url": tm.get("teamIconUrl")}

        loc = m.get("location")
        loc_id = _id(loc.get("locationID")) if loc else None
        if loc_id:
            out["locations"][loc_id] = {"location_id": loc_id, "city": loc.get("locationCity"),
                                        "stadium": loc.get("locationStadium")}

        g = m.get("group")
        gid = _id(g.get("groupID")) if g else None
        if gid:
            out["groups"][gid] = {"group_id": gid, "league_id": lid,
                                  "group_order": g.get("groupOrderID"), "group_name": g.get("groupName")}

        out["matches"][mid] = {
            "match_id": mid, "league_id": lid, "group_id": gid,
            "team1_id": _id(t["team1"].get("teamId")) if t["team1"] else None,
            "team2_id": _id(t["team2"].get("teamId")) if t["team2"] else None,
            # team1_group_name / team2_group_name: khai báo trong swagger nhưng API luôn trả null
            "location_id": loc_id,
            "match_time_utc": parse_dt(m.get("matchDateTimeUTC")),
            "match_time_local": parse_dt(m.get("matchDateTime")),
            "time_zone_id": m.get("timeZoneID"),
            "is_finished": bool(m.get("matchIsFinished")),
            "viewers": m.get("numberOfViewers"),
            "api_updated_at": parse_dt(m.get("lastUpdateDateTime")),
        }

        for r in m.get("matchResults") or []:
            out["results"][r["resultID"]] = {
                "result_id": r["resultID"], "match_id": mid, "result_name": r.get("resultName"),
                "result_order": r.get("resultOrderID"), "result_type_id": r.get("resultTypeID"),
                "result_type_kind": r.get("resultTypeKind"), "description": r.get("resultDescription"),
                "points_team1": r.get("pointsTeam1"), "points_team2": r.get("pointsTeam2")}

        for gl in m.get("goals") or []:
            out["goals"][gl["goalID"]] = {
                "goal_id": gl["goalID"], "match_id": mid, "minute": gl.get("matchMinute"),
                "scorer_id": gl.get("goalGetterID"), "scorer_name": gl.get("goalGetterName"),
                "scoring_team_id": gl.get("scoringTeamId"),
                "score_team1": gl.get("scoreTeam1"), "score_team2": gl.get("scoreTeam2"),
                "is_penalty": gl.get("isPenalty"), "is_own_goal": gl.get("isOwnGoal"),
                "is_overtime": gl.get("isOvertime"), "comment": gl.get("comment")}
    return out
