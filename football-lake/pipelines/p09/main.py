"""Ingest football-data.org API v4: dựng lại mô hình quan hệ vào lake."""
import os
import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, today, summary
from lake.http import SESSION

BASE = "https://api.football-data.org/v4"
SRC = "football-data.org"
D = today()
TOKEN = os.getenv("FOOTBALL_DATA_TOKEN")

# Nếu không có token thì dùng chuỗi rỗng để tránh lỗi TypeError ở headers
if not TOKEN:
    print("WARNING: FOOTBALL_DATA_TOKEN is not set in environment.")
    TOKEN = ""
    
HEADERS = {"X-Auth-Token": TOKEN}

# 12 giải free tier hay dùng nhất; PL là trọng tâm của bộ 8 nguồn kia
COMPETITIONS = ["PL", "PD", "BL1", "SA", "FL1", "CL"]
FOCUS = "PL"          # giải chính để build fact chi tiết


def call(path: str, params: dict | None = None) -> dict:
    """Free tier: 10 request/phút -> luôn chờ 6.5s giữa các lần gọi."""
    r = SESSION.get(f"{BASE}{path}", headers=HEADERS, params=params, timeout=30)
    if r.status_code == 429:
        wait = int(r.headers.get("Retry-After", 60))
        print(f"    ! 429 rate limit, chờ {wait}s")
        time.sleep(wait)
        return call(path, params)
    r.raise_for_status()
    time.sleep(6.5)
    return r.json()


# ---------- BRONZE ----------
def ingest_competitions() -> dict:
    body = call("/competitions")
    put_json_gz(f"bronze/football_data_org/competitions/ingest_date={D}/competitions.json.gz",
                body, SRC, meta={"count": body["count"]})
    return body


def ingest_teams(code: str) -> dict:
    body = call(f"/competitions/{code}/teams")
    put_json_gz(
        f"bronze/football_data_org/teams/competition={code}/ingest_date={D}/teams.json.gz",
        body, SRC, meta={"competition": code, "teams": body["count"]})
    return body


def ingest_matches(code: str) -> dict:
    body = call(f"/competitions/{code}/matches")
    season_start = body["filters"].get("season", "unknown")
    put_json_gz(
        f"bronze/football_data_org/matches/competition={code}"
        f"/season={season_start}/ingest_date={D}/matches.json.gz",
        body, SRC, meta={"competition": code, "matches": len(body["matches"])})
    return body


def ingest_standings(code: str) -> dict:
    body = call(f"/competitions/{code}/standings")
    put_json_gz(
        f"bronze/football_data_org/standings/competition={code}"
        f"/ingest_date={D}/standings.json.gz",
        body, SRC, meta={"competition": code})
    return body


# ---------- SILVER: tái dựng bảng quan hệ ----------
def build_competitions_dim(body: dict):
    rows = [{
        "competition_id": c["id"], "code": c.get("code"), "name": c["name"],
        "area": c["area"]["name"], "type": c.get("type"),
        "current_season_id": (c.get("currentSeason") or {}).get("id"),
        "current_season_start": (c.get("currentSeason") or {}).get("startDate"),
        "current_matchday": (c.get("currentSeason") or {}).get("currentMatchday"),
    } for c in body["competitions"]]
    df = pd.DataFrame(rows)
    put_parquet(f"silver/dim/fdo_competitions/ingest_date={D}/part-0.parquet",
                df, SRC)
    return df


def build_teams_dim(code: str, body: dict):
    rows = [{
        "team_id": t["id"], "name": t["name"], "short_name": t["shortName"],
        "tla": t.get("tla"), "founded": t.get("founded"),
        "venue": t.get("venue"), "club_colors": t.get("clubColors"),
        "coach_name": (t.get("coach") or {}).get("name"),
        "coach_nationality": (t.get("coach") or {}).get("nationality"),
        "squad_size": len(t.get("squad", [])),
        "competition": code,
    } for t in body["teams"]]
    df = pd.DataFrame(rows)
    put_parquet(
        f"silver/dim/fdo_teams/competition={code}/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


def build_players_dim(code: str, body: dict):
    """Squad nằm lồng trong mỗi team -> tách thành bảng riêng, FK = team_id."""
    rows = []
    for t in body["teams"]:
        for p in t.get("squad", []):
            rows.append({
                "player_id": p["id"], "name": p["name"],
                "position": p.get("position"), "date_of_birth": p.get("dateOfBirth"),
                "nationality": p.get("nationality"),
                "team_id": t["id"], "team_name": t["name"],
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["date_of_birth"] = pd.to_datetime(df["date_of_birth"], errors="coerce")
        put_parquet(
            f"silver/dim/fdo_players/competition={code}/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"players": len(df)})
    return df


def build_matches_fact(code: str, body: dict):
    rows = []
    for m in body["matches"]:
        score = m.get("score", {})
        full = score.get("fullTime", {})
        rows.append({
            "match_id": m["id"], "competition": code,
            "season_id": m["season"]["id"],
            "matchday": m.get("matchday"), "stage": m.get("stage"),
            "utc_date": m["utcDate"], "status": m["status"],
            "home_team_id": m["homeTeam"]["id"], "home_team": m["homeTeam"]["name"],
            "away_team_id": m["awayTeam"]["id"], "away_team": m["awayTeam"]["name"],
            "home_goals": full.get("home"), "away_goals": full.get("away"),
            "winner": score.get("winner"),
            "referee": (m.get("referees") or [{}])[0].get("name")
                       if m.get("referees") else None,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["utc_date"] = pd.to_datetime(df["utc_date"])
        season = df["season_id"].iloc[0] if not df.empty else "unknown"
        put_parquet(
            f"silver/matches/fdo_matches/competition={code}/season={season}/part-0.parquet",
            df, SRC, meta={"matches": len(df)})
    return df


def build_standings_fact(code: str, body: dict):
    """Chỉ lấy bảng TOTAL (bỏ HOME/AWAY breakdown để đơn giản)."""
    rows = []
    for table in body["standings"]:
        if table["type"] != "TOTAL":
            continue
        for t in table["table"]:
            rows.append({
                "competition": code, "ingest_date": D,
                "rank": t["position"], "team_id": t["team"]["id"],
                "team": t["team"]["name"], "played": t["playedGames"],
                "won": t["won"], "draw": t["draw"], "lost": t["lost"],
                "points": t["points"], "goals_for": t["goalsFor"],
                "goals_against": t["goalsAgainst"], "goal_diff": t["goalDifference"],
                "form": t.get("form"),
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(
            f"silver/standings/fdo_standings/competition={code}/ingest_date={D}/part-0.parquet",
            df, SRC)
    return df

def run_pipeline():
    print("[1/5] competitions")
    try:
        comps = ingest_competitions()
        build_competitions_dim(comps)
    except Exception as e:
        print(f"Error fetching competitions: {e}")
        return

    for code in COMPETITIONS:
        print(f"\n[2/5] teams — {code}")
        try:
            teams = ingest_teams(code)
            build_teams_dim(code, teams)
            build_players_dim(code, teams)
        except Exception as e:
            print(f"Error fetching teams for {code}: {e}")

        print(f"[3/5] matches — {code}")
        try:
            matches = ingest_matches(code)
            build_matches_fact(code, matches)
        except Exception as e:
            print(f"Error fetching matches for {code}: {e}")

        print(f"[4/5] standings — {code}")
        try:
            standings = ingest_standings(code)
            build_standings_fact(code, standings)
        except Exception as e:
            print(f"Error fetching standings for {code}: {e}")

    print("\n[5/5] tổng kết")
    summary("bronze/football_data_org/")
    summary("silver/dim/fdo_teams/")
    summary("silver/matches/fdo_matches/")


if __name__ == "__main__":
    run_pipeline()
