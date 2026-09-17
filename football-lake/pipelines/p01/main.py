"""Ingest Fantasy Premier League API vào MinIO bronze + silver."""
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))

import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, read_json_gz, today, summary
from lake.http import get

BASE = "https://fantasy.premierleague.com/api"
SRC = "fpl-api"
D = today()
SEASON = "2025-26"          # đổi theo mùa hiện tại


# ---------- BRONZE ----------
def ingest_bootstrap() -> dict:
    data = get(f"{BASE}/bootstrap-static/").json()
    put_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz",
                data, SRC, meta={"players": len(data["elements"])})
    return [e["id"] for e in data["elements"]]


def ingest_fixtures() -> list:
    data = get(f"{BASE}/fixtures/").json()
    put_json_gz(f"bronze/fpl/fixtures/ingest_date={D}/fixtures.json.gz",
                data, SRC, meta={"fixtures": len(data)})


def ingest_player_histories(player_ids, sleep=0.25):
    """Gọi 1 request/cầu thủ. ~700 request, mất khoảng 4-5 phút."""
    ok = 0
    for i, pid in enumerate(player_ids, 1):
        try:
            data = get(f"{BASE}/element-summary/{pid}/").json()
        except Exception as e:
            print(f"  ! player {pid} lỗi: {e}")
            continue
        put_json_gz(
            f"bronze/fpl/element_summary/ingest_date={D}/player_id={pid:04d}.json.gz",
            data, SRC, meta={"gw_rows": len(data.get("history", []))})
        ok += 1
        if i % 50 == 0:
            print(f"  ... {i}/{len(player_ids)}")
        time.sleep(sleep)      # lịch sự với server, tránh bị chặn IP
    print(f"[bronze] element_summary: {ok}/{len(player_ids)} cầu thủ")


def ingest_live_gw(gw: int):
    data = get(f"{BASE}/event/{gw}/live/").json()
    put_json_gz(f"bronze/fpl/event_live/ingest_date={D}/gw={gw:02d}.json.gz",
                data, SRC)


# ---------- SILVER ----------
def build_player_dim():
    bootstrap = read_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz")
    teams = {t["id"]: t["name"] for t in bootstrap["teams"]}
    pos = {p["id"]: p["singular_name_short"] for p in bootstrap["element_types"]}

    df = pd.DataFrame(bootstrap["elements"])[[
        "id", "first_name", "second_name", "web_name", "team",
        "element_type", "now_cost", "total_points", "minutes",
        "goals_scored", "assists", "clean_sheets", "expected_goals",
        "expected_assists", "selected_by_percent", "status",
    ]]
    df["team_name"] = df["team"].map(teams)
    df["position"] = df["element_type"].map(pos)
    df["price_m"] = df["now_cost"] / 10.0
    df["full_name"] = df["first_name"] + " " + df["second_name"]

    # ép kiểu số cho các cột API trả về dạng string
    for c in ["expected_goals", "expected_assists", "selected_by_percent"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.drop(columns=["now_cost", "element_type"])
    put_parquet(f"silver/players/fpl_player_dim/ingest_date={D}/part-0.parquet",
                df, SRC)
    return df


def build_player_gw_fact(player_ids=None):
    """Gộp history từng GW của mọi cầu thủ thành 1 fact table."""
    if not player_ids:
        bootstrap = read_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz")
        player_ids = [e["id"] for e in bootstrap["elements"]]
        
    rows = []
    for pid in player_ids:
        key = f"bronze/fpl/element_summary/ingest_date={D}/player_id={pid:04d}.json.gz"
        try:
            data = read_json_gz(key)
        except Exception:
            continue
        for h in data.get("history", []):
            rows.append(h)

    df = pd.DataFrame(rows)
    if df.empty:
        print("  ! chưa có history — mùa giải có thể chưa bắt đầu")
        return df

    keep = ["element", "fixture", "opponent_team", "round", "kickoff_time",
            "was_home", "total_points", "minutes", "goals_scored", "assists",
            "clean_sheets", "goals_conceded", "bonus", "bps", "influence",
            "creativity", "threat", "ict_index", "expected_goals",
            "expected_assists", "value", "selected", "transfers_in",
            "transfers_out"]
    df = df[[c for c in keep if c in df.columns]].rename(
        columns={"element": "player_id", "round": "gameweek"})
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], errors="coerce")
    for c in ["influence", "creativity", "threat", "ict_index",
              "expected_goals", "expected_assists"]:
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["price_m"] = df["value"] / 10.0

    put_parquet(f"silver/players/fpl_player_gw/season={SEASON}/part-0.parquet",
                df, SRC, meta={"gameweeks": df["gameweek"].nunique()})
    return df


def build_fixtures():
    bootstrap = read_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz")
    fixtures = read_json_gz(f"bronze/fpl/fixtures/ingest_date={D}/fixtures.json.gz")
    teams = {t["id"]: t["name"] for t in bootstrap["teams"]}
    df = pd.DataFrame(fixtures)[[
        "id", "event", "kickoff_time", "team_h", "team_a",
        "team_h_score", "team_a_score", "finished", "team_h_difficulty",
        "team_a_difficulty",
    ]].rename(columns={"id": "fixture_id", "event": "gameweek"})
    df["home_team"] = df["team_h"].map(teams)
    df["away_team"] = df["team_a"].map(teams)
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"], errors="coerce")
    put_parquet(f"silver/fixtures/fpl_fixtures/season={SEASON}/part-0.parquet",
                df, SRC)
    return df


if __name__ == "__main__":
    print("[1/5] bootstrap-static")
    pids = ingest_bootstrap()
    print(f"      {len(pids)} cầu thủ")

    print("[2/5] fixtures")
    ingest_fixtures()

    print("[3/5] element-summary (chậm, ~5 phút)")
    ingest_player_histories(pids)

    print("[4/5] live gameweek")
    bs = read_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz")
    current = next((e["id"] for e in bs["events"] if e["is_current"]), 1)
    ingest_live_gw(current)

    print("[5/5] silver")
    build_player_dim()
    build_player_gw_fact(pids)
    build_fixtures()

    summary("bronze/fpl/")
    summary("silver/players/")
