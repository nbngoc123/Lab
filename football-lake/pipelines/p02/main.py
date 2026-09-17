"""Ingest API-Football với quota budgeting + checkpoint."""
import json
import os
import time
import pandas as pd
from lake.minio_io import (put_json_gz, put_parquet, read_json_gz,
                           exists, today, summary, S3, BUCKET)
from lake.http import SESSION

BASE = "https://v3.football.api-sports.io"
SRC = "api-football"
D = today()
LEAGUE, SEASON = 39, 2024 # 2024/2025 season in API-Football is usually denoted by starting year 2024
DAILY_BUDGET = 20         # Giới hạn 20 request/ngày để test nhanh (tránh dùng hết 100 quota)
CHECKPOINT_KEY = "_meta/api_football/checkpoint.json"

# Thay thế os.getenv nếu chưa có, hoặc để nguyên
API_KEY = os.getenv("API_FOOTBALL_KEY", "your_api_key_here") 
HEADERS = {"x-apisports-key": API_KEY}
_used = 0
_remaining = None


def call(path: str, params: dict) -> dict:
    """Gọi API, đếm quota, dừng hẳn nếu chạm budget."""
    global _used, _remaining
    if _used >= DAILY_BUDGET:
        raise RuntimeError(f"Đã dùng hết budget {_used}/{DAILY_BUDGET} request hôm nay")

    r = SESSION.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=30)
    _used += 1
    _remaining = r.headers.get("x-ratelimit-requests-remaining")
    r.raise_for_status()
    body = r.json()

    if body.get("errors"):
        raise RuntimeError(f"API trả lỗi: {body['errors']}")
    print(f"    [quota] dùng {_used}/{DAILY_BUDGET} | server còn {_remaining}")
    time.sleep(1.0)          # free plan giới hạn ~10 req/phút
    return body


# ---------- checkpoint ----------
def load_checkpoint() -> set:
    if not exists(CHECKPOINT_KEY):
        return set()
    raw = S3.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)["Body"].read()
    return set(json.loads(raw)["done_fixture_ids"])


def save_checkpoint(done: set):
    S3.put_object(
        Bucket=BUCKET, Key=CHECKPOINT_KEY,
        Body=json.dumps({"done_fixture_ids": sorted(list(done)),
                         "updated_at": D}).encode(),
        ContentType="application/json")
    print(f"  ✓ checkpoint: {len(done)} fixture đã ingest")


# ---------- BRONZE ----------
def ingest_teams():
    key = f"bronze/api_football/teams/season={SEASON}/teams.json.gz"
    if exists(key):
        print("  · teams đã có, bỏ qua (tiết kiệm quota)")
        return read_json_gz(key)
    body = call("/teams", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    return body


def ingest_standings():
    body = call("/standings", {"league": LEAGUE, "season": SEASON})
    put_json_gz(
        f"bronze/api_football/standings/ingest_date={D}/standings.json.gz",
        body, SRC)
    return body


def ingest_fixtures():
    body = call("/fixtures", {"league": LEAGUE, "season": SEASON})
    put_json_gz(
        f"bronze/api_football/fixtures/season={SEASON}/ingest_date={D}/fixtures.json.gz",
        body, SRC, meta={"count": body["results"]})
    return body


def ingest_fixture_details(fixtures: dict, done: set):
    """Chỉ lấy trận đã đá xong và chưa có trong checkpoint."""
    if not fixtures.get("response"):
        print("  ! Không có dữ liệu fixtures")
        return done
        
    finished = [f for f in fixtures["response"]
                if f["fixture"]["status"]["short"] in ["FT", "AET", "PEN"]
                and f["fixture"]["id"] not in done]
    finished.sort(key=lambda f: f["fixture"]["date"], reverse=True)  # mới nhất trước

    prefix_base = f"bronze/api_football/fixture_detail/season={SEASON}"
    for f in finished:
        fid = f["fixture"]["id"]
        if _used + 3 > DAILY_BUDGET:
            print("  · hết budget, dừng — chạy lại ngày mai để tiếp tục")
            break
        try:
            for name, path in [("events", "/fixtures/events"),
                               ("lineups", "/fixtures/lineups"),
                               ("statistics", "/fixtures/statistics")]:
                body = call(path, {"fixture": fid})
                put_json_gz(f"{prefix_base}/fixture_id={fid}/{name}.json.gz",
                            body, SRC, meta={"fixture_id": fid})
            done.add(fid)
            print(f"  ✓ fixture {fid} xong "
                  f"({f['teams']['home']['name']} vs {f['teams']['away']['name']})")
        except RuntimeError as e:
            print(f"  ! dừng: {e}")
            break
    return done


# ---------- SILVER ----------
def build_events(done: set):
    rows = []
    for fid in done:
        key = (f"bronze/api_football/fixture_detail/season={SEASON}"
               f"/fixture_id={fid}/events.json.gz")
        if not exists(key):
            continue
        events = read_json_gz(key).get("response", [])
        for e in events:
            rows.append({
                "fixture_id": fid,
                "minute": e["time"]["elapsed"],
                "minute_extra": e["time"].get("extra"),
                "team": e["team"]["name"],
                "player": (e.get("player") or {}).get("name"),
                "assist": (e.get("assist") or {}).get("name"),
                "type": e["type"],
                "detail": e["detail"],
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(f"silver/matches/af_match_events/season={SEASON}/part-0.parquet",
                    df, SRC, meta={"fixtures": df.fixture_id.nunique()})
    return df


def build_stats(done: set):
    """statistics trả về list {type, value} -> pivot thành cột."""
    rows = []
    for fid in done:
        key = (f"bronze/api_football/fixture_detail/season={SEASON}"
               f"/fixture_id={fid}/statistics.json.gz")
        if not exists(key):
            continue
        stats = read_json_gz(key).get("response", [])
        for side in stats:
            rec = {"fixture_id": fid, "team": side["team"]["name"]}
            for s in side["statistics"]:
                col = s["type"].lower().replace(" ", "_").replace("%", "pct")
                val = s["value"]
                if isinstance(val, str) and val.endswith("%"):
                    val = float(val.rstrip("%"))
                rec[col] = val
            rows.append(rec)
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(f"silver/matches/af_match_stats/season={SEASON}/part-0.parquet",
                    df, SRC)
    return df


def build_standings(body: dict):
    rows = []
    responses = body.get("response", [])
    if not responses:
        return pd.DataFrame()
        
    for table in responses[0]["league"]["standings"]:
        for t in table:
            rows.append({
                "rank": t["rank"], "team": t["team"]["name"],
                "points": t["points"], "played": t["all"]["played"],
                "win": t["all"]["win"], "draw": t["all"]["draw"],
                "lose": t["all"]["lose"],
                "goals_for": t["all"]["goals"]["for"],
                "goals_against": t["all"]["goals"]["against"],
                "goal_diff": t["goalsDiff"], "form": t["form"],
            })
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(f"silver/standings/af_standings/ingest_date={D}/part-0.parquet",
                    df, SRC)
    return df
