"""
p24: API-Football (Full Pipeline) - Phiên bản nâng cấp
Lấy toàn diện dữ liệu EPL từ v3.football.api-sports.io:
  - Teams & Venues (bộ nhớ cache, tiết kiệm quota)
  - Standings (bảng xếp hạng)
  - Fixtures (lịch + kết quả 380 trận)
  - Fixture Details: Events, Lineups, Statistics (theo checkpoint)
  - Top Scorers, Top Assists
Yêu cầu: API_FOOTBALL_KEY trong .env (Free: 100 req/ngày)
"""
import json
import os
import time

import pandas as pd

from lake.minio_io import (
    put_json_gz, put_parquet, read_json_gz,
    exists, today, summary, S3, BUCKET
)
from lake.http import SESSION

# ─────────────────── config ───────────────────────────────────────────────
BASE       = "https://v3.football.api-sports.io"
SRC        = "api-football"
D          = today()
LEAGUE     = 39          # Premier League
SEASON     = 2024        # 2024/2025
DAILY_BUDGET = 80        # Giới hạn 80/100 req/ngày (giữ lại 20 dự phòng)
CHECKPOINT_KEY = "_meta/api_football_p24/checkpoint.json"

API_KEY  = os.getenv("API_FOOTBALL_KEY", "")
HEADERS  = {"x-apisports-key": API_KEY}
_used    = 0
_remaining = None


# ─────────────────── quota-aware caller ──────────────────────────────────
def call(path: str, params: dict) -> dict:
    """Gọi API, đếm quota, tự dừng nếu chạm budget."""
    global _used, _remaining
    if _used >= DAILY_BUDGET:
        raise RuntimeError(f"Đã dùng hết budget {_used}/{DAILY_BUDGET} req hôm nay")

    r = SESSION.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=30)
    _used += 1
    _remaining = r.headers.get("x-ratelimit-requests-remaining")
    r.raise_for_status()
    body = r.json()

    if body.get("errors") and body["errors"] != []:
        raise RuntimeError(f"API lỗi: {body['errors']}")
    print(f"    [quota] {_used}/{DAILY_BUDGET} dùng | còn {_remaining} trên server")
    time.sleep(6.2)  # Free plan: max ~10 req/phút
    return body


# ─────────────────── checkpoint ──────────────────────────────────────────
def load_checkpoint() -> set:
    if not exists(CHECKPOINT_KEY):
        return set()
    raw = S3.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)["Body"].read()
    return set(json.loads(raw)["done_fixture_ids"])


def save_checkpoint(done: set):
    S3.put_object(
        Bucket=BUCKET, Key=CHECKPOINT_KEY,
        Body=json.dumps({
            "done_fixture_ids": sorted(list(done)),
            "updated_at": D
        }).encode(),
        ContentType="application/json"
    )
    print(f"  ✓ checkpoint: {len(done)} fixture đã ingest")


# ─────────────────── BRONZE ──────────────────────────────────────────────
def ingest_teams():
    """Teams + Venues (cache vĩnh viễn, chỉ gọi 1 lần)."""
    key = f"bronze/api_football/teams/season={SEASON}/teams.json.gz"
    if exists(key):
        print("  · teams đã cache, bỏ qua (tiết kiệm quota)")
        return read_json_gz(key)
    body = call("/teams", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} teams + venues -> bronze")
    return body


def ingest_standings():
    """Bảng xếp hạng — cập nhật mỗi ngày."""
    body = call("/standings", {"league": LEAGUE, "season": SEASON})
    put_json_gz(
        f"bronze/api_football/standings/ingest_date={D}/standings.json.gz",
        body, SRC
    )
    print("  ✓ standings -> bronze")
    return body


def ingest_fixtures():
    """Toàn bộ 380 trận (có kết quả + chưa đá)."""
    body = call("/fixtures", {"league": LEAGUE, "season": SEASON})
    put_json_gz(
        f"bronze/api_football/fixtures/season={SEASON}/ingest_date={D}/fixtures.json.gz",
        body, SRC, meta={"count": body["results"]}
    )
    print(f"  ✓ {body['results']} fixtures -> bronze")
    return body


def ingest_top_scorers():
    """Top ghi bàn mùa giải."""
    key = f"bronze/api_football/top_scorers/season={SEASON}/ingest_date={D}/scorers.json.gz"
    body = call("/players/topscorers", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} top scorers -> bronze")
    return body


def ingest_fixture_details(fixtures: dict, done: set) -> set:
    """
    Lấy Events + Lineups + Statistics cho mỗi trận đã đá xong (FT/AET/PEN).
    Dùng checkpoint để tiếp tục nếu bị ngắt giữa chừng.
    """
    if not fixtures.get("response"):
        print("  ! Không có fixtures")
        return done

    finished = [
        f for f in fixtures["response"]
        if f["fixture"]["status"]["short"] in ("FT", "AET", "PEN")
        and f["fixture"]["id"] not in done
    ]
    # Ưu tiên trận mới nhất trước
    finished.sort(key=lambda f: f["fixture"]["date"], reverse=True)

    print(f"  · {len(finished)} trận cần lấy chi tiết (đã có {len(done)} checkpoint)")

    prefix = f"bronze/api_football/fixture_detail/season={SEASON}"
    for f in finished:
        fid = f["fixture"]["id"]
        home = f["teams"]["home"]["name"]
        away = f["teams"]["away"]["name"]

        # Mỗi trận cần 3 request (events + lineups + stats)
        if _used + 3 > DAILY_BUDGET:
            print("  · hết budget hôm nay — checkpoint đã lưu, ngày mai tiếp tục")
            break
        try:
            for endpoint_name, api_path in [
                ("events",     "/fixtures/events"),
                ("lineups",    "/fixtures/lineups"),
                ("statistics", "/fixtures/statistics"),
            ]:
                body = call(api_path, {"fixture": fid})
                put_json_gz(
                    f"{prefix}/fixture_id={fid}/{endpoint_name}.json.gz",
                    body, SRC, meta={"fixture_id": fid}
                )
            done.add(fid)
            print(f"  ✓ {home} vs {away} (id={fid})")
        except RuntimeError as e:
            print(f"  ! dừng: {e}")
            break

    return done


# ─────────────────── SILVER ──────────────────────────────────────────────
def build_silver_fixtures(fixtures: dict):
    """Kết quả từng trận -> silver."""
    rows = []
    for item in fixtures.get("response", []):
        fixt  = item["fixture"]
        teams = item["teams"]
        goals = item["goals"]
        score = item["score"]
        lg    = item["league"]
        rows.append({
            "fixture_id":    fixt["id"],
            "date":          fixt["date"],
            "status":        fixt["status"]["long"],
            "round":         lg["round"],
            "home_team_id":  teams["home"]["id"],
            "home_team":     teams["home"]["name"],
            "away_team_id":  teams["away"]["id"],
            "away_team":     teams["away"]["name"],
            "home_goals":    goals["home"],
            "away_goals":    goals["away"],
            "ht_home":       score["halftime"]["home"],
            "ht_away":       score["halftime"]["away"],
            "referee":       fixt.get("referee"),
            "venue":         (fixt.get("venue") or {}).get("name"),
        })
    df = pd.DataFrame(rows)
    df["ingest_date"] = D
    put_parquet(
        f"silver/matches/af_fixtures/season={SEASON}/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"rows": len(df)}
    )
    print(f"  ✓ {len(df)} fixtures -> silver")


def build_silver_standings(body: dict):
    """Bảng xếp hạng -> silver."""
    rows = []
    responses = body.get("response", [])
    if not responses:
        return
    for table in responses[0]["league"]["standings"]:
        for t in table:
            rows.append({
                "rank":          t["rank"],
                "team":          t["team"]["name"],
                "team_id":       t["team"]["id"],
                "points":        t["points"],
                "played":        t["all"]["played"],
                "win":           t["all"]["win"],
                "draw":          t["all"]["draw"],
                "lose":          t["all"]["lose"],
                "goals_for":     t["all"]["goals"]["for"],
                "goals_against": t["all"]["goals"]["against"],
                "goal_diff":     t["goalsDiff"],
                "form":          t["form"],
            })
    df = pd.DataFrame(rows)
    df["ingest_date"] = D
    put_parquet(
        f"silver/standings/af_standings/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"rows": len(df)}
    )
    print(f"  ✓ {len(df)} standings rows -> silver")


def build_silver_events(done: set):
    """Events (bàn thắng, thẻ phạt, thay người) -> silver."""
    rows = []
    for fid in done:
        key = f"bronze/api_football/fixture_detail/season={SEASON}/fixture_id={fid}/events.json.gz"
        if not exists(key):
            continue
        for e in read_json_gz(key).get("response", []):
            rows.append({
                "fixture_id":    fid,
                "minute":        e["time"]["elapsed"],
                "minute_extra":  e["time"].get("extra"),
                "team":          e["team"]["name"],
                "team_id":       e["team"]["id"],
                "player":        (e.get("player") or {}).get("name"),
                "player_id":     (e.get("player") or {}).get("id"),
                "assist":        (e.get("assist") or {}).get("name"),
                "type":          e["type"],
                "detail":        e["detail"],
                "comments":      e.get("comments"),
            })
    if rows:
        df = pd.DataFrame(rows)
        put_parquet(
            f"silver/matches/af_match_events/season={SEASON}/part-0.parquet",
            df, SRC, meta={"fixtures": df["fixture_id"].nunique()}
        )
        print(f"  ✓ {len(df)} events ({df['fixture_id'].nunique()} trận) -> silver")


def build_silver_lineups(done: set):
    """Đội hình ra sân (11 người + dự bị) -> silver."""
    rows = []
    for fid in done:
        key = f"bronze/api_football/fixture_detail/season={SEASON}/fixture_id={fid}/lineups.json.gz"
        if not exists(key):
            continue
        for side in read_json_gz(key).get("response", []):
            team_name = side["team"]["name"]
            team_id   = side["team"]["id"]
            formation = side.get("formation")
            for role, players in [("start", side.get("startXI", [])),
                                   ("sub",   side.get("substitutes", []))]:
                for item in players:
                    p = item.get("player", {})
                    rows.append({
                        "fixture_id": fid,
                        "team_id":    team_id,
                        "team":       team_name,
                        "formation":  formation,
                        "role":       role,
                        "player_id":  p.get("id"),
                        "player":     p.get("name"),
                        "number":     p.get("number"),
                        "position":   p.get("pos"),
                        "grid":       p.get("grid"),
                    })
    if rows:
        df = pd.DataFrame(rows)
        put_parquet(
            f"silver/matches/af_lineups/season={SEASON}/part-0.parquet",
            df, SRC, meta={"fixtures": df["fixture_id"].nunique()}
        )
        print(f"  ✓ {len(df)} lineup rows ({df['fixture_id'].nunique()} trận) -> silver")


def build_silver_stats(done: set):
    """Thống kê đội bóng (shots, passes, ball possession...) -> silver."""
    rows = []
    for fid in done:
        key = f"bronze/api_football/fixture_detail/season={SEASON}/fixture_id={fid}/statistics.json.gz"
        if not exists(key):
            continue
        for side in read_json_gz(key).get("response", []):
            rec = {
                "fixture_id": fid,
                "team":       side["team"]["name"],
                "team_id":    side["team"]["id"],
            }
            for s in side["statistics"]:
                col = s["type"].lower().replace(" ", "_").replace("%", "pct")
                val = s["value"]
                if isinstance(val, str) and val.endswith("%"):
                    val = float(val.rstrip("%"))
                rec[col] = val
            rows.append(rec)
    if rows:
        df = pd.DataFrame(rows)
        put_parquet(
            f"silver/matches/af_match_stats/season={SEASON}/part-0.parquet",
            df, SRC, meta={"fixtures": df["fixture_id"].nunique()}
        )
        print(f"  ✓ {len(df)} stats rows ({df['fixture_id'].nunique()} trận) -> silver")


def build_silver_top_scorers(body: dict):
    """Vua phá lưới -> silver."""
    rows = []
    for item in body.get("response", []):
        p = item["player"]
        s = item["statistics"][0] if item.get("statistics") else {}
        rows.append({
            "player_id":   p["id"],
            "player":      p["name"],
            "nationality": p.get("nationality"),
            "age":         p.get("age"),
            "team":        (s.get("team") or {}).get("name"),
            "goals":       (s.get("goals") or {}).get("total"),
            "assists":     (s.get("goals") or {}).get("assists"),
            "appearances": (s.get("games") or {}).get("appearences"),
            "minutes":     (s.get("games") or {}).get("minutes"),
        })
    if rows:
        df = pd.DataFrame(rows)
        df["ingest_date"] = D
        put_parquet(
            f"silver/players/af_top_scorers/season={SEASON}/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} top scorers -> silver")


# ─────────────────── MAIN ────────────────────────────────────────────────
def run_pipeline():
    if not API_KEY:
        print("  ! Thiếu API_FOOTBALL_KEY trong .env")
        return

    print(f"=== p24: API-Football Full Pipeline (budget={DAILY_BUDGET} req) ===\n")

    # ── BRONZE ──
    print("[1/7] Teams & Venues")
    teams_body = ingest_teams()

    print("\n[2/7] Standings")
    standings_body = ingest_standings()

    print("\n[3/7] Fixtures (380 trận)")
    fixtures_body = ingest_fixtures()

    print("\n[4/7] Top Scorers")
    scorers_body = ingest_top_scorers()

    print("\n[5/7] Fixture Details (Events + Lineups + Statistics)")
    done = load_checkpoint()
    done = ingest_fixture_details(fixtures_body, done)
    save_checkpoint(done)

    # ── SILVER ──
    print("\n[6/7] Build Silver — Fixtures & Standings & Scorers")
    build_silver_fixtures(fixtures_body)
    build_silver_standings(standings_body)
    build_silver_top_scorers(scorers_body)

    print("\n[7/7] Build Silver — Match Details (Events + Lineups + Stats)")
    build_silver_events(done)
    build_silver_lineups(done)
    build_silver_stats(done)

    # ── SUMMARY ──
    print("\n=== SUMMARY ===")
    summary("bronze/api_football/")
    summary("silver/matches/af_")
    summary("silver/standings/af_")
    summary("silver/players/af_")


if __name__ == "__main__":
    run_pipeline()
