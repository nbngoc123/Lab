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
import hashlib
import json
import os
import time
from urllib.parse import urlparse

import pandas as pd

from lake.minio_io import (
    put_json_gz, put_bytes, put_parquet, read_json_gz,
    exists, today, summary, S3, BUCKET
)
from lake.http import SESSION

MIME = {".png": "image/png", ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg", ".webp": "image/webp"}

# ─────────────────── config ───────────────────────────────────────────────
BASE       = "https://v3.football.api-sports.io"
SRC        = "api-football"
D          = today()
LEAGUE     = 39          # Premier League
SEASON     = 2024        # 2024/2025
TEST_MODE  = os.getenv("TEST_MODE") == "1"
DAILY_BUDGET = 5 if TEST_MODE else 75        # Giới hạn 75/100 req/ngày (để an toàn dưới 100)
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


# ─── TOP STATS (không tốn nhiều quota) ───────────────────────────────────────
def ingest_top_assists():
    key = f"bronze/api_football/top_assists/season={SEASON}/ingest_date={D}/assists.json.gz"
    body = call("/players/topassists", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} top assists -> bronze")
    return body


def ingest_top_yellow_cards():
    key = f"bronze/api_football/top_yellow/season={SEASON}/ingest_date={D}/yellow.json.gz"
    body = call("/players/topyellowcards", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} top yellow cards -> bronze")
    return body


def ingest_top_red_cards():
    key = f"bronze/api_football/top_red/season={SEASON}/ingest_date={D}/red.json.gz"
    body = call("/players/topredcards", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} top red cards -> bronze")
    return body


def ingest_injuries():
    """Danh sách chấn thương hiện tại."""
    key = f"bronze/api_football/injuries/season={SEASON}/ingest_date={D}/injuries.json.gz"
    body = call("/injuries", {"league": LEAGUE, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"]})
    print(f"  ✓ {body['results']} injuries -> bronze")
    return body


# ─── PLAYERS (profile + season stats) ────────────────────────────────────────
def ingest_players_squad():
    """
    Lấy danh sách cầu thủ EPL kèm thống kê mùa giải.
    API trả về ~20 cầu thủ/page. Free tier: page 1 thôi (tiết kiệm quota).
    Mỗi lần chạy tăng thêm 1 page, checkpoint bằng page đã lấy.
    """
    ck_key = "_meta/api_football_p24/players_checkpoint.json"
    if exists(ck_key):
        raw = S3.get_object(Bucket=BUCKET, Key=ck_key)["Body"].read()
        last_page = json.loads(raw).get("last_page", 0)
    else:
        last_page = 0

    page = last_page + 1
    key = f"bronze/api_football/players/season={SEASON}/page={page}/players.json.gz"
    if exists(key):
        print(f"  · players page {page} đã cache, bỏ qua")
        return

    body = call("/players", {"league": LEAGUE, "season": SEASON, "page": page})
    total_pages = (body.get("paging") or {}).get("total", 1)
    put_json_gz(key, body, SRC, meta={"page": page, "total_pages": total_pages})
    print(f"  ✓ players page {page}/{total_pages} -> bronze")

    # Lưu checkpoint page
    S3.put_object(
        Bucket=BUCKET, Key=ck_key,
        Body=json.dumps({"last_page": page, "total_pages": total_pages}).encode(),
        ContentType="application/json"
    )
    return body, page, total_pages


# ─── BINARY MEDIA (logo + venue + player photo) ───────────────────────────────
def _fetch_binary(url: str, s3_key: str, entity: str, eid: str, role: str) -> dict | None:
    """Tải 1 file ảnh về MinIO. Idempotent."""
    if not url or not url.startswith("http"):
        return None
    if exists(s3_key):
        try:
            head = S3.head_object(Bucket=BUCKET, Key=s3_key)
            return {"s3_key": s3_key, "entity": entity, "entity_id": str(eid),
                    "role": role, "size_bytes": head.get("ContentLength", 0),
                    "sha256": "cached", "source_url": url, "ingest_date": D}
        except Exception:
            return None
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"    ! không tải {role} {eid}: {e}")
        return None
    data = r.content
    ext = os.path.splitext(urlparse(url).path)[1].lower() or ".png"
    ctype = MIME.get(ext, "image/png")
    put_bytes(s3_key, data, SRC, content_type=ctype,
              meta={"entity": entity, "entity_id": str(eid), "role": role})
    time.sleep(0.3)
    return {"s3_key": s3_key, "entity": entity, "entity_id": str(eid),
            "role": role, "size_bytes": len(data),
            "sha256": hashlib.sha256(data).hexdigest(),
            "source_url": url, "ingest_date": D}


def ingest_media_binary(teams_body: dict) -> list:
    """
    Tải binary: logo đội, ảnh sân (từ /teams), và ảnh cầu thủ (từ /players đã cache).
    Không tốn quota API — chỉ HTTP GET đến CDN.
    """
    manifest = []

    # A1: Logo đội + ảnh sân
    for item in teams_body.get("response", []):
        team = item.get("team", {})
        venue = item.get("venue", {})
        tid = team.get("id")
        tname = team.get("name", tid)

        # Logo đội
        logo_url = team.get("logo")
        if logo_url:
            ext = os.path.splitext(urlparse(logo_url).path)[1].lower() or ".png"
            mf = _fetch_binary(
                logo_url,
                f"bronze/api_football/media/entity=team/team_id={tid}/logo{ext}",
                "team", tid, "logo"
            )
            if mf:
                manifest.append(mf)

        # Ảnh sân vận động
        venue_url = venue.get("image")
        vid = venue.get("id", tid)
        if venue_url:
            ext = os.path.splitext(urlparse(venue_url).path)[1].lower() or ".jpg"
            mf = _fetch_binary(
                venue_url,
                f"bronze/api_football/media/entity=venue/venue_id={vid}/photo{ext}",
                "venue", vid, "photo"
            )
            if mf:
                manifest.append(mf)
        print(f"    · {tname}: logo + venue")

    # B: Ảnh cầu thủ từ players đã cache
    import glob
    pages = [k for k in _list_bronze_keys(f"bronze/api_football/players/season={SEASON}/")]
    for pk in pages:
        try:
            pbody = read_json_gz(pk)
        except Exception:
            continue
        for item in pbody.get("response", []):
            p = item.get("player", {})
            pid = p.get("id")
            photo_url = p.get("photo")
            if pid and photo_url:
                ext = os.path.splitext(urlparse(photo_url).path)[1].lower() or ".png"
                mf = _fetch_binary(
                    photo_url,
                    f"bronze/api_football/media/entity=player/player_id={pid}/photo{ext}",
                    "player", pid, "photo"
                )
                if mf:
                    manifest.append(mf)

    print(f"  ✓ {len(manifest)} files media -> bronze")
    return manifest


def _list_bronze_keys(prefix: str) -> list:
    """List tất cả object keys dưới prefix trong MinIO."""
    paginator = S3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


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


# ─── SILVER: players + media manifest ────────────────────────────────────────
def build_silver_players():
    """Gộp tất cả pages players -> silver parquet."""
    rows = []
    for pk in _list_bronze_keys(f"bronze/api_football/players/season={SEASON}/"):
        try:
            body = read_json_gz(pk)
        except Exception:
            continue
        for item in body.get("response", []):
            p = item.get("player", {})
            s = item["statistics"][0] if item.get("statistics") else {}
            rows.append({
                "player_id":    p.get("id"),
                "name":         p.get("name"),
                "firstname":    p.get("firstname"),
                "lastname":     p.get("lastname"),
                "age":          p.get("age"),
                "nationality":  p.get("nationality"),
                "height":       p.get("height"),
                "weight":       p.get("weight"),
                "photo_url":    p.get("photo"),
                "team_id":      (s.get("team") or {}).get("id"),
                "team":         (s.get("team") or {}).get("name"),
                "position":     (s.get("games") or {}).get("position"),
                "appearances":  (s.get("games") or {}).get("appearences"),
                "minutes":      (s.get("games") or {}).get("minutes"),
                "goals":        (s.get("goals") or {}).get("total"),
                "assists":      (s.get("goals") or {}).get("assists"),
                "yellow_cards": (s.get("cards") or {}).get("yellow"),
                "red_cards":    (s.get("cards") or {}).get("red"),
                "shots_total":  (s.get("shots") or {}).get("total"),
                "shots_on":     (s.get("shots") or {}).get("on"),
                "passes_total": (s.get("passes") or {}).get("total"),
                "passes_key":   (s.get("passes") or {}).get("key"),
                "dribbles_att": (s.get("dribbles") or {}).get("attempts"),
                "dribbles_ok":  (s.get("dribbles") or {}).get("success"),
                "rating":       (s.get("games") or {}).get("rating"),
            })
    if rows:
        df = pd.DataFrame(rows)
        df["ingest_date"] = D
        put_parquet(
            f"silver/players/af_players/season={SEASON}/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} players -> silver")


def build_silver_media_manifest(manifest: list):
    """Lưu manifest ảnh (path + sha256 + metadata) -> silver parquet."""
    if not manifest:
        return
    df = pd.DataFrame(manifest)
    put_parquet(
        f"silver/media/af_media_manifest/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"rows": len(df)}
    )
    print(f"  ✓ {len(df)} media entries -> silver manifest")


def build_silver_injuries(body: dict):
    """Chấn thương -> silver."""
    rows = []
    for item in body.get("response", []):
        p  = item.get("player", {})
        tm = item.get("team", {})
        fx = item.get("fixture", {})
        rows.append({
            "player_id":   p.get("id"),
            "player":      p.get("name"),
            "type":        p.get("type"),
            "reason":      p.get("reason"),
            "team_id":     tm.get("id"),
            "team":        tm.get("name"),
            "fixture_id":  fx.get("id"),
            "fixture_date": fx.get("date"),
        })
    if rows:
        df = pd.DataFrame(rows)
        df["ingest_date"] = D
        put_parquet(
            f"silver/players/af_injuries/season={SEASON}/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} injuries -> silver")


# ─────────────────── MAIN ────────────────────────────────────────────────
def run_pipeline():
    if not API_KEY:
        print("  ! Thiếu API_FOOTBALL_KEY trong .env")
        return

    print(f"=== p24: API-Football Full Pipeline (budget={DAILY_BUDGET} req) ===\n")

    teams_body = None
    standings_body = None
    fixtures_body = None
    scorers_body = None
    assists_body = None
    yellow_body = None
    red_body = None
    injuries_body = None
    manifest = []
    done = load_checkpoint()

    try:
        # ── BRONZE ──
        print("[1/9] Teams & Venues")
        teams_body = ingest_teams()

        print("\n[2/9] Standings")
        standings_body = ingest_standings()

        print("\n[3/9] Fixtures (380 trận)")
        fixtures_body = ingest_fixtures()

        print("\n[4/9] Top Scorers + Assists + Yellow + Red cards")
        scorers_body   = ingest_top_scorers()
        assists_body   = ingest_top_assists()
        yellow_body    = ingest_top_yellow_cards()
        red_body       = ingest_top_red_cards()

        print("\n[5/9] Injuries")
        injuries_body = ingest_injuries()

        print("\n[6/9] Players (1 page/ngày, tích lũy)")
        ingest_players_squad()

        print("\n[7/9] Fixture Details (Events + Lineups + Statistics)")
        done = ingest_fixture_details(fixtures_body, done)
        save_checkpoint(done)

        print("\n[8/9] Binary Media (logo + venue + player photos — không tốn quota API)")
        manifest = ingest_media_binary(teams_body)
    except Exception as e:
        print(f"\n[!] Dừng lấy Bronze do lỗi (hoặc hết Quota): {e}")
        print("[!] Đang tự động chuyển sang Bước 9: Build Silver với dữ liệu hiện có...")
        save_checkpoint(done)

    # ── SILVER ──
    print("\n[9/9] Build Silver")
    if fixtures_body: build_silver_fixtures(fixtures_body)
    if standings_body: build_silver_standings(standings_body)
    if scorers_body: build_silver_top_scorers(scorers_body)
    build_silver_events(done)
    build_silver_lineups(done)
    build_silver_stats(done)
    build_silver_players()
    if manifest: build_silver_media_manifest(manifest)
    if injuries_body: build_silver_injuries(injuries_body)

    # ── SUMMARY ──
    print("\n=== SUMMARY ===")
    summary("bronze/api_football/")
    summary("silver/matches/af_")
    summary("silver/standings/af_")
    summary("silver/players/af_")
    summary("silver/media/af_")


if __name__ == "__main__":
    run_pipeline()
