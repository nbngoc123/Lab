# 02 — API-Football → MinIO

**Kiểu ingest:** REST API có API key, **rate limit chặt** (free: 100 req/ngày)
**Tần suất:** 1 lần/ngày, có quota budget
**Độ khó:** ★★★☆☆ — điểm khó là quản lý quota, không phải code

---

## 1. Vì sao chọn nguồn này

Đây là nguồn duy nhất trong bộ 8 cho bạn **event-level của giải đang diễn ra** (bàn thắng phút thứ mấy, thẻ, thay người, đội hình xuất phát, thống kê trận). FPL không có, Football-Data.co.uk không có, StatsBomb có nhưng là mùa cũ. Nó lấp đúng khoảng trống đó.

Điểm dạy được nhiều nhất: **ingest dưới ràng buộc quota** — bạn buộc phải viết checkpoint, idempotency, và budget tracking. Đây mới là thứ giống production.

## 2. Đăng ký & endpoint

1. Tạo tài khoản tại `dashboard.api-football.com` (hoặc qua RapidAPI).
2. Lấy key → điền vào `.env`: `API_FOOTBALL_KEY=...`
3. Free plan: **100 request/ngày**, header trả về `x-ratelimit-requests-remaining`.

| Endpoint | Ý nghĩa | Chi phí quota |
|---|---|---|
| `/fixtures?league=39&season=2025` | Toàn bộ 380 trận PL cả mùa | **1 request** |
| `/fixtures?league=39&season=2025&from=..&to=..` | Trận trong khoảng ngày | 1 |
| `/fixtures/events?fixture={id}` | Sự kiện 1 trận | 1/trận |
| `/fixtures/lineups?fixture={id}` | Đội hình 1 trận | 1/trận |
| `/fixtures/statistics?fixture={id}` | Thống kê 1 trận | 1/trận |
| `/standings?league=39&season=2025` | Bảng xếp hạng | 1 |
| `/teams?league=39&season=2025` | 20 đội + sân | 1 |

Base: `https://v3.football.api-sports.io`, header `x-apisports-key`.
`league=39` là Premier League.

## 3. Chiến lược quota (quan trọng)

Với 100 req/ngày, đừng cố lấy hết. Phân bổ:

```
  1 req  → /teams            (chỉ chạy 1 lần, sau đó cache)
  1 req  → /standings        (mỗi ngày)
  1 req  → /fixtures         (mỗi ngày, để biết trận nào đã đá xong)
 96 req  → chi tiết trận: 3 req/trận × 32 trận chưa ingest
  1 req  → dự phòng
```

→ Sau ~4 ngày chạy là đủ chi tiết cả vòng đấu. Dùng **checkpoint trong MinIO** để biết trận nào đã lấy rồi.

## 4. Layout trong lake

```
bronze/api_football/teams/season=2025/teams.json.gz
bronze/api_football/standings/ingest_date=2026-09-16/standings.json.gz
bronze/api_football/fixtures/season=2025/ingest_date=2026-09-16/fixtures.json.gz
bronze/api_football/fixture_detail/season=2025/fixture_id=1035048/events.json.gz
                                                                /lineups.json.gz
                                                                /statistics.json.gz
_meta/api_football/quota_log/2026-09-16.json      ← nhật ký quota
_meta/api_football/checkpoint.json                 ← fixture_id đã ingest

silver/matches/af_match_events/season=2025/part-0.parquet
silver/matches/af_match_stats/season=2025/part-0.parquet
silver/standings/af_standings/ingest_date=2026-09-16/part-0.parquet
```

## 5. Script ingest — `pipelines/p02_api_football.py`

```python
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
LEAGUE, SEASON = 39, 2025
DAILY_BUDGET = 95           # chừa 5 req dự phòng
CHECKPOINT_KEY = "_meta/api_football/checkpoint.json"

HEADERS = {"x-apisports-key": os.getenv("API_FOOTBALL_KEY")}
_used = 0
_remaining = None


def call(path: str, params: dict) -> dict:
    """Gọi API, đếm quota, dừng hẳn nếu chạm budget."""
    global _used, _remaining
    if _used >= DAILY_BUDGET:
        raise RuntimeError(f"Đã dùng hết budget {DAILY_BUDGET} request hôm nay")

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
        Body=json.dumps({"done_fixture_ids": sorted(done),
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
    finished = [f for f in fixtures["response"]
                if f["fixture"]["status"]["short"] == "FT"
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
        for e in read_json_gz(key)["response"]:
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
        for side in read_json_gz(key)["response"]:
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
    for table in body["response"][0]["league"]["standings"]:
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
    put_parquet(f"silver/standings/af_standings/ingest_date={D}/part-0.parquet",
                df, SRC)
    return df


def log_quota():
    S3.put_object(
        Bucket=BUCKET, Key=f"_meta/api_football/quota_log/{D}.json",
        Body=json.dumps({"date": D, "used": _used,
                         "server_remaining": _remaining}).encode(),
        ContentType="application/json")


if __name__ == "__main__":
    done = load_checkpoint()
    print(f"[0] checkpoint hiện tại: {len(done)} fixture")

    try:
        print("[1] teams");      ingest_teams()
        print("[2] standings");  st = ingest_standings()
        print("[3] fixtures");   fx = ingest_fixtures()
        print("[4] fixture details")
        done = ingest_fixture_details(fx, done)

        print("[5] silver")
        build_standings(st)
        build_events(done)
        build_stats(done)
    finally:
        save_checkpoint(done)
        log_quota()
        summary("bronze/api_football/")
        summary("silver/matches/")
```

## 6. Kết quả mong đợi (lần chạy đầu)

```
[0] checkpoint hiện tại: 0 fixture
[1] teams
    [quota] dùng 1/95 | server còn 99
  ✓ s3://football-lake/bronze/api_football/teams/season=2025/teams.json.gz  (4,112 B, ...)
[2] standings
    [quota] dùng 2/95 | server còn 98
[3] fixtures
    [quota] dùng 3/95 | server còn 97
  ✓ .../fixtures.json.gz  (52,904 B, ...)
[4] fixture details
  ✓ fixture 1035048 xong (Arsenal vs Manchester City)
  ✓ fixture 1035047 xong (Liverpool vs Everton)
  ...
  · hết budget, dừng — chạy lại ngày mai để tiếp tục
[5] silver
  ✓ .../af_standings/ingest_date=2026-09-16/part-0.parquet  (6,221 B, 20 rows)
  ✓ .../af_match_events/season=2025/part-0.parquet  (18,440 B, 412 rows)
  ✓ checkpoint: 30 fixture đã ingest

[summary] bronze/api_football/: 93 objects, 1.74 MB
```

Chạy lần 2 hôm sau: checkpoint đã có 30 fixture → script tự bỏ qua và lấy 30 trận tiếp theo. Đây chính là điểm đáng khoe khi trình bày.

## 7. Truy vấn kiểm chứng

```sql
-- Phút ghi bàn phân bố thế nào?
SELECT (minute / 15)::INT * 15 AS phut_bat_dau,
       COUNT(*) AS so_ban
FROM read_parquet('s3://football-lake/silver/matches/af_match_events/**/*.parquet')
WHERE type = 'Goal'
GROUP BY 1 ORDER BY 1;

-- Đội nào kiểm soát bóng cao nhưng ít sút trúng đích?
SELECT team,
       ROUND(AVG(ball_possession), 1) AS possession,
       ROUND(AVG(shots_on_goal), 1)   AS sut_trung_dich
FROM read_parquet('s3://football-lake/silver/matches/af_match_stats/**/*.parquet')
GROUP BY team
ORDER BY possession DESC;
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `errors: {"requests": "You have reached..."}` | Hết quota ngày | Script đã tự dừng; chờ reset 00:00 UTC |
| `response: []` nhưng không lỗi | Sai `league`/`season`, hoặc season chưa mở với free plan | Free plan thường **chỉ mở một số mùa nhất định** — gọi `/leagues?id=39` để xem `seasons` nào được phép |
| `statistics` có `value: null` | Trận chưa đủ dữ liệu | Giữ null ở silver, xử lý ở gold |
| Cột trong `af_match_stats` không đồng nhất giữa các trận | API trả thiếu loại thống kê | `pd.DataFrame(rows)` tự điền NaN — chấp nhận được |

## 9. Mở rộng

- Chuyển `DAILY_BUDGET` thành biến môi trường để nâng khi lên plan trả phí.
- Poll `/fixtures?live=all` mỗi 60 giây trong lúc có trận đang đá → đẩy vào `bronze/api_football/live/` để mô phỏng streaming hợp lệ (xem gợi ý ở file 00).
- Đối chiếu kết quả trận của API-Football với Football-Data.co.uk (file 03) → dựng bảng data-quality check giữa hai nguồn độc lập.
