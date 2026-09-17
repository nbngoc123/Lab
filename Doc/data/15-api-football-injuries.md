# 15 — API-Football: Injuries (chấn thương + treo giò) → MinIO

**Kiểu ingest:** REST API, dùng lại API key đã có ở file `02`, rate-limit chung 1 pool với `02`
**Tần suất:** 1 lần/ngày (snapshot)
**Độ khó:** ★☆☆☆☆ — chỉ 1 endpoint, tốn đúng **1 request/ngày**

---

## 1. Vì sao chọn nguồn này

Bảy nguồn trước cho bạn kết quả, đội hình, xG, dimension cầu thủ — nhưng không nguồn nào trả lời được câu hỏi **"ai đang không thể ra sân, vì lý do gì"**. Đây là biến ảnh hưởng trực tiếp tới kết quả trận đấu (đội thiếu trụ cột thắng ít hơn) nhưng gần như không nguồn miễn phí nào có dưới dạng API sạch — đa số chỉ có ở trang báo dạng text tự do.

API-Football đã có sẵn key và pipeline (`02-api-football.md`) — nguồn này chỉ là **thêm 1 endpoint vào đúng file đó**, dùng lại toàn bộ hạ tầng: `call()` với quota budget, checkpoint, layout MinIO. Không cần đăng ký gì mới.

**Đính chính quan trọng:** API-Football **chỉ có 1 endpoint `/injuries`**, không có endpoint `/sidelined` riêng biệt (đó là thuật ngữ của Sportmonks, một nhà cung cấp khác, không áp dụng ở đây). Field `player.reason` trong response của `/injuries` đã tự phân biệt lý do — bao gồm cả chấn thương lẫn treo giò/kỷ luật — nên 1 endpoint là đủ cho cả hai loại "absence".

## 2. Endpoint

| Endpoint | Ý nghĩa | Chi phí quota |
|---|---|---|
| `/injuries?league=39&season=2025` | Snapshot toàn giải: ai đang chấn thương/treo giò tại thời điểm gọi | **1 request** |
| `/injuries?fixture={id}` | Ai vắng mặt tại đúng 1 trận cụ thể (khác snapshot hiện tại) | 1/trận — tùy chọn, tốn quota hơn |
| `/injuries?team={id}&season=2025` | Lọc theo 1 đội | 1 |
| `/injuries?player={id}&season=2025` | Lịch sử absence của 1 cầu thủ | 1 |

Base: `https://v3.football.api-sports.io`, header `x-apisports-key` — **dùng chung key với file `02`**.

Response mỗi bản ghi gồm:

```json
{
  "player": {"id": 306, "name": "M. Odegaard", "photo": "...",
             "type": "Missing Fixture", "reason": "Knee Injury"},
  "team": {"id": 42, "name": "Arsenal", "logo": "..."},
  "fixture": {"id": 1208021, "timezone": "UTC", "date": "2026-09-20T14:00:00+00:00"},
  "league": {"id": 39, "season": 2025, "name": "Premier League", "country": "England"}
}
```

`reason` là field then chốt để phân loại: chứa "Injury"/"Knee"/"Ankle"... với chấn thương thật, hoặc "Suspended"/"Red Card"/"Yellow Card Accumulation" với treo giò.

## 3. Chiến lược quota

Vì `02` và `15` dùng **chung 1 pool 100 request/ngày**, cần chia lịch:

```
Ngày chạy 02 (fixtures/events/standings/...) → chiếm ~90-95 request
Ngày chạy 15 (injuries snapshot)             → chỉ cần 1 request

→ Khuyến nghị: chạy 15 TRƯỚC 02 trong cùng ngày (1 request rẻ, không
  tranh chấp), hoặc gộp injuries vào ngay đầu vòng lặp của p02.
```

## 4. Layout trong lake

```
bronze/api_football/injuries/season=2025/ingest_date=2026-09-16/injuries.json.gz
bronze/api_football/injuries_by_fixture/season=2025/fixture_id=1208021/injuries.json.gz   ← tùy chọn
_meta/api_football/injuries_checkpoint.json
_meta/api_football/injuries_quota_log/2026-09-16.json

silver/players/player_absence/season=2025/ingest_date=2026-09-16/part-0.parquet
```

Vì đây là **snapshot theo ngày** (không phải nguồn tĩnh), partition bằng `ingest_date=` — mỗi lần chạy tạo 1 bản mới, cho phép dựng time-series "ai chấn thương vào ngày nào" thay vì chỉ có trạng thái mới nhất.

## 5. Script ingest — `pipelines/p02c_injuries.py`

Tạo file riêng, import chung helper với `p02`:

```python
"""
15 — Mở rộng API-Football: ingest /injuries -> silver/player_absence.parquet

Chỉ có 1 endpoint thật: GET /injuries
Tham số hợp lệ: league, season, fixture, team, player, date, ids, timezone
(Không có endpoint /sidelined riêng — đó là thuật ngữ Sportmonks, không áp
dụng cho API-Football. Field `reason` trong response của /injuries đã phân
biệt chấn thương vs treo giò/kỷ luật.)

Dùng lại đúng pattern đã có trong pipelines/p02_api_football.py:
call() với quota budget + sleep, checkpoint trong MinIO, layout bronze/silver
theo quy ước file 00.
"""
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
LEAGUE, SEASON = 39, 2025          # 39 = Premier League

# Quota riêng cho lần chạy injuries — CỘNG DỒN với những gì p02 đã dùng
# trong ngày. Free plan = 100 req/ngày TỔNG cho cả tài khoản.
DAILY_BUDGET = 30
CHECKPOINT_KEY = "_meta/api_football/injuries_checkpoint.json"

HEADERS = {"x-apisports-key": os.getenv("API_FOOTBALL_KEY")}
_used = 0
_remaining = None


def call(path: str, params: dict) -> dict:
    """Giống hệt call() trong p02 — đếm quota, dừng nếu chạm budget."""
    global _used, _remaining
    if _used >= DAILY_BUDGET:
        raise RuntimeError(f"Đã dùng hết budget {DAILY_BUDGET} request cho injuries hôm nay")

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
    return set(json.loads(raw)["done_dates"])


def save_checkpoint(done: set):
    S3.put_object(
        Bucket=BUCKET, Key=CHECKPOINT_KEY,
        Body=json.dumps({"done_dates": sorted(done), "updated_at": D}).encode(),
        ContentType="application/json")
    print(f"  ✓ checkpoint injuries: {len(done)} ngày đã ingest")


# ---------- BRONZE ----------
def ingest_injuries_current() -> dict:
    """Snapshot toàn giải — 1 request duy nhất, chạy hàng ngày."""
    body = call("/injuries", {"league": LEAGUE, "season": SEASON})
    put_json_gz(
        f"bronze/api_football/injuries/season={SEASON}/ingest_date={D}/injuries.json.gz",
        body, SRC, meta={"count": body["results"]})
    return body


def ingest_injuries_by_fixture(fixture_ids: list, done: set):
    """Tùy chọn: injuries tại đúng thời điểm 1 trận — hữu ích cho feature
    engineering ("đội hình thiếu ai NGAY TRẬN ĐÓ"), tốn 1 request/fixture."""
    for fid in fixture_ids:
        ckey = f"fixture_{fid}"
        if ckey in done or _used >= DAILY_BUDGET:
            continue
        try:
            body = call("/injuries", {"fixture": fid})
        except RuntimeError as e:
            print(f"  ! dừng: {e}")
            break
        put_json_gz(
            f"bronze/api_football/injuries_by_fixture/season={SEASON}"
            f"/fixture_id={fid}/injuries.json.gz",
            body, SRC, meta={"fixture_id": fid, "count": body["results"]})
        done.add(ckey)
        print(f"  ✓ injuries fixture {fid} xong ({body['results']} bản ghi)")
    return done


# ---------- SILVER ----------
def build_player_absence(body: dict) -> pd.DataFrame:
    rows = []
    for r in body.get("response", []):
        player = r.get("player", {}) or {}
        team = r.get("team", {}) or {}
        fixture = r.get("fixture", {}) or {}
        league = r.get("league", {}) or {}
        rows.append({
            "player_id": player.get("id"),
            "player_name": player.get("name"),
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "fixture_id": fixture.get("id"),
            "fixture_date": fixture.get("date"),
            "league_id": league.get("id"),
            "league_name": league.get("name"),
            "season": league.get("season"),
            "absence_type": player.get("type"),      # vd: "Missing Fixture"
            "absence_reason": player.get("reason"),  # vd: "Injury", "Suspended"
            "ingest_date": D,
        })
    df = pd.DataFrame(rows)
    if df.empty:
        print("  ! không có bản ghi injuries nào (có thể ngoài mùa/giữa vòng đấu)")
        return df

    df["fixture_date"] = pd.to_datetime(df["fixture_date"], errors="coerce")
    df["is_suspension"] = df["absence_reason"].str.contains(
        "suspen", case=False, na=False)

    put_parquet(
        f"silver/players/player_absence/season={SEASON}/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"season": SEASON, "rows": len(df)})
    print(f"  ✓ player_absence: {len(df)} dòng "
          f"({df['is_suspension'].sum()} treo giò, "
          f"{(~df['is_suspension']).sum()} chấn thương)")
    return df


def log_quota():
    S3.put_object(
        Bucket=BUCKET, Key=f"_meta/api_football/injuries_quota_log/{D}.json",
        Body=json.dumps({"date": D, "used": _used,
                         "server_remaining": _remaining}).encode(),
        ContentType="application/json")


if __name__ == "__main__":
    print("[1/2] injuries snapshot toàn giải (1 request)")
    body = ingest_injuries_current()

    print("[2/2] silver: player_absence")
    df = build_player_absence(body)

    log_quota()
    summary("bronze/api_football/injuries/")
    summary("silver/players/player_absence/")
```

Chạy:

```bash
python -m pipelines.p02c_injuries
```

## 6. Kết quả mong đợi

```
[1/2] injuries snapshot toàn giải (1 request)
    [quota] dùng 1/30 | server còn 99
  ✓ s3://football-lake/bronze/api_football/injuries/season=2025/ingest_date=2026-09-16/injuries.json.gz  (4,812 B, ...)
[2/2] silver: player_absence
  ✓ .../player_absence/season=2025/ingest_date=2026-09-16/part-0.parquet  (2,108 B, ...)
  ✓ player_absence: 22 dòng (6 treo giò, 16 chấn thương)

[summary] bronze/api_football/injuries/: 1 objects, 0.005 MB
[summary] silver/players/player_absence/: 1 objects, 0.002 MB
```

## 7. Truy vấn kiểm chứng

```sql
-- Đội nào đang thiếu quân nhiều nhất hôm nay?
SELECT team_name, COUNT(*) AS so_nguoi_vang,
       SUM(CASE WHEN is_suspension THEN 1 ELSE 0 END) AS treo_gio,
       SUM(CASE WHEN NOT is_suspension THEN 1 ELSE 0 END) AS chan_thuong
FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet')
WHERE ingest_date = (SELECT MAX(ingest_date)
                     FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet'))
GROUP BY team_name
ORDER BY so_nguoi_vang DESC;

-- Loại chấn thương nào phổ biến nhất?
SELECT absence_reason, COUNT(*) AS n
FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet')
WHERE NOT is_suspension
GROUP BY absence_reason ORDER BY n DESC;

-- GHÉP NGUỒN: đội thiếu quân có thắng ít hơn không? (join với fpl_fixtures file 01)
SELECT f.gameweek, f.home_team, f.away_team,
       h.n_vang AS home_thieu_quan, a.n_vang AS away_thieu_quan,
       f.team_h_score, f.team_a_score
FROM read_parquet('s3://football-lake/silver/fixtures/fpl_fixtures/**/*.parquet') f
LEFT JOIN (SELECT team_name, COUNT(*) AS n_vang
           FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet')
           GROUP BY team_name) h ON h.team_name = f.home_team
LEFT JOIN (SELECT team_name, COUNT(*) AS n_vang
           FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet')
           GROUP BY team_name) a ON a.team_name = f.away_team
ORDER BY f.gameweek DESC;
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `errors: {"requests": "..."}` | Hết quota ngày — nhớ là **chung pool** với file `02` | Kiểm tra `_meta/api_football/quota_log/` xem `02` đã dùng bao nhiêu trước khi chạy `15` |
| `response: []` dù đúng mùa | Giữa vòng đấu, tạm thời không ai chấn thương mới | Bình thường — silver sẽ có 0 dòng ngày đó, không phải lỗi |
| `absence_reason` trống hoặc None | Một số giải/đội không cập nhật chi tiết lý do | Giữ NaN, không suy diễn — xử lý ở gold nếu cần |
| Cờ `is_suspension` sai (fallback vào "chấn thương") | Text lý do không chứa từ "suspend" dù đúng là treo giò (vd: "Red Card") | Mở rộng regex trong `str.contains` nếu quan sát thấy pattern khác |

## 9. Mở rộng

- Kéo `ingest_injuries_by_fixture()` cho các fixture sắp diễn ra (lấy `fixture_id` từ `bronze/api_football/fixtures/` của file `02`) để có đúng đội hình vắng mặt dự kiến — feature quan trọng cho model dự đoán kết quả.
- So sánh `player_absence` với `physioroom` (file `16`) và `transfermarkt` (file `17`) trên cùng ngày để làm bảng **data-quality check chéo 3 nguồn độc lập** — nguồn nào báo chấn thương mà 2 nguồn kia không có thì đáng nghi.
- Join `player_id` với `wd_players` (file `06`, Wikidata QID) để chuẩn hóa tên cầu thủ giữa các nguồn.
