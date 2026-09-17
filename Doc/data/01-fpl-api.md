# 01 — Fantasy Premier League API → MinIO

**Kiểu ingest:** REST API công khai, không cần auth
**Tần suất:** 1 lần/ngày (hoặc sau mỗi gameweek)
**Độ khó:** ★☆☆☆☆ — nên làm đầu tiên

---

## 1. Vì sao chọn nguồn này

Đây là API chính thức của Premier League phục vụ game Fantasy. Không key, không rate limit công bố, JSON sạch và ổn định nhiều năm. Nó cho bạn **toàn bộ ~700 cầu thủ PL với chỉ số theo từng gameweek** — đủ để dựng một fact table thật, không phải dữ liệu đồ chơi.

## 2. Các endpoint dùng

| Endpoint | Nội dung | Kích thước |
|---|---|---|
| `/api/bootstrap-static/` | Tất cả cầu thủ, đội, gameweek, vị trí, giá, điểm tổng | ~1.2 MB JSON |
| `/api/fixtures/` | 380 trận cả mùa, kèm kết quả và chỉ số trận | ~600 KB |
| `/api/element-summary/{player_id}/` | Lịch sử từng GW của 1 cầu thủ + mùa trước | ~15 KB × 700 |
| `/api/event/{gw}/live/` | Điểm & thống kê live của toàn bộ cầu thủ trong GW | ~400 KB |

Base URL: `https://fantasy.premierleague.com`

## 3. Layout trong lake

```
bronze/fpl/bootstrap_static/ingest_date=2026-09-16/bootstrap.json.gz
bronze/fpl/fixtures/ingest_date=2026-09-16/fixtures.json.gz
bronze/fpl/element_summary/ingest_date=2026-09-16/player_id=001.json.gz
                                                 /player_id=002.json.gz
bronze/fpl/event_live/ingest_date=2026-09-16/gw=05.json.gz

silver/players/fpl_player_dim/ingest_date=2026-09-16/part-0.parquet
silver/players/fpl_player_gw/season=2025-26/part-0.parquet
silver/fixtures/fpl_fixtures/season=2025-26/part-0.parquet
```

## 4. Script ingest — `pipelines/p01_fpl.py`

```python
"""Ingest Fantasy Premier League API vào MinIO bronze + silver."""
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
    return data


def ingest_fixtures() -> list:
    data = get(f"{BASE}/fixtures/").json()
    put_json_gz(f"bronze/fpl/fixtures/ingest_date={D}/fixtures.json.gz",
                data, SRC, meta={"fixtures": len(data)})
    return data


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
def build_player_dim(bootstrap: dict):
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


def build_player_gw_fact(player_ids):
    """Gộp history từng GW của mọi cầu thủ thành 1 fact table."""
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


def build_fixtures(fixtures: list, bootstrap: dict):
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
    bs = ingest_bootstrap()
    pids = [e["id"] for e in bs["elements"]]
    print(f"      {len(pids)} cầu thủ, {len(bs['teams'])} đội")

    print("[2/5] fixtures")
    fx = ingest_fixtures()

    print("[3/5] element-summary (chậm, ~5 phút)")
    ingest_player_histories(pids)

    print("[4/5] live gameweek")
    current = next((e["id"] for e in bs["events"] if e["is_current"]), 1)
    ingest_live_gw(current)

    print("[5/5] silver")
    build_player_dim(bs)
    build_player_gw_fact(pids)
    build_fixtures(fx, bs)

    summary("bronze/fpl/")
    summary("silver/players/")
```

Chạy:

```bash
python -m pipelines.p01_fpl
```

## 5. Kết quả mong đợi

```
[1/5] bootstrap-static
  ✓ s3://football-lake/bronze/fpl/bootstrap_static/ingest_date=2026-09-16/bootstrap.json.gz  (241,883 B, sha256:8f21ad...)
      712 cầu thủ, 20 đội
[2/5] fixtures
  ✓ s3://football-lake/bronze/fpl/fixtures/ingest_date=2026-09-16/fixtures.json.gz  (38,412 B, ...)
[3/5] element-summary (chậm, ~5 phút)
  ... 50/712
  ... 700/712
[bronze] element_summary: 712/712 cầu thủ
...
[summary] bronze/fpl/: 716 objects, 6.83 MB
[summary] silver/players/: 2 objects, 1.94 MB
```

## 6. Truy vấn kiểm chứng (DuckDB)

Dùng connection ở file `00`, rồi:

```sql
-- Top 10 cầu thủ hiệu quả nhất theo điểm/triệu giá
SELECT web_name, team_name, position, price_m, total_points,
       ROUND(total_points / NULLIF(price_m, 0), 2) AS pts_per_m
FROM read_parquet('s3://football-lake/silver/players/fpl_player_dim/**/*.parquet')
WHERE minutes > 300
ORDER BY pts_per_m DESC
LIMIT 10;

-- Phong độ theo gameweek của 1 cầu thủ
SELECT gameweek, total_points, minutes, expected_goals, expected_assists
FROM read_parquet('s3://football-lake/silver/players/fpl_player_gw/**/*.parquet')
WHERE player_id = 328
ORDER BY gameweek;
```

Kết quả mẫu:

```
┌──────────────┬───────────────┬──────────┬─────────┬──────────────┬───────────┐
│   web_name   │   team_name   │ position │ price_m │ total_points │ pts_per_m │
├──────────────┼───────────────┼──────────┼─────────┼──────────────┼───────────┤
│ ...          │ ...           │ MID      │     5.5 │           38 │      6.91 │
```

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `403 Forbidden` sau vài trăm request | Gọi quá nhanh, Cloudflare chặn | Tăng `sleep` lên 0.5s, đặt User-Agent thật |
| `history` rỗng | Mùa giải chưa đá GW nào | Dùng `history_past` để có dữ liệu mùa trước |
| Cột `expected_goals` là string | API trả về số dạng chuỗi | Đã xử lý bằng `pd.to_numeric` trong script |
| Parquet lỗi `mixed types` | Một số cầu thủ thiếu field | Ép `df = df.reindex(columns=keep)` trước khi ghi |

## 8. Mở rộng

- Lên lịch bằng cron/Airflow chạy sau mỗi GW: dữ liệu mới tự vào partition `ingest_date` mới, không ghi đè.
- So `expected_goals` của FPL với xG của StatsBomb (file 04) để kiểm chứng chéo hai nhà cung cấp.
- Join `fpl_player_gw.fixture` với `fpl_fixtures.fixture_id` để có bảng "cầu thủ × đối thủ × sân nhà/khách".
