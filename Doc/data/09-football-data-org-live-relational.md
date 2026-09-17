# 09 — football-data.org (API v4, database quan hệ đang sống) → MinIO

**Kiểu ingest:** REST API có key, nhưng **mô hình dữ liệu là quan hệ chuẩn** (Area/Competition/Season/Team/Person/Match/Standing liên kết bằng ID)
**Tần suất:** hàng ngày (mùa giải đang diễn ra, cập nhật liên tục)
**Độ khó:** ★★☆☆☆
**Điểm khác biệt với file 05:** đây là nguồn **còn sống** — do Daniel Freitag duy trì liên tục từ 2013, vẫn cập nhật tới hiện tại (2026), trong khi European Soccer DB (file 05) là snapshot tĩnh dừng lại ở mùa 2015/16.

---

## 1. Vì sao chọn nguồn này

File 05 chứng minh bạn biết ingest từ RDBMS — nhưng nó là xác ướp: không đội mới thăng hạng, không cầu thủ chuyển nhượng gần đây, không trận nào sau 2016. Nguồn này lấp đúng khoảng trống đó: **cùng một kiểu mô hình quan hệ, nhưng dữ liệu là thật và đang cập nhật mỗi ngày**.

Giá trị dạy học: bạn phải tự dựng lại các bảng quan hệ (`competitions`, `seasons`, `teams`, `matches`, `standings`) **từ một API**, thay vì được cho sẵn file `.sqlite`. Đây chính là công việc thật của một data engineer khi nguồn dữ liệu là SaaS chứ không phải database nội bộ.

## 2. Đăng ký & mô hình dữ liệu

1. Đăng ký free tại `football-data.org` → nhận token qua email
2. `.env`: `FOOTBALL_DATA_TOKEN=xxxx`
3. Free tier: **10 request/phút**, giới hạn **12 giải lớn** (PL, La Liga, Serie A, Bundesliga, Ligue 1, Champions League, World Cup...), không có lineup/sự kiện chi tiết (cần gói trả phí) — nhưng fixtures, kết quả, standings, đội, cầu thủ thì đủ dùng.

Base URL: `https://api.football-data.org/v4`, header `X-Auth-Token`.

Sơ đồ quan hệ (đúng nghĩa ERD):

```
Area (quốc gia/khu vực)
  └── Competition (giải đấu)  ──code: "PL", "PD", "BL1"...
        └── Season (mùa giải)
              ├── Team (đội) ──┬── Squad → Person (cầu thủ)
              │                └── coach → Person (HLV)
              ├── Match (trận) ── FK: homeTeam.id, awayTeam.id, season.id
              └── Standing (bảng xếp hạng) ── FK: team.id
```

| Endpoint | Trả về | Đóng vai trò |
|---|---|---|
| `/competitions` | Danh sách giải + season hiện tại | Dimension "competitions" |
| `/competitions/{code}` | Chi tiết 1 giải, kèm `currentSeason` | Bổ sung dimension |
| `/competitions/{code}/teams` | Đội + squad (cầu thủ) của giải | Dimension "teams" + "players" |
| `/competitions/{code}/matches` | Toàn bộ trận trong mùa | Fact "matches" |
| `/competitions/{code}/standings` | Bảng xếp hạng hiện tại | Fact "standings" (snapshot theo ngày) |
| `/teams/{id}` | Chi tiết 1 đội, coach, sân | Enrichment |

## 3. Layout trong lake

```
bronze/football_data_org/competitions/ingest_date=2026-09-16/competitions.json.gz
bronze/football_data_org/teams/competition=PL/ingest_date=2026-09-16/teams.json.gz
bronze/football_data_org/matches/competition=PL/season=2025/ingest_date=2026-09-16/matches.json.gz
bronze/football_data_org/standings/competition=PL/ingest_date=2026-09-16/standings.json.gz

silver/dim/fdo_competitions/ingest_date=2026-09-16/part-0.parquet
silver/dim/fdo_teams/competition=PL/ingest_date=2026-09-16/part-0.parquet
silver/dim/fdo_players/competition=PL/ingest_date=2026-09-16/part-0.parquet
silver/matches/fdo_matches/competition=PL/season=2025/part-0.parquet
silver/standings/fdo_standings/competition=PL/ingest_date=2026-09-16/part-0.parquet
```

`standings` được snapshot theo `ingest_date` (không ghi đè) — vì bảng xếp hạng đổi theo từng vòng đấu, bạn muốn giữ lịch sử để vẽ được biểu đồ "thứ hạng qua từng tuần".

## 4. Script ingest — `pipelines/p09_football_data_org.py`

```python
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
    put_parquet(
        f"silver/standings/fdo_standings/competition={code}/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


if __name__ == "__main__":
    print("[1/5] competitions")
    comps = ingest_competitions()
    build_competitions_dim(comps)

    for code in COMPETITIONS:
        print(f"\n[2/5] teams — {code}")
        teams = ingest_teams(code)
        build_teams_dim(code, teams)
        build_players_dim(code, teams)

        print(f"[3/5] matches — {code}")
        matches = ingest_matches(code)
        build_matches_fact(code, matches)

        print(f"[4/5] standings — {code}")
        standings = ingest_standings(code)
        build_standings_fact(code, standings)

    print("\n[5/5] tổng kết")
    summary("bronze/football_data_org/")
    summary("silver/dim/fdo_teams/")
    summary("silver/matches/fdo_matches/")
```

Với 6 giải × 3 request (teams, matches, standings) + 1 request competitions = 19 request, ở tốc độ 10 req/phút mất khoảng **2 phút** cho một lần chạy đầy đủ.

## 5. Kết quả mong đợi

```
[1/5] competitions
  ✓ s3://football-lake/bronze/football_data_org/competitions/ingest_date=2026-09-16/competitions.json.gz  (18,204 B, ...)

[2/5] teams — PL
  ✓ .../teams/competition=PL/ingest_date=2026-09-16/teams.json.gz  (94,112 B, ...)
[3/5] matches — PL
  ✓ .../matches/competition=PL/season=2025/ingest_date=2026-09-16/matches.json.gz  (218,004 B, matches: 380)
[4/5] standings — PL
  ✓ .../standings/competition=PL/ingest_date=2026-09-16/standings.json.gz  (9,441 B, ...)

[2/5] teams — PD
...
[5/5] tổng kết
[summary] bronze/football_data_org/: 25 objects, 3.12 MB
[summary] silver/dim/fdo_teams/: 6 objects, 0.18 MB
[summary] silver/matches/fdo_matches/: 6 objects, 1.44 MB
```

Chạy pipeline này **mỗi ngày**: `standings` sẽ tích lũy một bản snapshot mới mỗi lần chạy (khác `matches`/`teams` vốn ghi đè theo `ingest_date` gần nhất) — sau vài tuần bạn có time series thứ hạng thật.

## 6. Truy vấn kiểm chứng

```sql
-- Đối chiếu 6 giải: đội nào phòng ngự tốt nhất theo goals_against
SELECT competition, team, points, played, goal_diff, goals_against
FROM read_parquet('s3://football-lake/silver/standings/fdo_standings/**/*.parquet')
WHERE rank <= 5
ORDER BY competition, rank;

-- Thứ hạng của 1 đội thay đổi qua các ngày ingest (cần chạy pipeline vài lần)
SELECT ingest_date, rank, points, played
FROM read_parquet('s3://football-lake/silver/standings/fdo_standings/competition=PL/**/*.parquet')
WHERE team = 'Arsenal FC'
ORDER BY ingest_date;

-- GHÉP NGUỒN: so kết quả trận với Football-Data.co.uk (file 03) — data quality check
SELECT a.home_team, a.away_team, a.utc_date::DATE AS ngay,
       a.home_goals AS fdo_home, a.away_goals AS fdo_away,
       b.home_goals AS fd_home,  b.away_goals AS fd_away
FROM read_parquet('s3://football-lake/silver/matches/fdo_matches/competition=PL/**/*.parquet') a
JOIN read_parquet('s3://football-lake/silver/matches/fd_matches/division=E0/season=2425/**/*.parquet') b
  ON a.home_team LIKE '%' || b.home_team || '%'
 AND a.utc_date::DATE = b.match_date::DATE
WHERE a.home_goals != b.home_goals OR a.away_goals != b.away_goals;
```

Truy vấn cuối nên trả về **0 dòng** nếu cả hai nguồn đều đúng — đây là bài kiểm tra chất lượng chéo giữa một nguồn "sống" (API cập nhật hàng ngày) và một nguồn batch (CSV cố định), rất đáng đưa vào phần trình bày.

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `403 Forbidden` trên 1 giải cụ thể | Giải đó không nằm trong free tier | Free tier chỉ gồm 12 giải cố định; bỏ giải đó khỏi `COMPETITIONS` |
| `429 Too Many Requests` | Vượt 10 req/phút | Script tự đọc header `Retry-After` và chờ |
| `squad` rỗng ở 1 vài đội | free tier đôi khi giới hạn độ sâu squad | Chấp nhận NaN, không phải lỗi ingest |
| Trận mùa cũ có `status = "TIMED"` thay vì `"FINISHED"` | Trận hoãn/dời lịch chưa cập nhật | Lọc `WHERE status = 'FINISHED'` khi tính thống kê |
| Tên đội lệch giữa `fdo_teams` và `fd_matches` (file 03) | "Arsenal FC" vs "Arsenal" | Dùng bảng `TEAM_ALIAS` đã tạo ở file 03, hoặc so khớp `LIKE` như ví dụ trên |

## 8. Mở rộng

- Đây là nguồn **sống** duy nhất trong 9 file cho bảng xếp hạng cập nhật hàng ngày — ghép với sentiment Reddit (file 07) theo `ingest_date` để xem tâm lý fan đổi theo thứ hạng thế nào.
- Free tier không có lineup/event chi tiết — với những trận PL, dùng API-Football (file 02) để bổ sung phần này, dùng `match_id`/ngày làm khóa nối.
- Nếu nâng cấp trả phí, endpoint `/matches/{id}` có thêm chi tiết (odds, referees đầy đủ) — schema silver ở trên đã chừa sẵn chỗ để mở rộng không cần đổi cấu trúc.
