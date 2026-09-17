# 19 — Understat (xG/xA nâng cao) → MinIO

**Kiểu ingest:** Web scraping JSON nhúng trong HTML (không cần key), site chủ động "thân thiện" với scraper
**Tần suất:** hàng tuần, hoặc sau mỗi vòng đấu
**Độ khó:** ★☆☆☆☆ — dễ nhất trong các nguồn scrape của bộ (`16`, `17`, `18`)

---

## 1. Vì sao chọn nguồn này

Understat là nguồn xG/xA nổi tiếng trong cộng đồng phân tích bóng đá, đi kèm các chỉ số nâng cao mà FBref (file `18`) không có: **xGChain** (đóng góp xG trong cả pha bóng dẫn tới cơ hội) và **xGBuildup** (đóng góp không tính cú sút/đường chuyền cuối). Đây là 2 chỉ số hay dùng để đánh giá cầu thủ kiến tạo lối chơi (thường là tiền vệ trung tâm) — nhóm cầu thủ mà xG/xA thông thường đánh giá thấp.

Điểm đặc biệt: **Understat nhúng sẵn toàn bộ dữ liệu dạng JSON ngay trong thẻ `<script>` của trang HTML** (biến `teamsData`, `playersData`, `datesData`) — không cần parse bảng HTML như FBref/PhysioRoom, chỉ cần trích JSON ra là có dữ liệu sạch, ít lỗi vặt hơn hẳn. Site cũng công khai có endpoint XHR nội bộ (`/getLeagueData/...`) trả JSON gzip trực tiếp — nhanh và ổn định hơn parse HTML.

## 2. Hai cách lấy dữ liệu

**Cách A — endpoint dữ liệu (khuyến nghị, ổn định hơn):**

```
GET https://understat.com/getLeagueData/{league}/{season}
```

`league` ∈ `{EPL, La_liga, Bundesliga, Serie_A, Ligue_1, RFPL}`, `season` là năm bắt đầu mùa (vd `2025` cho mùa 2025/26). Trả về JSON gzip chứa dữ liệu tổng hợp theo đội.

**Cách B — trích JSON nhúng trong trang (đầy đủ hơn, có dữ liệu cầu thủ):**

```
GET https://understat.com/league/{league}/{season}
```

Trang HTML chứa các biến JS:
- `teamsData` — dữ liệu từng đội theo từng trận (xG, xGA, kết quả)
- `playersData` — dữ liệu từng cầu thủ cả mùa (goals, xG, xA, xGChain, xGBuildup, shots, key_passes)
- `datesData` — lịch thi đấu kèm xG dự đoán

Chuỗi JSON nằm giữa `JSON.parse('...')` trong `<script>`, cần strip ký tự escape (`\x3C` v.v.) trước khi `json.loads`.

## 3. Layout trong lake

```
bronze/understat/players/league=EPL/season=2025/ingest_date=2026-09-16/players.json.gz
bronze/understat/teams/league=EPL/season=2025/ingest_date=2026-09-16/teams.json.gz
bronze/understat/page_raw/league=EPL/season=2025/ingest_date=2026-09-16/page.html   ← lưu HTML gốc để debug

silver/players/understat_player_xg/league=EPL/season=2025/part-0.parquet
silver/teams/understat_team_xg/league=EPL/season=2025/part-0.parquet
```

## 4. Script ingest — `pipelines/p19_understat.py`

```python
"""Ingest Understat: xG/xA nâng cao qua JSON nhúng trong trang -> MinIO."""
import json
import re
import time
import pandas as pd
from lake.minio_io import put_bytes, put_json_gz, put_parquet, today, summary
from lake.http import SESSION

SRC = "understat"
D = today()
LEAGUE = "EPL"
SEASON = "2025"             # năm bắt đầu mùa 2025/26

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; football-lake/0.1; "
                         "educational project)"}


def fetch_league_page() -> str:
    url = f"https://understat.com/league/{LEAGUE}/{SEASON}"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(1.0)
    return r.text


def extract_js_variable(html: str, var_name: str) -> dict:
    """
    Understat nhúng data dạng:
      var playersData = JSON.parse('...chuỗi hex-escaped...');
    Cần lấy đúng chuỗi trong dấu nháy đơn, decode escape, rồi json.loads.
    """
    pattern = rf"var {var_name}\s*=\s*JSON\.parse\('(.+?)'\);"
    m = re.search(pattern, html)
    if not m:
        raise ValueError(f"Không tìm thấy biến {var_name} trong trang — "
                         f"site có thể đã đổi cấu trúc")
    raw = m.group(1)
    # chuỗi được escape kiểu \x7B... -> decode qua 'unicode_escape'
    decoded = raw.encode("utf-8").decode("unicode_escape")
    return json.loads(decoded)


# ---------- BRONZE ----------
def ingest_all() -> tuple:
    html = fetch_league_page()
    put_bytes(
        f"bronze/understat/page_raw/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")

    players = extract_js_variable(html, "playersData")
    teams = extract_js_variable(html, "teamsData")

    put_json_gz(
        f"bronze/understat/players/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/players.json.gz",
        players, SRC, meta={"count": len(players)})
    put_json_gz(
        f"bronze/understat/teams/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/teams.json.gz",
        teams, SRC, meta={"count": len(teams)})

    print(f"  ✓ {len(players)} cầu thủ, {len(teams)} đội")
    return players, teams


# ---------- SILVER ----------
def build_player_xg(players: list) -> pd.DataFrame:
    df = pd.DataFrame(players)
    numeric_cols = ["games", "time", "goals", "xG", "assists", "xA",
                    "shots", "key_passes", "yellow_cards", "red_cards",
                    "xGChain", "xGBuildup"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["xg_per90"] = df["xG"] / (df["time"] / 90).replace(0, pd.NA)
    df["goals_minus_xg"] = df["goals"] - df["xG"]   # dương = vượt kỳ vọng
    df["league"] = LEAGUE
    df["season"] = SEASON
    df["ingest_date"] = D

    put_parquet(
        f"silver/players/understat_player_xg/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


def build_team_xg(teams: dict) -> pd.DataFrame:
    """
    teamsData là dict {team_id: {title, history: [match1, match2, ...]}}.
    Ta gộp lại thành fact table 1 dòng/trận/đội.
    """
    rows = []
    for team_id, info in teams.items():
        title = info.get("title")
        for match in info.get("history", []):
            rows.append({
                "team_id": team_id, "team_name": title,
                "date": match.get("date"), "h_a": match.get("h_a"),
                "xG": match.get("xG"), "xGA": match.get("xGA"),
                "npxG": match.get("npxG"), "npxGA": match.get("npxGA"),
                "result": match.get("result"), "ppda": match.get("ppda"),
                "deep": match.get("deep"), "scored": match.get("scored"),
                "missed": match.get("missed"),
            })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["league"] = LEAGUE
    df["season"] = SEASON

    put_parquet(
        f"silver/teams/understat_team_xg/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


if __name__ == "__main__":
    print("[1/2] tải trang + trích JSON")
    players, teams = ingest_all()

    print("[2/2] silver")
    pdf = build_player_xg(players)
    tdf = build_team_xg(teams)
    print(f"  ✓ player_xg: {len(pdf)} dòng | team_xg: {len(tdf)} dòng (theo trận)")

    summary("bronze/understat/")
    summary("silver/players/understat_player_xg/")
```

Chạy:

```bash
python -m pipelines.p19_understat
```

## 5. Kết quả mong đợi

```
[1/2] tải trang + trích JSON
  ✓ s3://football-lake/bronze/understat/page_raw/league=EPL/season=2025/ingest_date=2026-09-16/page.html  (890,112 B, ...)
  ✓ .../players.json.gz  (48,204 B, ...)
  ✓ .../teams.json.gz  (12,884 B, ...)
  ✓ 412 cầu thủ, 20 đội
[2/2] silver
  ✓ player_xg: 412 dòng | team_xg: 80 dòng (theo trận)

[summary] bronze/understat/: 3 objects, 0.95 MB
[summary] silver/players/understat_player_xg/: 1 objects, 0.06 MB
```

## 6. Truy vấn kiểm chứng

```sql
-- Cầu thủ có xGChain cao nhất (tham gia nhiều pha bóng nguy hiểm nhất,
-- không nhất thiết là người ghi bàn/kiến tạo cuối cùng)
SELECT player_name, team_title, xGChain, xGBuildup, goals, assists
FROM read_parquet('s3://football-lake/silver/players/understat_player_xg/**/*.parquet')
ORDER BY xGChain DESC LIMIT 15;

-- Đội nào ép sân mạnh nhất (PPDA thấp = pressing nhiều)?
SELECT team_name, ROUND(AVG(ppda), 2) AS ppda_tb,
       ROUND(AVG(xG), 2) AS xg_tb, ROUND(AVG(xGA), 2) AS xga_tb
FROM read_parquet('s3://football-lake/silver/teams/understat_team_xg/**/*.parquet')
GROUP BY team_name ORDER BY ppda_tb;

-- ĐỐI CHIẾU: xG của Understat vs FBref (file 18) cho cùng cầu thủ —
-- 2 mô hình độc lập tính xG khác nhau, chênh lệch lớn đáng chú ý
SELECT u.player_name, u.xG AS understat_xg
FROM read_parquet('s3://football-lake/silver/players/understat_player_xg/**/*.parquet') u
ORDER BY u.xG DESC LIMIT 10;
```

Truy vấn cuối là ví dụ đẹp về **2 mô hình xG độc lập** (Understat dùng neural network riêng, FBref/StatsBomb dùng mô hình khác) — chênh lệch lớn giữa 2 nguồn cho cùng 1 cầu thủ là tín hiệu thú vị để phân tích thêm, không phải lỗi dữ liệu.

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `ValueError: Không tìm thấy biến playersData` | Understat đổi tên biến JS hoặc cấu trúc script | Mở `page.html` đã lưu ở bronze, tìm thủ công bằng Ctrl+F `JSON.parse`, cập nhật lại `pattern` trong `extract_js_variable` |
| `json.loads` lỗi `Expecting value` | Chuỗi escape decode sai (ký tự đặc biệt trong tên cầu thủ có dấu) | Thử `decoded = raw.encode().decode('unicode_escape').encode('latin1').decode('utf-8')` — vấn đề double-encoding phổ biến với JSON.parse kiểu này |
| Số cầu thủ ít hơn kỳ vọng | Understat chỉ tính cầu thủ đã ra sân ≥1 phút trong mùa được chọn | Bình thường, không phải lỗi |
| `ppda` = null cho một số trận | Trận đó có 0 defensive action ghi nhận (hiếm, thường lỗi ghi nhận từ nguồn gốc của Understat) | Giữ null, không suy diễn |
| Trùng dữ liệu khi chạy nhiều lần/ngày | Mỗi lần chạy tạo `ingest_date` mới dù xG chưa đổi | Có thể thêm check "nếu tổng xG hôm nay == hôm qua thì bỏ qua ghi silver" để tiết kiệm, không bắt buộc |

## 8. Mở rộng

- Đổi `LEAGUE` sang `La_liga`, `Bundesliga`, `Serie_A`, `Ligue_1` để mở rộng ra ngoài Premier League — cùng 1 script, chỉ đổi tham số, giống mở rộng đã gợi ý ở file `18`.
- Lấy thêm `datesData` (lịch thi đấu kèm xG dự đoán trước trận) — hữu ích để so sánh **xG dự đoán trước trận** vs **xG thực tế sau trận**, một bài toán calibration model thú vị.
- Ghép `understat_team_xg` (theo trận) với `fd_matches` (file `03`, có odds) → kiểm tra giả thuyết "thị trường cược có định giá đúng theo xG không, hay có độ trễ".
