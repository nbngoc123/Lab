# 19 — Understat (xG/xA nâng cao, qua thư viện cộng đồng) → MinIO

**Kiểu ingest:** Gọi qua thư viện Python bên thứ 3 (`jeke-understat-scrapper`, fork của `understatapi`), không tự viết scraper
**Tần suất:** hàng tuần, hoặc sau mỗi vòng đấu
**Độ khó:** ★★☆☆☆ — dễ về code, nhưng phụ thuộc vào thư viện bên ngoài còn được maintain hay không

---

## 1. Đổi hướng so với bản nháp trước — vì sao

Bản đầu tôi viết cho nguồn này là tự viết scraper (`requests` + regex trích JSON từ `<script>`). Khi kiểm tra thực tế, phát hiện **`understat.com` có `robots.txt` chặn truy cập tự động** — công cụ fetch của tôi bị từ chối thẳng với lỗi `ROBOTS_DISALLOWED`. Đây là khác biệt quan trọng so với PhysioRoom (file `16`) và FBref (file `18`), cả hai đều cho phép fetch bình thường.

Bạn đã chọn hướng: **dùng thư viện cộng đồng có sẵn thay vì tự viết scraper**. Cách này không loại bỏ hoàn toàn việc "vi phạm robots.txt về mặt kỹ thuật" — bản chất vẫn là lấy dữ liệu từ trang mà robots.txt không cho phép bot — nhưng có 2 điểm khác biệt đáng cân nhắc: (1) đây là việc **cộng đồng phân tích bóng đá đã làm công khai nhiều năm**, nhiều bài báo/nghiên cứu học thuật dùng Understat qua đúng các thư viện này; (2) bạn không tự viết logic né tránh chặn bot, chỉ dùng lại công cụ đã có sẵn — trách nhiệm và rủi ro nằm ở việc bạn hiểu rõ và chấp nhận, không phải ở việc tôi giúp bạn lách kỹ thuật.

**Khuyến nghị cá nhân của tôi:** vẫn nên giữ tần suất thấp (hàng tuần, không phải hàng ngày) và không dùng ở quy mô thương mại — giống tinh thần đã áp dụng cho Transfermarkt (file `17`).

## 2. Thư viện dùng

```bash
pip install jeke-understat-scrapper
```

Đây là **fork được maintain** của `understatapi` gốc (`pip install understatapi` — dự án gốc có dấu hiệu không còn cập nhật kịp khi Understat đổi sang tải dữ liệu kiểu AJAX). Fork này giữ **cùng namespace import** để tương thích ngược:

```python
from understatapi import UnderstatClient   # đúng, dù cài package tên "jeke-understat-scrapper"
```

**Lưu ý bắt buộc:** đây là package bên thứ 3, không phải của Understat, method signature có thể đổi giữa các phiên bản. Sau khi cài, kiểm tra lại bằng `help(UnderstatClient)` hoặc đọc README trên PyPI của đúng phiên bản bạn cài trước khi chạy pipeline thật — script dưới đây dựa trên API đã xác nhận qua tài liệu công khai tại thời điểm viết (09/2026), nhưng nên coi là điểm khởi đầu, không phải cam kết tuyệt đối.

## 3. Các "endpoint" (method) sẽ dùng

Thư viện tổ chức theo đúng cấu trúc trang Understat:

| Method | Tương đương trang | Dữ liệu |
|---|---|---|
| `UnderstatClient().league(league="EPL").get_player_data(season=...)` | `/league/EPL/{season}` | Toàn bộ cầu thủ cả mùa: goals, xG, xA, xGChain, xGBuildup... |
| `UnderstatClient().league(league="EPL").get_team_data(season=...)` | `/league/EPL/{season}` | Dữ liệu từng đội theo từng trận (xG, xGA, ppda) |
| `UnderstatClient().league(league="EPL").get_match_data(season=...)` | `/league/EPL/{season}` | Lịch thi đấu kèm xG dự đoán mỗi trận (`datesData`) |
| `UnderstatClient().match(match=match_id).get_shot_data()` | `/match/{id}` | **Từng cú sút kèm tọa độ x/y trên sân** — chỉ nguồn này trong cả bộ 19 file có |

`league` nhận giá trị: `EPL`, `La_liga`, `Bundesliga`, `Serie_A`, `Ligue_1`, `RFPL`. `season` là năm bắt đầu mùa (`2025` cho mùa 2025/26).

Field cầu thủ đã xác nhận đầy đủ (bao gồm `npg` — non-penalty goals mà bản nháp trước tôi bỏ sót):

```json
{"id": "1740", "player_name": "...", "games": "27", "time": "2293",
 "goals": "11", "xG": "13.36...", "assists": "9", "xA": "4.06...",
 "shots": "87", "key_passes": "40", "yellow_cards": "5", "red_cards": "0",
 "position": "M S", "team_title": "...", "npg": "6", "npxG": "7.27...",
 "xGChain": "17.38...", "xGBuildup": "8.96..."}
```

## 4. Layout trong lake

```
bronze/understat/players/league=EPL/season=2025/ingest_date=2026-09-16/players.json.gz
bronze/understat/teams/league=EPL/season=2025/ingest_date=2026-09-16/teams.json.gz
bronze/understat/matches/league=EPL/season=2025/ingest_date=2026-09-16/matches.json.gz
bronze/understat/shots/league=EPL/season=2025/match_id=27124/shots.json.gz    ← tùy chọn, tốn nhiều lời gọi

silver/players/understat_player_xg/league=EPL/season=2025/part-0.parquet
silver/teams/understat_team_xg/league=EPL/season=2025/part-0.parquet
silver/matches/understat_match_xg/league=EPL/season=2025/part-0.parquet
silver/events/understat_shots/league=EPL/season=2025/part-0.parquet          ← tùy chọn
```

## 5. Script ingest — `pipelines/p19_understat.py`

```python
"""
Ingest Understat qua thư viện cộng đồng jeke-understat-scrapper (import
name: understatapi) -> MinIO.

LƯU Ý: understat.com chặn bot qua robots.txt. Việc dùng thư viện này vẫn
là lấy dữ liệu từ trang không cho phép truy cập tự động — quyết định dùng
đã được cân nhắc, giữ tần suất thấp (hàng tuần) và không dùng thương mại.
"""
import time
import pandas as pd
from understatapi import UnderstatClient
from lake.minio_io import put_json_gz, put_parquet, today, summary

SRC = "understat"
D = today()
LEAGUE = "EPL"
SEASON = "2025"

client = UnderstatClient()


# ---------- BRONZE ----------
def ingest_players() -> list:
    data = client.league(league=LEAGUE).get_player_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/players/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/players.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_teams() -> dict:
    data = client.league(league=LEAGUE).get_team_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/teams/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/teams.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_matches() -> list:
    data = client.league(league=LEAGUE).get_match_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/matches/league={LEAGUE}/season={SEASON}"
        f"/ingest_date={D}/matches.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_shots_for_matches(match_ids: list, max_matches=20, sleep=1.5):
    """
    Tùy chọn, TỐN NHIỀU LỜI GỌI (1 request/trận) — mặc định giới hạn 20
    trận/lần chạy để không lạm dụng nguồn đã bị robots.txt chặn. Tăng dần
    theo thời gian (mỗi tuần thêm 20 trận) thay vì lấy hết 1 lần.
    """
    shots_all = []
    for mid in match_ids[:max_matches]:
        try:
            shots = client.match(match=str(mid)).get_shot_data()
        except Exception as e:
            print(f"  ! match {mid}: lỗi {e}")
            continue
        put_json_gz(
            f"bronze/understat/shots/league={LEAGUE}/season={SEASON}"
            f"/match_id={mid}/shots.json.gz",
            shots, SRC, meta={"match_id": mid})
        shots_all.append((mid, shots))
        time.sleep(sleep)
    print(f"  ✓ lấy shot data cho {len(shots_all)}/{len(match_ids[:max_matches])} trận")
    return shots_all


# ---------- SILVER ----------
def build_player_xg(players: list) -> pd.DataFrame:
    df = pd.DataFrame(players)
    numeric_cols = ["games", "time", "goals", "xG", "assists", "xA", "shots",
                    "key_passes", "yellow_cards", "red_cards", "npg",
                    "npxG", "xGChain", "xGBuildup"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["xg_per90"] = df["xG"] / (df["time"] / 90).replace(0, pd.NA)
    df["goals_minus_xg"] = df["goals"] - df["xG"]
    df["league"], df["season"], df["ingest_date"] = LEAGUE, SEASON, D

    put_parquet(
        f"silver/players/understat_player_xg/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


def build_team_xg(teams: dict) -> pd.DataFrame:
    """teams là dict {team_id: {title, history: [match1, ...]}}."""
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
                "scored": match.get("scored"), "missed": match.get("missed"),
            })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["league"], df["season"] = LEAGUE, SEASON
    put_parquet(
        f"silver/teams/understat_team_xg/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


def build_match_xg(matches: list) -> pd.DataFrame:
    """datesData: lịch thi đấu kèm xG dự đoán mỗi trận."""
    df = pd.DataFrame(matches)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["league"], df["season"] = LEAGUE, SEASON
    put_parquet(
        f"silver/matches/understat_match_xg/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


def build_shots(shots_all: list) -> pd.DataFrame:
    """shotsData thường có dạng {'h': [...], 'a': [...]} theo đội nhà/khách."""
    rows = []
    for mid, shots in shots_all:
        for side in ("h", "a"):
            for s in shots.get(side, []):
                s = dict(s)
                s["match_id"] = mid
                s["side"] = side
                rows.append(s)
    df = pd.DataFrame(rows)
    if df.empty:
        print("  ! không có shot data nào để build")
        return df
    for c in ["X", "Y", "xG", "minute"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["league"], df["season"] = LEAGUE, SEASON
    put_parquet(
        f"silver/events/understat_shots/league={LEAGUE}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


if __name__ == "__main__":
    print("[1/4] players")
    players = ingest_players()
    print(f"  ✓ {len(players)} cầu thủ")

    print("[2/4] teams")
    teams = ingest_teams()
    print(f"  ✓ {len(teams)} đội")

    print("[3/4] matches (datesData)")
    matches = ingest_matches()
    print(f"  ✓ {len(matches)} trận")

    print("[4/4] silver")
    build_player_xg(players)
    build_team_xg(teams)
    build_match_xg(matches)

    # Shot-level: TÙY CHỌN, comment sẵn vì tốn nhiều request tới nguồn
    # đã bị robots.txt chặn — chỉ bật khi thực sự cần shot map.
    # finished_ids = [m["id"] for m in matches if m.get("isResult")]
    # shots_all = ingest_shots_for_matches(finished_ids, max_matches=20)
    # build_shots(shots_all)

    summary("bronze/understat/")
    summary("silver/players/understat_player_xg/")
```

Chạy:

```bash
python -m pipelines.p19_understat
```

## 6. Kết quả mong đợi

```
[1/4] players
  ✓ s3://football-lake/bronze/understat/players/league=EPL/season=2025/ingest_date=2026-09-16/players.json.gz  (48,204 B, ...)
  ✓ 412 cầu thủ
[2/4] teams
  ✓ .../teams.json.gz  (12,884 B, ...)
  ✓ 20 đội
[3/4] matches (datesData)
  ✓ .../matches.json.gz  (9,112 B, ...)
  ✓ 80 trận
[4/4] silver
  ✓ .../understat_player_xg/.../part-0.parquet  (rows: 412)
  ✓ .../understat_team_xg/.../part-0.parquet  (rows: 80)
  ✓ .../understat_match_xg/.../part-0.parquet  (rows: 80)

[summary] bronze/understat/: 3 objects, 0.9 MB
[summary] silver/players/understat_player_xg/: 1 objects, 0.06 MB
```

## 7. Truy vấn kiểm chứng

```sql
-- Cầu thủ có xGChain cao nhất (đóng góp nhiều pha bóng nguy hiểm nhất)
SELECT player_name, team_title, xGChain, xGBuildup, goals, assists
FROM read_parquet('s3://football-lake/silver/players/understat_player_xg/**/*.parquet')
ORDER BY xGChain DESC LIMIT 15;

-- Đội ép sân mạnh nhất (PPDA thấp = pressing nhiều)
SELECT team_name, ROUND(AVG(ppda), 2) AS ppda_tb,
       ROUND(AVG(xG), 2) AS xg_tb, ROUND(AVG(xGA), 2) AS xga_tb
FROM read_parquet('s3://football-lake/silver/teams/understat_team_xg/**/*.parquet')
GROUP BY team_name ORDER BY ppda_tb;

-- (nếu bật shot-level) Bản đồ sút của 1 cầu thủ
SELECT X, Y, xG, minute, result
FROM read_parquet('s3://football-lake/silver/events/understat_shots/**/*.parquet')
WHERE player = 'Erling Haaland';
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `ModuleNotFoundError: understatapi` sau khi `pip install jeke-understat-scrapper` | Tên package (pip) khác tên module (import) — đây là chủ đích của fork để tương thích ngược | Đảm bảo dùng đúng `from understatapi import UnderstatClient`, không phải `from jeke_understat_scrapper import ...` |
| `AttributeError: 'League' object has no attribute 'get_match_data'` | Phiên bản thư viện bạn cài có method signature khác bản tôi tham khảo | Chạy `dir(client.league(league="EPL"))` để liệt kê method thật có, cập nhật lại tên gọi |
| Toàn bộ request trả lỗi/timeout | Understat có thể đã tăng cường chặn bot hơn nữa (họ có quyền làm vậy vì đã ghi rõ trong robots.txt) | Đây là rủi ro cố hữu đã cảnh báo ở mục 1 — không có cách khắc phục "đúng", chỉ có thể giảm tần suất hoặc dừng dùng nguồn này |
| Số cầu thủ/trận ít hơn kỳ vọng | Bình thường — Understat chỉ tính cầu thủ đã ra sân ≥1 phút, trận đã đá xong mới có `isResult=true` | Không phải lỗi |
| `shots.json.gz` rỗng cho 1 số `match_id` | Trận chưa đá hoặc chưa có shot data ghi nhận | Lọc `isResult` trước khi gọi `get_shot_data`, như code mẫu đã làm |

## 9. Mở rộng

- Đổi `LEAGUE` sang `La_liga`, `Bundesliga`, `Serie_A`, `Ligue_1` để mở rộng ra ngoài Premier League.
- Ghép `understat_shots` (nếu bật) với `af_match_events` (file `02`, có phút ghi bàn từ API-Football) → đối chiếu 2 nguồn độc lập cho cùng 1 bàn thắng.
- Vì đây là nguồn có rủi ro dừng hoạt động cao nhất trong cả bộ 19 file (robots.txt + phụ thuộc thư viện bên thứ 3), nên **thiết kế mọi query/dashboard downstream để không phụ thuộc cứng vào nguồn này** — coi `understat_player_xg` là "nice-to-have" đối chiếu với xG của FBref (file `18`), không phải nguồn duy nhất cho biến xG trong model ML sau này.