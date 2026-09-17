# 18 — FBref (Sports Reference) → MinIO

**Kiểu ingest:** Web scraping (HTML table), không cần key/auth, **có rate-limit mềm bắt buộc tôn trọng**
**Tần suất:** hàng tuần (stats mùa giải không đổi nhanh từng ngày)
**Độ khó:** ★★★☆☆ — bảng dễ đọc, nhưng có 1 "bẫy" kỹ thuật đặc trưng của site cần xử lý riêng

---

## 1. Vì sao chọn nguồn này

FBref (thuộc Sports Reference / StatsBomb) là nguồn **stats chi tiết nhất có thể scrape miễn phí**: không chỉ goal/assist mà còn xG, xAG, progressive passes/carries, tackles, pressures, touches theo từng khu vực sân — gần với độ sâu của StatsBomb (file `04`) nhưng **cập nhật theo mùa hiện tại**, trong khi StatsBomb open data chỉ có mùa cũ. Đây là mảnh ghép còn thiếu giữa "FPL/API-Football nông nhưng mới" và "StatsBomb sâu nhưng cũ".

FBref công khai ghi rõ khuyến khích lấy dữ liệu qua công cụ như `pandas.read_html`, chỉ yêu cầu **tôn trọng rate-limit** (không dùng thư viện chuyên dụng như `soccerdata` nếu không cần, nhưng bắt buộc giãn cách request). Site chủ động cho scrape ở mức độ vừa phải — khác hẳn Transfermarkt (file `17`).

## 2. "Bẫy" quan trọng cần biết trước

FBref **ẩn phần lớn các bảng nâng cao (advanced stats) bên trong HTML comment** (`<!-- ... -->`) để tránh bị scrape trực tiếp bằng `pd.read_html` thông thường. Bảng chính (Standard Stats) thì hiện bình thường, nhưng các bảng như Shooting, Passing, GCA, Defense, Possession... **nằm trong comment và `pd.read_html` sẽ bỏ qua hoàn toàn** nếu không xử lý.

Cách xử lý chuẩn: dùng `BeautifulSoup` tìm mọi node `Comment`, decode nội dung bên trong, rồi mới `pd.read_html` trên chuỗi đó.

## 3. URL và cấu trúc

Base: `https://fbref.com/en/comps/9/{stat_type}/Premier-League-Stats` (mã `9` = Premier League)

| stat_type | Nội dung |
|---|---|
| (trống, trang gốc) | Standard Stats — goal, assist, xG, xAG cơ bản |
| `shooting/` | Sút, sút trúng đích, xG per shot |
| `passing/` | Passing accuracy, progressive passes, key passes |
| `gca/` | Goal & Shot Creating Actions |
| `defense/` | Tackles, interceptions, blocks, pressures |
| `possession/` | Touches theo khu vực sân, carries, take-ons |
| `misc/` | Thẻ, phạm lỗi, tranh chấp không chiến |

Mỗi trang chứa **2 bảng chính**: bảng "Squad Stats" (theo đội) hiện bình thường, và bảng "Player Stats" (theo cầu thủ) — bảng cầu thủ với stat nâng cao thường nằm trong comment như mô tả ở mục 2.

## 4. Layout trong lake

```
bronze/fbref/stats/stat_type=standard/season=2025-2026/ingest_date=2026-09-16/page.html
bronze/fbref/stats/stat_type=shooting/season=2025-2026/ingest_date=2026-09-16/page.html
...

silver/players/fbref_player_stats/stat_type=standard/season=2025-2026/part-0.parquet
silver/players/fbref_player_stats/stat_type=shooting/season=2025-2026/part-0.parquet
silver/teams/fbref_squad_stats/stat_type=standard/season=2025-2026/part-0.parquet
```

## 5. Script ingest — `pipelines/p18_fbref.py`

```python
"""Ingest FBref: standard + advanced player/squad stats -> MinIO."""
import re
import time
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup, Comment
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import SESSION

SRC = "fbref"
D = today()
SEASON = "2025-2026"
LEAGUE_ID = 9              # Premier League

STAT_TYPES = {
    "standard": "",
    "shooting": "shooting/",
    "passing": "passing/",
    "gca": "gca/",
    "defense": "defense/",
    "possession": "possession/",
    "misc": "misc/",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; football-lake/0.1; "
                         "educational project)"}
MIN_DELAY_SEC = 6.0         # FBref khuyến nghị giãn cách vài giây/request


def fetch_page(path: str) -> str:
    url = f"https://fbref.com/en/comps/{LEAGUE_ID}/{path}Premier-League-Stats"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(MIN_DELAY_SEC)
    return r.text


def extract_all_tables(html: str) -> list:
    """
    Bảng nâng cao của FBref nằm trong HTML comment để né scraper đơn giản.
    Ta lấy mọi <table> hiện trực tiếp + mọi <table> ẩn trong comment.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = []

    # bảng hiện trực tiếp
    for t in soup.find_all("table"):
        tables.append(str(t))

    # bảng ẩn trong comment
    comments = soup.find_all(string=lambda s: isinstance(s, Comment))
    for c in comments:
        if "<table" in c:
            inner = BeautifulSoup(c, "html.parser")
            for t in inner.find_all("table"):
                tables.append(str(t))

    dfs = []
    for html_table in tables:
        try:
            df = pd.read_html(StringIO(html_table))[0]
            dfs.append(df)
        except ValueError:
            continue
    return dfs


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """FBref dùng multi-index cột (nhóm 'Performance', 'Expected'...) -> gộp phẳng."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in col if "Unnamed" not in c]).strip("_")
                     for col in df.columns]
    # loại dòng header lặp lại giữa bảng (FBref chèn header phụ mỗi ~25 dòng)
    if "Rk" in df.columns:
        df = df[df["Rk"] != "Rk"]
    return df


# ---------- BRONZE + SILVER ----------
def ingest_stat_type(name: str, path: str):
    print(f"  · {name}...")
    html = fetch_page(path)
    put_bytes(
        f"bronze/fbref/stats/stat_type={name}/season={SEASON}"
        f"/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")

    tables = extract_all_tables(html)
    if not tables:
        print(f"    ! không tìm thấy bảng nào cho {name}")
        return

    # bảng lớn nhất theo số dòng thường là Player Stats (nhiều cầu thủ hơn đội)
    tables_sorted = sorted(tables, key=len, reverse=True)
    player_df = clean_columns(tables_sorted[0])
    player_df["stat_type"] = name
    player_df["season"] = SEASON
    player_df["ingest_date"] = D

    put_parquet(
        f"silver/players/fbref_player_stats/stat_type={name}"
        f"/season={SEASON}/part-0.parquet",
        player_df, SRC, meta={"rows": len(player_df)})

    # bảng nhỏ hơn (20 dòng ~ số đội) là Squad Stats, nếu tồn tại
    squad_candidates = [t for t in tables if 15 <= len(t) <= 25]
    if squad_candidates:
        squad_df = clean_columns(squad_candidates[0])
        squad_df["stat_type"] = name
        squad_df["season"] = SEASON
        put_parquet(
            f"silver/teams/fbref_squad_stats/stat_type={name}"
            f"/season={SEASON}/part-0.parquet",
            squad_df, SRC, meta={"rows": len(squad_df)})

    print(f"    ✓ {len(player_df)} dòng cầu thủ")


if __name__ == "__main__":
    print(f"[1/1] FBref: {len(STAT_TYPES)} loại stats")
    for name, path in STAT_TYPES.items():
        ingest_stat_type(name, path)

    summary("bronze/fbref/")
    summary("silver/players/fbref_player_stats/")
```

Chạy:

```bash
python -m pipelines.p18_fbref
```

Lưu ý thời gian chạy: 7 loại stats × ~6 giây giãn cách = **ít nhất 42 giây**, chưa kể thời gian tải trang — chạy 1 lần mất khoảng 2-3 phút, hoàn toàn chấp nhận được cho tần suất hàng tuần.

## 6. Kết quả mong đợi

```
[1/1] FBref: 7 loại stats
  · standard...
  ✓ s3://football-lake/bronze/fbref/stats/stat_type=standard/season=2025-2026/ingest_date=2026-09-16/page.html  (312,884 B, ...)
    ✓ 580 dòng cầu thủ
  · shooting...
    ✓ 580 dòng cầu thủ
  · passing...
    ✓ 580 dòng cầu thủ
  ...

[summary] bronze/fbref/: 7 objects, 2.1 MB
[summary] silver/players/fbref_player_stats/: 7 objects, 0.9 MB
```

580 dòng ≈ số cầu thủ đã ra sân ít nhất 1 phút trong mùa, tính cả cầu thủ đã chuyển đi/đến giữa mùa.

## 7. Truy vấn kiểm chứng

```sql
-- Top 10 cầu thủ có xG cao nhất nhưng ghi bàn ít hơn kỳ vọng (underperform)
SELECT Player, Squad,
       CAST(Performance_Gls AS DOUBLE) AS goals,
       CAST(Expected_xG AS DOUBLE) AS xg,
       CAST(Performance_Gls AS DOUBLE) - CAST(Expected_xG AS DOUBLE) AS chenh_lech
FROM read_parquet('s3://football-lake/silver/players/fbref_player_stats/stat_type=standard/**/*.parquet')
WHERE CAST(Expected_xG AS DOUBLE) > 3
ORDER BY chenh_lech ASC
LIMIT 10;

-- ĐỐI CHIẾU: xG của FBref vs FPL expected_goals (file 01) cho cùng cầu thủ
-- (cần bảng ánh xạ tên trước khi join thật, đây là minh họa concept)
SELECT f.Player, f.Squad,
       CAST(f.Expected_xG AS DOUBLE) AS fbref_xg
FROM read_parquet('s3://football-lake/silver/players/fbref_player_stats/stat_type=standard/**/*.parquet') f
ORDER BY fbref_xg DESC LIMIT 10;
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Bảng advanced stats bị thiếu/rỗng | Quên xử lý bảng nằm trong HTML comment | Dùng đúng `extract_all_tables()` như trong script, không chỉ gọi `pd.read_html` trực tiếp trên response |
| `429 Too Many Requests` | Gọi quá nhanh, vượt rate-limit mềm của site | Tăng `MIN_DELAY_SEC` lên 10-15s; **không** chạy song song nhiều request |
| Cột tên lạ kiểu `('Unnamed: 0_level_0', 'Rk')` | Multi-index header của FBref chưa được gộp phẳng | Dùng `clean_columns()`; nếu vẫn lỗi, in `df.columns.tolist()` ra để map thủ công |
| Dòng dữ liệu lặp lại giá trị `Rk` = "Rk" | FBref chèn dòng header phụ giữa bảng dài (~mỗi 25 dòng) để dễ đọc bằng mắt | Đã lọc trong `clean_columns()`: `df[df["Rk"] != "Rk"]` |
| Số cầu thủ khác giữa các `stat_type` | Một số stat (vd: GCA) chỉ tính cho cầu thủ có hành động liên quan | Bình thường — không phải lỗi, chấp nhận NaN khi join |

## 9. Mở rộng

- Thêm `match_logs` theo từng cầu thủ (URL dạng `fbref.com/en/players/{id}/matchlogs/{season}/...`) nếu cần chi tiết theo từng trận thay vì tổng mùa — đã thấy cấu trúc này khi kiểm tra trang mẫu, tương tự cách file `01` (FPL) có `element-summary` theo GW.
- Join `fbref_player_stats` với `fpl_player_dim` (file `01`) và `af_match_stats` (file `02`) qua tên cầu thủ đã chuẩn hóa (bảng alias như file `03` đã làm cho tên đội) → dựng 1 bảng "player stats consensus" từ 3 nguồn độc lập.
- FBref cũng có dữ liệu **các giải khác** (đổi `LEAGUE_ID`: La Liga = 12, Serie A = 11, Bundesliga = 20, Ligue 1 = 13) — mở rộng lake ra ngoài Premier League gần như miễn phí về công sức code.
