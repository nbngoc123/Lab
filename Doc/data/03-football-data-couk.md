# 03 — Football-Data.co.uk (CSV batch) → MinIO

**Kiểu ingest:** Flat file CSV, batch nhiều mùa, URL có quy luật
**Tần suất:** 1 lần cho lịch sử + hàng tuần cho mùa hiện tại
**Độ khó:** ★★☆☆☆ — dễ code, khó ở chỗ **schema thay đổi theo năm**

---

## 1. Vì sao chọn nguồn này

Đây là nguồn cho bạn **chiều sâu lịch sử**: 30+ mùa Premier League, mỗi trận có kết quả, thống kê (sút, phạt góc, thẻ) và **tỷ lệ cược của 10+ nhà cái**. FPL/API-Football chỉ có mùa hiện tại. Nguồn này biến lake của bạn từ "snapshot" thành "time series".

Giá trị dạy học lớn nhất: **schema drift**. File mùa 1993 có 10 cột, mùa 2024 có 100+ cột. Xử lý được chuyện này là biết làm data engineering thật.

## 2. Quy luật URL

```
https://www.football-data.co.uk/mmz4281/{SEASON}/{DIV}.csv
```

- `SEASON`: 4 chữ số, ví dụ `2425` = mùa 2024/25, `9394` = mùa 1993/94
- `DIV`: `E0` (Premier League), `E1` (Championship), `E2`, `E3`, `EC` (Conference), `SC0` (Scottish Prem), `D1` (Bundesliga), `SP1` (La Liga), `I1` (Serie A), `F1` (Ligue 1)

→ Lấy 10 mùa × 4 hạng Anh = 40 file, chỉ vài MB, tải trong ~1 phút.

## 3. Ý nghĩa các cột chính

| Cột | Ý nghĩa |
|---|---|
| `Date`, `Time` | Ngày giờ trận (định dạng **không nhất quán** giữa các mùa) |
| `HomeTeam`, `AwayTeam` | Tên đội |
| `FTHG`, `FTAG`, `FTR` | Bàn thắng & kết quả toàn trận (H/D/A) |
| `HTHG`, `HTAG`, `HTR` | Hiệp 1 |
| `HS`, `AS`, `HST`, `AST` | Sút / sút trúng đích |
| `HC`, `AC` | Phạt góc |
| `HY`, `AY`, `HR`, `AR` | Thẻ vàng / thẻ đỏ |
| `B365H/D/A`, `PSH/D/A`, `WHH/D/A` | Odds 1X2 của Bet365, Pinnacle, William Hill |
| `AvgH/D/A`, `MaxH/D/A` | Odds trung bình / cao nhất thị trường |
| `B365>2.5`, `B365<2.5` | Odds tài/xỉu 2.5 bàn |

## 4. Layout trong lake

```
bronze/football_data_couk/E0/season=2425/E0.csv
bronze/football_data_couk/E0/season=2324/E0.csv
bronze/football_data_couk/E1/season=2425/E1.csv
...
_meta/football_data_couk/schema_report.json     ← báo cáo schema drift

silver/matches/fd_matches/division=E0/season=2425/part-0.parquet
silver/odds/fd_odds/division=E0/season=2425/part-0.parquet
```

Ghi chú: bronze giữ **nguyên bytes CSV gốc**, không parse. Mọi việc làm sạch để ở silver.

## 5. Script ingest — `pipelines/p03_football_data.py`

```python
"""Ingest Football-Data.co.uk: CSV nhiều mùa, xử lý schema drift."""
import io
import json
import pandas as pd
from lake.minio_io import (put_bytes, put_parquet, read_bytes, exists,
                           summary, S3, BUCKET)
from lake.http import get

SRC = "football-data.co.uk"
BASE = "https://www.football-data.co.uk/mmz4281"

DIVISIONS = ["E0", "E1", "E2", "E3"]        # 4 hạng đấu Anh
SEASONS = ["1516", "1617", "1718", "1819", "1920",
           "2021", "2122", "2223", "2324", "2425"]

CORE = ["Div", "Date", "Time", "HomeTeam", "AwayTeam",
        "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR",
        "HS", "AS", "HST", "AST", "HC", "AC",
        "HY", "AY", "HR", "AR", "Referee"]

ODDS_BOOKS = ["B365", "BW", "IW", "PS", "WH", "VC", "Avg", "Max"]


# ---------- BRONZE ----------
def download_all() -> dict:
    """Tải raw CSV, không parse. Trả về dict {(div, season): key}."""
    keys, failed = {}, []
    for div in DIVISIONS:
        for season in SEASONS:
            key = f"bronze/football_data_couk/{div}/season={season}/{div}.csv"
            if exists(key):
                print(f"  · {div}/{season} đã có, bỏ qua")
                keys[(div, season)] = key
                continue
            url = f"{BASE}/{season}/{div}.csv"
            try:
                content = get(url).content
            except Exception as e:
                print(f"  ! {div}/{season} lỗi: {e}")
                failed.append((div, season))
                continue
            put_bytes(key, content, SRC, content_type="text/csv",
                      meta={"division": div, "season": season,
                            "source_url": url})
            keys[(div, season)] = key
    if failed:
        print(f"  ! {len(failed)} file không tải được: {failed}")
    return keys


# ---------- phân tích schema drift ----------
def schema_report(keys: dict):
    report = {}
    all_cols = set()
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key, nrows=1)
        cols = list(df.columns)
        report[f"{div}_{season}"] = {"n_cols": len(cols), "cols": cols}
        all_cols |= set(cols)

    # cột nào xuất hiện ở mọi file?
    common = set.intersection(*[set(v["cols"]) for v in report.values()])
    report["_summary"] = {
        "total_distinct_columns": len(all_cols),
        "columns_present_in_all_files": sorted(common),
        "n_common": len(common),
    }
    S3.put_object(
        Bucket=BUCKET, Key="_meta/football_data_couk/schema_report.json",
        Body=json.dumps(report, indent=2).encode(),
        ContentType="application/json")
    print(f"  ✓ schema drift: {len(all_cols)} cột khác nhau, "
          f"{len(common)} cột chung cho mọi file")
    return report


def _read_csv(key: str, **kw) -> pd.DataFrame:
    """CSV của site này encoding lộn xộn + có dòng rỗng cuối file."""
    raw = read_bytes(key)
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc,
                               on_bad_lines="skip", **kw)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Không decode được {key}")


def _parse_date(s: pd.Series) -> pd.Series:
    """Mùa cũ dùng dd/mm/yy, mùa mới dd/mm/yyyy. Thử cả hai."""
    d = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    fallback = pd.to_datetime(s, format="%d/%m/%y", errors="coerce")
    return d.fillna(fallback)


# ---------- SILVER ----------
def build_matches(keys: dict):
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key)
        df = df.dropna(subset=["HomeTeam", "AwayTeam"])   # bỏ dòng rác cuối file
        if df.empty:
            continue

        # chuẩn hoá: cột nào thiếu -> tạo với NaN, giữ schema đồng nhất
        out = df.reindex(columns=CORE).copy()
        out["Date"] = _parse_date(out["Date"])
        out["division"] = div
        out["season"] = f"20{season[:2]}-{season[2:]}"

        for c in ["FTHG", "FTAG", "HTHG", "HTAG", "HS", "AS", "HST", "AST",
                  "HC", "AC", "HY", "AY", "HR", "AR"]:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

        out["total_goals"] = out["FTHG"] + out["FTAG"]
        out["match_id"] = (out["division"] + "_" + out["season"] + "_"
                           + out["Date"].dt.strftime("%Y%m%d") + "_"
                           + out["HomeTeam"].str.replace(" ", "")
                           + "_" + out["AwayTeam"].str.replace(" ", ""))

        out = out.rename(columns={
            "Date": "match_date", "HomeTeam": "home_team",
            "AwayTeam": "away_team", "FTHG": "home_goals",
            "FTAG": "away_goals", "FTR": "result"})

        put_parquet(
            f"silver/matches/fd_matches/division={div}/season={season}/part-0.parquet",
            out, SRC, meta={"division": div, "season": season})


def build_odds(keys: dict):
    """Tách odds ra bảng riêng, unpivot thành long format."""
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key).dropna(subset=["HomeTeam", "AwayTeam"])
        if df.empty:
            continue
        date = _parse_date(df["Date"])
        mid = (div + "_20" + season[:2] + "-" + season[2:] + "_"
               + date.dt.strftime("%Y%m%d") + "_"
               + df["HomeTeam"].str.replace(" ", "") + "_"
               + df["AwayTeam"].str.replace(" ", ""))

        rows = []
        for book in ODDS_BOOKS:
            cols = [f"{book}H", f"{book}D", f"{book}A"]
            if not all(c in df.columns for c in cols):
                continue
            part = pd.DataFrame({
                "match_id": mid,
                "bookmaker": book,
                "odds_home": pd.to_numeric(df[cols[0]], errors="coerce"),
                "odds_draw": pd.to_numeric(df[cols[1]], errors="coerce"),
                "odds_away": pd.to_numeric(df[cols[2]], errors="coerce"),
            })
            rows.append(part.dropna(subset=["odds_home"]))

        if not rows:
            continue
        odds = pd.concat(rows, ignore_index=True)
        # margin của nhà cái = tổng xác suất ngầm - 1
        odds["implied_margin"] = (
            1/odds.odds_home + 1/odds.odds_draw + 1/odds.odds_away - 1).round(4)

        put_parquet(
            f"silver/odds/fd_odds/division={div}/season={season}/part-0.parquet",
            odds, SRC)


if __name__ == "__main__":
    print("[1/4] tải CSV")
    keys = download_all()
    print(f"      {len(keys)} file trong lake")

    print("[2/4] phân tích schema drift")
    schema_report(keys)

    print("[3/4] silver: matches")
    build_matches(keys)

    print("[4/4] silver: odds")
    build_odds(keys)

    summary("bronze/football_data_couk/")
    summary("silver/matches/fd_matches/")
    summary("silver/odds/")
```

## 6. Kết quả mong đợi

```
[1/4] tải CSV
  ✓ s3://football-lake/bronze/football_data_couk/E0/season=1516/E0.csv  (95,231 B, ...)
  ...
      40 file trong lake
[2/4] phân tích schema drift
  ✓ schema drift: 148 cột khác nhau, 62 cột chung cho mọi file
[3/4] silver: matches
  ✓ .../fd_matches/division=E0/season=2425/part-0.parquet  (21,109 B, 380 rows)
  ...
[4/4] silver: odds
  ✓ .../fd_odds/division=E0/season=2425/part-0.parquet  (48,772 B, 2,850 rows)

[summary] bronze/football_data_couk/: 40 objects, 13.91 MB
[summary] silver/matches/fd_matches/: 40 objects, 2.63 MB
[summary] silver/odds/: 40 objects, 8.44 MB
```

Tổng cộng khoảng **15.200 trận** và **~110.000 dòng odds**. Đây là bảng fact lớn nhất, đủ để chạy phân tích thật.

## 7. Truy vấn kiểm chứng

```sql
-- Lợi thế sân nhà có giảm dần theo thời gian không?
SELECT season,
       ROUND(100.0 * SUM(result = 'H') / COUNT(*), 1) AS pct_thang_san_nha,
       ROUND(100.0 * SUM(result = 'D') / COUNT(*), 1) AS pct_hoa,
       ROUND(AVG(total_goals), 2)                     AS ban_tb
FROM read_parquet('s3://football-lake/silver/matches/fd_matches/division=E0/**/*.parquet')
GROUP BY season ORDER BY season;

-- Nhà cái nào có biên lợi nhuận thấp nhất (odds tốt nhất cho người chơi)?
SELECT bookmaker,
       ROUND(AVG(implied_margin) * 100, 2) AS margin_pct,
       COUNT(*) AS n
FROM read_parquet('s3://football-lake/silver/odds/fd_odds/**/*.parquet')
GROUP BY bookmaker ORDER BY margin_pct;
```

Kết quả mẫu cho truy vấn thứ hai — Pinnacle (`PS`) thường có margin thấp nhất (~2-3%), Bet365 khoảng 5-6%. Nếu ra đúng xu hướng này thì pipeline của bạn đã chạy chuẩn.

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `UnicodeDecodeError` | File cũ dùng cp1252 (tên trọng tài có dấu) | Script đã thử 3 encoding tuần tự |
| Vài dòng cuối toàn NaN | CSV gốc có dòng trống | `dropna(subset=["HomeTeam","AwayTeam"])` |
| `match_date` toàn NaT ở mùa cũ | Format `dd/mm/yy` | Script có fallback parse 2 format |
| Mùa cũ thiếu cột `HS`, `AST` | Dữ liệu chưa thu thập hồi đó | `reindex(columns=CORE)` điền NaN — đúng ý đồ |
| Tên đội không khớp với nguồn khác | "Man United" vs "Manchester United" | Cần bảng mapping — xem mục 9 |

## 9. Mở rộng: bảng ánh xạ tên đội

Vấn đề sẽ gặp khi join nguồn này với FPL/API-Football. Tạo dimension thủ công:

```python
TEAM_ALIAS = {
    "Man United": "Manchester United", "Man City": "Manchester City",
    "Spurs": "Tottenham Hotspur", "Newcastle": "Newcastle United",
    "Wolves": "Wolverhampton Wanderers", "Nott'm Forest": "Nottingham Forest",
    "Sheffield United": "Sheffield Utd", "West Brom": "West Bromwich Albion",
    "Brighton": "Brighton & Hove Albion", "Leicester": "Leicester City",
}
```

Ghi bảng này lên `silver/dim/team_alias/part-0.parquet` — nó là thành phần thiết yếu để 8 nguồn nói chuyện được với nhau.

Ý tưởng khác: dùng odds trung bình (`Avg`) làm baseline dự đoán, so với mô hình xG bạn xây từ StatsBomb (file 04) — một bài toán gold-layer hoàn chỉnh.
