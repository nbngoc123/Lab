# 10 — Wikimedia Pageviews API (time-series độ hot) → MinIO

**Kiểu ingest:** REST API, free, không cần key, dữ liệu **time-series theo ngày**
**Tần suất:** hàng ngày (dữ liệu hôm qua mới có sẵn)
**Độ khó:** ★★☆☆☆

---

## 1. Vì sao chọn nguồn này

Chín nguồn trước đều nói về *chuyện gì xảy ra trên sân*. Nguồn này nói về *người ta quan tâm tới ai nhiều đến mức nào* — lượt xem trang Wikipedia của từng đội/cầu thủ theo từng ngày, tính bằng API chính thức của Wikimedia Foundation.

Giá trị: đây là proxy rẻ và sạch nhất cho "độ hot" mà không cần OAuth, không rate-limit khắt khe, và **có sẵn dữ liệu từ 2015 tới hiện tại** — cho phép bạn dựng time series dài hơn cả StatsBomb. Ghép với kết quả trận (file 03/05/09), bạn trả lời được câu: "thắng một trận derby thì lượt xem Wikipedia của cầu thủ ghi bàn tăng bao nhiêu %, và duy trì bao lâu?"

## 2. Endpoint

Base: `https://wikimedia.org/api/rest_v1/metrics/pageviews`

| Endpoint | Ý nghĩa |
|---|---|
| `/per-article/en.wikipedia/all-access/user/{article}/daily/{start}/{end}` | Lượt xem 1 trang theo ngày |
| `/top/en.wikipedia/all-access/{year}/{month}/{day}` | Top 1000 trang xem nhiều nhất 1 ngày (dùng để phát hiện "viral") |

Không cần key nhưng **bắt buộc User-Agent mô tả rõ**, giống Wikidata (file 06) — cùng hệ sinh thái Wikimedia.

`article` phải đúng tên trang Wikipedia, ví dụ `Arsenal_F.C.`, `Erling_Haaland`, `Bukayo_Saka`. Dấu cách thành `_`.

## 3. Layout trong lake

```
bronze/wikimedia_pageviews/per_article/entity=team/article=Arsenal_F.C./ingest_date=2026-09-16/daily.json.gz
bronze/wikimedia_pageviews/per_article/entity=player/article=Bukayo_Saka/ingest_date=2026-09-16/daily.json.gz
bronze/wikimedia_pageviews/top_daily/date=2026-09-15/top.json.gz

silver/text/wm_pageviews/entity=team/part-0.parquet
silver/text/wm_pageviews/entity=player/part-0.parquet
```

Vì đây là time series tích lũy, mỗi lần chạy nên **kéo full range từ đầu tới hiện tại** rồi ghi đè — API trả nhanh (vài trăm ms/trang) nên không tốn kém.

## 4. Script ingest — `pipelines/p10_wikimedia_pageviews.py`

```python
"""Ingest Wikimedia Pageviews: time-series độ hot của đội/cầu thủ."""
import time
from datetime import date, timedelta
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, today, summary
from lake.http import SESSION

SRC = "wikimedia-pageviews"
D = today()
BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews"
UA = "football-lake/0.1 (educational project; contact: you@example.com)"

TEAMS = {
    "Arsenal_F.C.": "Arsenal", "Chelsea_F.C.": "Chelsea",
    "Liverpool_F.C.": "Liverpool", "Manchester_City_F.C.": "Manchester City",
    "Manchester_United_F.C.": "Manchester United",
    "Tottenham_Hotspur_F.C.": "Tottenham Hotspur",
    "Newcastle_United_F.C.": "Newcastle United",
}
PLAYERS = {
    "Erling_Haaland": "Erling Haaland", "Bukayo_Saka": "Bukayo Saka",
    "Mohamed_Salah": "Mohamed Salah", "Kevin_De_Bruyne": "Kevin De Bruyne",
    "Declan_Rice": "Declan Rice",
}

START = "20230101"           # yyyymmdd
END = date.today().strftime("%Y%m%d")


def fetch_daily(article: str) -> dict:
    url = (f"{BASE}/per-article/en.wikipedia/all-access/user/"
           f"{article}/daily/{START}/{END}")
    r = SESSION.get(url, headers={"User-Agent": UA}, timeout=30)
    if r.status_code == 404:
        print(f"    ! không tìm thấy trang '{article}'")
        return {"items": []}
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()


def fetch_top(day: date) -> dict:
    url = f"{BASE}/top/en.wikipedia/all-access/{day.year}/{day.month:02d}/{day.day:02d}"
    r = SESSION.get(url, headers={"User-Agent": UA}, timeout=30)
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()


# ---------- BRONZE ----------
def ingest_entities(entities: dict, entity_type: str) -> list:
    results = []
    for article, label in entities.items():
        body = fetch_daily(article)
        n = len(body.get("items", []))
        put_json_gz(
            f"bronze/wikimedia_pageviews/per_article/entity={entity_type}"
            f"/article={article}/ingest_date={D}/daily.json.gz",
            body, SRC, meta={"article": article, "days": n})
        print(f"  · {label}: {n} ngày dữ liệu")
        results.append((article, label, body))
    return results


def ingest_top_yesterday():
    yday = date.today() - timedelta(days=1)
    body = fetch_top(yday)
    put_json_gz(
        f"bronze/wikimedia_pageviews/top_daily/date={yday.isoformat()}/top.json.gz",
        body, SRC)
    return body


# ---------- SILVER ----------
def build_silver(results: list, entity_type: str) -> pd.DataFrame:
    rows = []
    for article, label, body in results:
        for item in body.get("items", []):
            rows.append({
                "entity_type": entity_type, "article": article, "label": label,
                "date": item["timestamp"][:8], "views": item["views"],
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")

    # rolling 7-ngày để lọc nhiễu weekday/weekend
    df = df.sort_values(["article", "date"])
    df["views_7d_avg"] = (df.groupby("article")["views"]
                          .transform(lambda s: s.rolling(7, min_periods=1).mean()))

    put_parquet(f"silver/text/wm_pageviews/entity={entity_type}/part-0.parquet",
                df, SRC, meta={"entities": df.article.nunique()})
    return df


def detect_spikes(df: pd.DataFrame, threshold=3.0) -> pd.DataFrame:
    """Ngày nào lượt xem vọt lên gấp N lần trung bình 7 ngày trước đó -> khả năng có sự kiện."""
    if df.empty:
        return df
    df = df.copy()
    df["baseline"] = (df.groupby("article")["views"]
                      .transform(lambda s: s.shift(1).rolling(7, min_periods=3).mean()))
    df["spike_ratio"] = df["views"] / df["baseline"]
    spikes = df[df["spike_ratio"] >= threshold].sort_values(
        "spike_ratio", ascending=False)
    if not spikes.empty:
        put_parquet("silver/text/wm_pageview_spikes/part-0.parquet", spikes, SRC)
        print(f"  ✓ phát hiện {len(spikes)} ngày có spike (>={threshold}x baseline)")
    return spikes


if __name__ == "__main__":
    print("[1/4] pageviews đội bóng")
    team_results = ingest_entities(TEAMS, "team")

    print("[2/4] pageviews cầu thủ")
    player_results = ingest_entities(PLAYERS, "player")

    print("[3/4] top trang hôm qua (bối cảnh chung)")
    ingest_top_yesterday()

    print("[4/4] silver + phát hiện spike")
    team_df = build_silver(team_results, "team")
    player_df = build_silver(player_results, "player")
    detect_spikes(pd.concat([team_df, player_df], ignore_index=True))

    summary("bronze/wikimedia_pageviews/")
    summary("silver/text/wm_pageviews/")
```

## 5. Kết quả mong đợi

```
[1/4] pageviews đội bóng
  ✓ s3://football-lake/bronze/wikimedia_pageviews/per_article/entity=team/article=Arsenal_F.C./ingest_date=2026-09-16/daily.json.gz  (28,441 B, ...)
  · Arsenal: 988 ngày dữ liệu
  · Chelsea: 988 ngày dữ liệu
  ...
[2/4] pageviews cầu thủ
  · Erling Haaland: 988 ngày dữ liệu
  ...
[3/4] top trang hôm qua (bối cảnh chung)
  ✓ .../top_daily/date=2026-09-15/top.json.gz  (142,004 B, ...)
[4/4] silver + phát hiện spike
  ✓ phát hiện 34 ngày có spike (>=3.0x baseline)

[summary] bronze/wikimedia_pageviews/: 14 objects, 1.9 MB
[summary] silver/text/wm_pageviews/: 2 objects, 0.6 MB
```

## 6. Truy vấn kiểm chứng

```sql
-- Ngày spike của cầu thủ khớp với ngày nào? (đối chiếu thủ công với lịch thi đấu)
SELECT label, date, views, ROUND(spike_ratio, 1) AS lan_tang
FROM read_parquet('s3://football-lake/silver/text/wm_pageview_spikes/*.parquet')
WHERE entity_type = 'player'
ORDER BY spike_ratio DESC LIMIT 15;

-- GHÉP NGUỒN: spike lượt xem có trùng ngày thi đấu của đội đó không?
SELECT p.label AS doi, p.date, p.views, p.spike_ratio,
       m.home_team, m.away_team, m.home_goals, m.away_goals
FROM read_parquet('s3://football-lake/silver/text/wm_pageview_spikes/*.parquet') p
JOIN read_parquet('s3://football-lake/silver/matches/fdo_matches/competition=PL/**/*.parquet') m
  ON p.date = m.utc_date::DATE
 AND (m.home_team LIKE '%' || p.label || '%' OR m.away_team LIKE '%' || p.label || '%')
WHERE p.entity_type = 'team'
ORDER BY p.date DESC;
```

Truy vấn thứ hai là ví dụ đẹp về **ghép time-series độ hot với fact trận đấu** — spike thường trùng ngày thi đấu hoặc ngày sau (nếu có kết quả gây chú ý).

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `404` cho 1 bài viết | Sai tên trang (thiếu `_F.C.`, dấu gạch dưới) | Kiểm tra URL thật trên `en.wikipedia.org` trước |
| `items` rỗng dù bài tồn tại | Trang có ít hơn 1 request/tháng, Wikimedia không trả | Bình thường với cầu thủ ít nổi tiếng |
| Baseline NaN ở đầu chuỗi | `rolling(min_periods=3)` chưa đủ dữ liệu 3 ngày đầu | Chấp nhận, spike detection tự bỏ qua |
| Trang đổi tên (cầu thủ đổi CLB, trang đổi tiêu đề) | Wikipedia redirect | Dùng `/pageviews` cho tên mới, chấp nhận gián đoạn chuỗi |

## 8. Mở rộng

- Thêm HLV, trọng tài nổi tiếng, hoặc cả giải đấu (`Premier_League`) vào danh sách theo dõi.
- Dùng `/top` daily để tự động phát hiện cầu thủ nào đang viral mà bạn chưa theo dõi — quét top 1000, lọc tên khớp cầu thủ trong `silver/dim/fdo_players` (file 09).
- Language edition khác (`es.wikipedia`, `pt.wikipedia`) để so độ hot theo khu vực địa lý.
