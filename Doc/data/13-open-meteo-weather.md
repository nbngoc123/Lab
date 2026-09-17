# 13 — Open-Meteo (thời tiết lịch sử theo sân) → MinIO

**Kiểu ingest:** REST API, free, không cần key, không rate-limit khắt khe
**Tần suất:** 1 lần cho lịch sử + hàng ngày cho trận sắp diễn ra
**Độ khó:** ★★☆☆☆ — dễ, giá trị nằm ở việc **ghép tọa độ với thời điểm trận đấu**

---

## 1. Vì sao chọn nguồn này

Đây là nguồn **enrichment theo thời gian + không gian**: thời tiết tại đúng sân, đúng ngày, đúng giờ một trận đấu diễn ra. Không nguồn nào trong 12 file trước có biến này, nhưng nó là yếu tố thật sự ảnh hưởng tới lối chơi (mưa to → nhiều lỗi chuyền, gió mạnh → ít bàn từ xa, nóng → thay người sớm hơn).

Cái hay của Open-Meteo: **hoàn toàn miễn phí, không cần đăng ký**, có cả API lịch sử (từ 1940) lẫn dự báo, độ chính xác đủ dùng cho phân tích thể thao. Nó phụ thuộc trực tiếp vào tọa độ sân đã lấy ở **file 06 (Wikidata)** — minh chứng rõ ràng cho việc các nguồn trong lake nuôi lẫn nhau.

## 2. Endpoint

Base lịch sử: `https://archive-api.open-meteo.com/v1/archive`
Base dự báo (7 ngày tới): `https://api.open-meteo.com/v1/forecast`

Tham số chính:

| Tham số | Ý nghĩa |
|---|---|
| `latitude`, `longitude` | Tọa độ sân |
| `start_date`, `end_date` | Khoảng ngày (yyyy-mm-dd) |
| `hourly` | Danh sách biến theo giờ: `temperature_2m,precipitation,windspeed_10m,relative_humidity_2m` |
| `timezone` | Nên để `auto` để API tự quy đổi theo tọa độ |

Không cần key, nhưng có rate limit mềm khoảng **10.000 request/ngày** cho non-commercial — thoải mái cho quy mô 20 sân × vài trăm trận.

## 3. Layout trong lake

```
bronze/open_meteo/historical/venue=Emirates_Stadium/ingest_date=2026-09-16/weather.json.gz
bronze/open_meteo/historical/venue=Anfield/ingest_date=2026-09-16/weather.json.gz
bronze/open_meteo/forecast/venue=Emirates_Stadium/ingest_date=2026-09-16/forecast.json.gz

silver/dim/om_venue_weather/part-0.parquet
silver/matches/fd_matches_weather/division=E0/season=2425/part-0.parquet   ← đã join với file 03
```

Vì lấy theo giờ trong nhiều năm, file JSON có thể khá lớn (mỗi sân/mùa ~50-80 KB đã gzip) — vẫn nhỏ so với StatsBomb (file 04).

## 4. Script ingest — `pipelines/p13_open_meteo.py`

```python
"""Ingest Open-Meteo: thời tiết lịch sử tại tọa độ sân, ghép với ngày thi đấu."""
import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, read_bytes, today, summary
from lake.http import get

SRC = "open-meteo"
D = today()
HIST_BASE = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = "temperature_2m,precipitation,windspeed_10m,relative_humidity_2m,weathercode"


def fetch_historical(lat: float, lon: float, start: str, end: str) -> dict:
    r = get(HIST_BASE, params={
        "latitude": lat, "longitude": lon,
        "start_date": start, "end_date": end,
        "hourly": HOURLY_VARS, "timezone": "auto",
    })
    time.sleep(0.2)
    return r.json()


def fetch_forecast(lat: float, lon: float) -> dict:
    r = get(FORECAST_BASE, params={
        "latitude": lat, "longitude": lon,
        "hourly": HOURLY_VARS, "forecast_days": 7, "timezone": "auto",
    })
    return r.json()


# ---------- BRONZE ----------
def load_stadiums() -> pd.DataFrame:
    """Đọc dimension sân đã ingest ở file 06 (Wikidata) — không hardcode tọa độ."""
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
      SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false;
      SET s3_url_style='path';
      SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin123';
    """)
    return con.sql("""
        SELECT venueLabel AS venue, lat, lon
        FROM read_parquet('s3://football-lake/silver/dim/wd_stadiums/**/*.parquet')
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).df()


def ingest_all_historical(stadiums: pd.DataFrame, start="2023-08-01", end="2024-06-01"):
    results = {}
    for _, row in stadiums.iterrows():
        venue_safe = row.venue.replace(" ", "_").replace("/", "_")
        body = fetch_historical(row.lat, row.lon, start, end)
        put_json_gz(
            f"bronze/open_meteo/historical/venue={venue_safe}"
            f"/ingest_date={D}/weather.json.gz",
            body, SRC, meta={"venue": row.venue, "lat": row.lat, "lon": row.lon})
        results[row.venue] = body
        print(f"  · {row.venue}: {len(body.get('hourly', {}).get('time', []))} giờ dữ liệu")
    return results


def ingest_forecast(stadiums: pd.DataFrame):
    for _, row in stadiums.iterrows():
        venue_safe = row.venue.replace(" ", "_").replace("/", "_")
        body = fetch_forecast(row.lat, row.lon)
        put_json_gz(
            f"bronze/open_meteo/forecast/venue={venue_safe}"
            f"/ingest_date={D}/forecast.json.gz",
            body, SRC, meta={"venue": row.venue})


# ---------- SILVER ----------
def build_hourly_table(results: dict) -> pd.DataFrame:
    rows = []
    for venue, body in results.items():
        h = body.get("hourly", {})
        times = h.get("time", [])
        for i, t in enumerate(times):
            rows.append({
                "venue": venue, "datetime": t,
                "temperature_c": h["temperature_2m"][i],
                "precipitation_mm": h["precipitation"][i],
                "windspeed_kmh": h["windspeed_10m"][i],
                "humidity_pct": h["relative_humidity_2m"][i],
                "weathercode": h["weathercode"][i],
            })
    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["datetime"])
    put_parquet("silver/dim/om_venue_weather/part-0.parquet", df, SRC,
                meta={"venues": df.venue.nunique(), "hours": len(df)})
    return df


def join_weather_to_matches(weather: pd.DataFrame, venue_map: dict):
    """
    Ghép thời tiết vào từng trận của file 03 (Football-Data.co.uk).
    venue_map: {home_team: venue_name} — nối qua đội sân nhà.
    """
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
      SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false;
      SET s3_url_style='path';
      SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin123';
    """)
    matches = con.sql("""
        SELECT * FROM read_parquet(
          's3://football-lake/silver/matches/fd_matches/division=E0/season=2324/**/*.parquet')
    """).df()

    matches["venue"] = matches["home_team"].map(venue_map)
    weather = weather.copy()
    weather["date"] = weather["datetime"].dt.date
    weather["hour"] = weather["datetime"].dt.hour
    # xấp xỉ giờ đá bằng 15h nếu Time trống (dữ liệu cũ hay thiếu giờ)
    daily_afternoon = weather[weather["hour"].between(14, 17)]
    daily_avg = (daily_afternoon.groupby(["venue", "date"])
                 [["temperature_c", "precipitation_mm", "windspeed_kmh"]]
                 .mean().reset_index())

    matches["match_date_only"] = pd.to_datetime(matches["match_date"]).dt.date
    merged = matches.merge(
        daily_avg, left_on=["venue", "match_date_only"],
        right_on=["venue", "date"], how="left")

    put_parquet(
        "silver/matches/fd_matches_weather/division=E0/season=2324/part-0.parquet",
        merged, SRC, meta={"matched": merged.temperature_c.notna().sum()})
    print(f"  ✓ ghép được thời tiết cho {merged.temperature_c.notna().sum()}/"
          f"{len(merged)} trận")
    return merged


if __name__ == "__main__":
    print("[1/4] đọc tọa độ sân từ Wikidata (file 06)")
    stadiums = load_stadiums()
    print(f"  · {len(stadiums)} sân có tọa độ")

    print("[2/4] tải thời tiết lịch sử")
    results = ingest_all_historical(stadiums)

    print("[3/4] tải dự báo 7 ngày tới")
    ingest_forecast(stadiums)

    print("[4/4] silver: build + join với matches")
    weather_df = build_hourly_table(results)

    # bảng ánh xạ đội -> tên sân (khớp với TEAM_ALIAS ở file 03)
    venue_map = {
        "Arsenal": "Emirates Stadium", "Liverpool": "Anfield",
        "Manchester City": "Etihad Stadium",
        "Manchester United": "Old Trafford",
        "Chelsea": "Stamford Bridge", "Tottenham": "Tottenham Hotspur Stadium",
    }
    join_weather_to_matches(weather_df, venue_map)

    summary("bronze/open_meteo/")
    summary("silver/dim/om_venue_weather/")
```

## 5. Kết quả mong đợi

```
[1/4] đọc tọa độ sân từ Wikidata (file 06)
  · 22 sân có tọa độ
[2/4] tải thời tiết lịch sử
  · Emirates Stadium: 7,320 giờ dữ liệu
  · Anfield: 7,320 giờ dữ liệu
  ...
[3/4] tải dự báo 7 ngày tới
[4/4] silver: build + join với matches
  ✓ ghép được thời tiết cho 178/190 trận

[summary] bronze/open_meteo/: 44 objects, 2.1 MB
[summary] silver/dim/om_venue_weather/: 1 objects, 3.8 MB
```

178/190 khớp (94%) là hợp lý — phần thiếu do sân không map được tên hoặc thiếu tọa độ ở Wikidata.

## 6. Truy vấn kiểm chứng

```sql
-- Trời mưa có ảnh hưởng số bàn thắng không?
SELECT
  CASE WHEN precipitation_mm > 1 THEN 'Có mưa' ELSE 'Không mưa' END AS dieu_kien,
  COUNT(*) AS so_tran,
  ROUND(AVG(total_goals), 2) AS ban_tb
FROM read_parquet('s3://football-lake/silver/matches/fd_matches_weather/**/*.parquet')
WHERE precipitation_mm IS NOT NULL
GROUP BY 1;

-- Nhiệt độ và tổng số thẻ (giả thuyết: nóng -> nhiều lỗi hơn)
SELECT
  CASE WHEN temperature_c > 20 THEN 'Nóng >20°C'
       WHEN temperature_c < 10 THEN 'Lạnh <10°C'
       ELSE 'Vừa' END AS nhom_nhiet_do,
  ROUND(AVG(hy + ay), 2) AS the_vang_tb
FROM read_parquet('s3://football-lake/silver/matches/fd_matches_weather/**/*.parquet')
WHERE temperature_c IS NOT NULL
GROUP BY 1;
```

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `hourly` trả rỗng | Ngày vượt quá phạm vi lịch sử của trạm gần nhất | Archive API phủ từ 1940 nhưng độ chi tiết vùng xa có thể kém — chấp nhận NaN |
| Join thời tiết vào trận bị lệch ngày | Timezone khác nhau giữa `match_date` (UTC) và thời tiết (`timezone=auto`, giờ địa phương) | Luôn convert về cùng timezone trước khi so ngày |
| Venue không khớp (`venue_map` thiếu đội) | Đội đổi sân hoặc thăng hạng, chưa có trong map thủ công | Mở rộng `venue_map`, đối chiếu với `esd_teams`/`wd_clubs` |
| Request bị từ chối hàng loạt | Vượt quota mềm (hiếm khi xảy ra ở quy mô này) | Thêm `time.sleep` dài hơn giữa các sân |

## 8. Mở rộng

- Bổ sung `windspeed` vào phân tích bàn thắng từ xa (kết hợp `shot_end_x/y` ở StatsBomb, file 04) — giả thuyết "gió mạnh giảm độ chính xác cú sút xa".
- Với trận sắp diễn ra, dùng `forecast` để tạo cảnh báo tự động ("dự báo mưa to trận tối nay") — ghép với ELO (file 12) ra một feature dự đoán tốt hơn.
- Thêm biến `soil_moisture` hoặc `cloud_cover` nếu cần phân tích sâu hơn — Open-Meteo hỗ trợ hàng chục biến khí tượng khác trong cùng endpoint.
