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
def load_stadiums() -> list:
    import io
    from lake.minio_io import S3, BUCKET
    
    prefix = "silver/dim/wd_stadiums/"
    try:
        objs = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    except Exception as e:
        print(f"  ! Lỗi S3: {e}")
        return []
        
    dfs = []
    for obj in objs:
        if obj["Key"].endswith(".parquet"):
            body = S3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            dfs.append(pd.read_parquet(io.BytesIO(body)))
            
    if not dfs:
        return []
        
    df = pd.concat(dfs, ignore_index=True)
    if "venueLabel" in df.columns:
        df = df.rename(columns={"venueLabel": "venue"})
    df = df[df.lat.notna() & df.lon.notna()]
    
    # Giới hạn lấy 2 sân để test nhanh theo yêu cầu
    return df.head(2).to_dict('records')


def ingest_all_historical(stadiums: list, start="2024-08-01", end="2024-09-01"):
    # Đổi start, end cho khoảng 1 tháng gần đây để test nhanh
    results = {}
    for row in stadiums:
        venue = row["venue"]
        lat = row["lat"]
        lon = row["lon"]
        venue_safe = venue.replace(" ", "_").replace("/", "_")
        body = fetch_historical(lat, lon, start, end)
        put_json_gz(
            f"bronze/open_meteo/historical/venue={venue_safe}"
            f"/ingest_date={D}/weather.json.gz",
            body, SRC, meta={"venue": venue, "lat": lat, "lon": lon})
        results[venue] = body
        print(f"  · {venue}: {len(body.get('hourly', {}).get('time', []))} giờ dữ liệu")
    return results


def ingest_forecast(stadiums: list):
    for row in stadiums:
        venue = row["venue"]
        lat = row["lat"]
        lon = row["lon"]
        venue_safe = venue.replace(" ", "_").replace("/", "_")
        body = fetch_forecast(lat, lon)
        put_json_gz(
            f"bronze/open_meteo/forecast/venue={venue_safe}"
            f"/ingest_date={D}/forecast.json.gz",
            body, SRC, meta={"venue": row.venue})


# ---------- SILVER ----------
def build_hourly_table(results: dict) -> pd.DataFrame:
    if not results:
        return pd.DataFrame()
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
    if weather.empty:
        return
    import io
    from lake.minio_io import S3, BUCKET

    prefix = "silver/matches/fd_matches/division=E0/season=2425/"
    try:
        objs = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    except Exception as e:
        print(f"  ! Lỗi đọc fd_matches (có thể do p03 chưa tải dữ liệu): {e}")
        return
        
    dfs = []
    for obj in objs:
        if obj["Key"].endswith(".parquet"):
            body = S3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            dfs.append(pd.read_parquet(io.BytesIO(body)))
            
    if not dfs:
        print("  ! Không tìm thấy dữ liệu trận đấu fd_matches")
        return
        
    matches = pd.concat(dfs, ignore_index=True)

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
        "silver/matches/fd_matches_weather/division=E0/season=2425/part-0.parquet",
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
        "Manchester City": "City of Manchester Stadium",
        "Manchester United": "Old Trafford",
        "Chelsea": "Stamford Bridge", "Tottenham": "Tottenham Hotspur Stadium",
    }
    join_weather_to_matches(weather_df, venue_map)

    summary("bronze/open_meteo/")
    summary("silver/dim/om_venue_weather/")
