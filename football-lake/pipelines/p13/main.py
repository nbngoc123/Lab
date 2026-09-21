"""Ingest Open-Meteo: thời tiết lịch sử tại tọa độ sân, ghép với ngày thi đấu."""
import os
import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, read_bytes, today, summary
from lake.http import get
from lake.team_lookup import add_team_key

TEST_MODE = os.getenv("TEST_MODE") == "1"

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
        if "venue" in df.columns:
            df = df.drop(columns=["venue"])
        df = df.rename(columns={"venueLabel": "venue"})
    df = df[df.lat.notna() & df.lon.notna()]
    # Thêm team_key để join qua alias thay vì so chuỗi tên sân
    df = add_team_key(df, "venue", source="openmeteo", out_col="team_key")
    if TEST_MODE:
        df = df.head(2)
    import numpy as np
    df = df.replace({np.nan: None})
    return df.to_dict("records")


def ingest_all_historical(stadiums: list, start="2025-08-01", end="2026-06-01"):
    """Dữ liệu lịch sử."""
    if TEST_MODE:
        start = "2024-08-01"
        end = "2024-08-02"
    print(f"  · Bắt đầu tải archive từ {start} đến {end}")
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
            body, SRC, meta={"venue": venue})


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
    # Issue #4: thêm partition ingest_date để tránh ghi đè giữa các lần chạy
    put_parquet(f"silver/dim/om_venue_weather/ingest_date={D}/part-0.parquet",
                df, SRC, meta={"venues": df.venue.nunique(), "hours": len(df)})
    return df


def join_weather_to_matches(weather: pd.DataFrame, stadiums: list):
    """
    Ghép thời tiết vào từng trỚn qua team_key chuẩn.
    stadiums: list of dicts có các trường 'venue' và 'team_key'
    """
    if weather.empty:
        return
    import io
    from lake.minio_io import S3, BUCKET

    # Issue #10: join qua team_key thay vì so chuỗi tên sân
    venue_to_team = {r["venue"]: r.get("team_key") for r in stadiums}

    # Đọc fd_matches mới nhất từ mọi mùa/giải đã có
    prefix = "silver/matches/fd_matches/"
    try:
        objs = S3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", [])
    except Exception as e:
        print(f"  ! Lỗi đọc fd_matches: {e}")
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
    # Map home_team_key → venue → join với weather
    team_to_venue = {v: k for k, v in venue_to_team.items() if v}
    matches["venue"] = matches["home_team_key"].map(team_to_venue)

    weather = weather.copy()
    weather["date"] = weather["datetime"].dt.date
    weather["hour"] = weather["datetime"].dt.hour
    daily_afternoon = weather[weather["hour"].between(14, 17)]
    daily_avg = (daily_afternoon.groupby(["venue", "date"])
                 [["temperature_c", "precipitation_mm", "windspeed_kmh"]]
                 .mean().reset_index())

    matches["match_date_only"] = pd.to_datetime(matches["match_date"]).dt.date
    merged = matches.merge(
        daily_avg, left_on=["venue", "match_date_only"],
        right_on=["venue", "date"], how="left")

    put_parquet(
        f"silver/matches/fd_matches_weather/ingest_date={D}/part-0.parquet",
        merged, SRC, meta={"matched": merged.temperature_c.notna().sum()})
    print(f"  ✓ ghép được thời tiết cho {merged.temperature_c.notna().sum()}/"
          f"{len(merged)} trận")
    return merged


if __name__ == "__main__":
    print("[1/4] đọc tọa độ sân từ Wikidata (file 06)")
    stadiums = load_stadiums()
    print(f"  · {len(stadiums)} sân có tọoa độ")

    print("[2/4] tải thời tiết lịch sử")
    results = ingest_all_historical(stadiums)

    print("[3/4] tải dự báo 7 ngày tới")
    ingest_forecast(stadiums)

    print("[4/4] silver: build + join với matches")
    weather_df = build_hourly_table(results)
    join_weather_to_matches(weather_df, stadiums)

    summary("bronze/open_meteo/")
    summary("silver/dim/om_venue_weather/")
