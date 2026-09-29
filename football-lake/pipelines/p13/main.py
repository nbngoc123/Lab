"""
Ingest Open-Meteo (p13): thời tiết lịch sử và dự báo tại tọa độ sân.
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.
"""
import os
import time
import pandas as pd
from lake.minio_io import put_json_gz, exists, today
from lake.http import get
from lake.team_lookup import add_team_key

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "open-meteo"
D = today()
HIST_BASE = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_BASE = "https://api.open-meteo.com/v1/forecast"

HOURLY_VARS = "temperature_2m,precipitation,windspeed_10m,relative_humidity_2m,weathercode"


def fetch_historical(lat: float, lon: float, start: str, end: str) -> dict:
    from lake.http import SESSION
    url = f"{HIST_BASE}?latitude={lat}&longitude={lon}&start_date={start}&end_date={end}&hourly={HOURLY_VARS}&timezone=auto"
    r = SESSION.get(url, timeout=30)
    if r.status_code == 429:
        print("  ! 429 Rate limit, chờ 60s...")
        time.sleep(60)
        return fetch_historical(lat, lon, start, end)
    r.raise_for_status()
    time.sleep(2)
    return r.json()


def fetch_forecast(lat: float, lon: float) -> dict:
    r = get(FORECAST_BASE, params={
        "latitude": lat, "longitude": lon,
        "hourly": HOURLY_VARS, "forecast_days": 7, "timezone": "auto",
    }, timeout=30)
    time.sleep(2)
    return r.json()


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
    df = add_team_key(df, "venue", source="openmeteo", out_col="team_key")
    if TEST_MODE:
        df = df.head(2)
    import numpy as np
    df = df.replace({np.nan: None})
    return df.to_dict("records")


def get_partitions() -> list[dict]:
    """Tạo partitions từ danh sách sân vận động lấy từ Silver."""
    return load_stadiums()


def ingest(partition: dict) -> str:
    """Tải thời tiết lịch sử và dự báo cho 1 sân vận động."""
    venue = partition["venue"]
    lat = partition["lat"]
    lon = partition["lon"]
    venue_safe = venue.replace(" ", "_").replace("/", "_")
    
    start = "2024-08-01" if TEST_MODE else "2020-08-01"
    end = "2024-08-02" if TEST_MODE else "2026-06-01"
    
    # Lấy lịch sử
    hist_key = f"bronze/open_meteo/historical/venue={venue_safe}/ingest_date={D}/weather.json.gz"
    if not exists(hist_key):
        body_hist = fetch_historical(lat, lon, start, end)
        put_json_gz(hist_key, body_hist, SRC, meta={"venue": venue, "lat": lat, "lon": lon})
        print(f"  ✓ Historical cho {venue}")
    else:
        print(f"  · {venue} lịch sử đã có")
        
    # Lấy dự báo
    fcast_key = f"bronze/open_meteo/forecast/venue={venue_safe}/ingest_date={D}/forecast.json.gz"
    if not exists(fcast_key):
        body_fcast = fetch_forecast(lat, lon)
        put_json_gz(fcast_key, body_fcast, SRC, meta={"venue": venue})
        print(f"  ✓ Dự báo cho {venue}")
    else:
        print(f"  · {venue} dự báo đã có")
        
    return hist_key
