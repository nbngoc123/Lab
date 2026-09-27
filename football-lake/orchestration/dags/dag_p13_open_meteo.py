import sys
from pathlib import Path
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

with DAG(
    dag_id="p13_open_meteo_weather_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["open-meteo", "weather", "bronze", "silver", "enrichment"],
) as dag:

    @task
    def task_load_stadiums():
        from pipelines.p13.main import load_stadiums
        return load_stadiums()   # dict nhỏ (20 sân) — OK truyền qua XCom

    @task
    def task_ingest_historical(stadiums):
        """Cào weather lịch sử -> ghi thẳng bronze MinIO, KHÔNG return data lớn qua XCom."""
        from pipelines.p13.main import ingest_all_historical
        ingest_all_historical(stadiums)   # bỏ return để tránh XCom timeout
        return None

    @task
    def task_ingest_forecast(stadiums):
        from pipelines.p13.main import ingest_forecast
        ingest_forecast(stadiums)

    @task
    def task_build_silver(stadiums):
        """Đọc thẳng từ bronze MinIO (không dùng XCom lớn từ historical_results)."""
        from pipelines.p13.main import build_hourly_table_from_minio, join_weather_to_matches
        weather_df = build_hourly_table_from_minio()
        join_weather_to_matches(weather_df, stadiums)

    # 1. Load coordinates
    stadiums = task_load_stadiums()

    # 2. Extract bronze (chạy song song, không truyền dữ liệu lớn qua XCom)
    hist_done = task_ingest_historical(stadiums)
    task_ingest_forecast(stadiums)

    # 3. Transform silver — đọc từ MinIO thay vì nhận XCom lớn
    task_build_silver(stadiums) >> hist_done  # đảm bảo historical xong trước khi build silver
