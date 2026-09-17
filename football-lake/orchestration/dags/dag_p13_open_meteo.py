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
        return load_stadiums()

    @task
    def task_ingest_historical(stadiums):
        from pipelines.p13.main import ingest_all_historical
        return ingest_all_historical(stadiums)

    @task
    def task_ingest_forecast(stadiums):
        from pipelines.p13.main import ingest_forecast
        ingest_forecast(stadiums)

    @task
    def task_build_silver(results):
        from pipelines.p13.main import build_hourly_table, join_weather_to_matches
        weather_df = build_hourly_table(results)
        
        venue_map = {
            "Arsenal": "Emirates Stadium", "Liverpool": "Anfield",
            "Manchester City": "City of Manchester Stadium",
            "Manchester United": "Old Trafford",
            "Chelsea": "Stamford Bridge", "Tottenham": "Tottenham Hotspur Stadium",
        }
        join_weather_to_matches(weather_df, venue_map)

    # 1. Load coordinates
    stadiums = task_load_stadiums()
    
    # 2. Extract bronze
    historical_results = task_ingest_historical(stadiums)
    task_ingest_forecast(stadiums)
    
    # 3. Transform silver
    task_build_silver(historical_results)
