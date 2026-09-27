import sys
from pathlib import Path
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

# Safeguard to ensure Airflow finds the pipelines module
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

with DAG(
    dag_id="p03_football_data_couk_csv_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["football-data", "csv", "bronze", "silver", "odds"],
) as dag:

    @task
    def task_download_all():
        from pipelines.p03.main import download_all
        return download_all()

    @task
    def task_schema_report(keys):
        from pipelines.p03.main import schema_report
        schema_report(keys)

    @task
    def task_build_matches(keys):
        from pipelines.p03.main import build_matches
        build_matches(keys)

    @task
    def task_build_odds(keys):
        from pipelines.p03.main import build_odds
        build_odds(keys)

    # --- DEFINE DEPENDENCIES ---
    
    # 1. Bronze Layer Extraction
    keys = task_download_all()
    
    # 2. Schema Analysis (Meta)
    t_report = task_schema_report(keys)
    
    # 3. Silver Layer Transformation
    t_matches = task_build_matches(keys)
    t_odds = task_build_odds(keys)

    # Set up orchestration flow
    keys >> t_report
    
    # Silver transformations wait for download to finish
    keys >> t_matches
    keys >> t_odds
