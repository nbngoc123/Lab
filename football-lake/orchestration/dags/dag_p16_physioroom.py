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
    dag_id="p16_physioroom_injury_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["physioroom", "bronze", "silver", "scraper"],
) as dag:

    @task
    def task_fetch_page():
        from pipelines.p16.main import fetch_page
        return fetch_page()

    @task
    def task_parse_tables(html: str):
        from pipelines.p16.main import parse_tables
        return parse_tables(html)

    @task
    def task_build_silver(tables_raw: list):
        from pipelines.p16.main import build_club_summary, build_player_injuries, build_injury_frequency
        build_club_summary(tables_raw[0])
        build_player_injuries(tables_raw[1])
        build_injury_frequency(tables_raw[3])

    html_content = task_fetch_page()
    tables_data = task_parse_tables(html_content)
    task_build_silver(tables_data)
