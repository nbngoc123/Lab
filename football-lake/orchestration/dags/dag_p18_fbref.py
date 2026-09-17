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
    dag_id="p18_fbref_scraper",
    start_date=datetime(2023, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["fbref", "scraper", "bronze", "silver"],
) as dag:

    @task
    def task_ingest_all_fbref():
        from pipelines.p18.main import ingest_all
        # Chạy 1 task duy nhất chứa vòng lặp để đảm bảo tính an toàn cho Rate Limit
        # (Chắc chắn code sẽ delay 6 giây sau mỗi lượt fetch)
        ingest_all()

    task_ingest_all_fbref()
