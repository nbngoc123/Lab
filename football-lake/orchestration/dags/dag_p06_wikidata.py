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
    dag_id="p06_wikidata_sparql_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["wikidata", "sparql", "bronze", "silver", "dim"],
) as dag:

    @task
    def task_ingest_all():
        from pipelines.p06.main import ingest_all
        return ingest_all()

    @task
    def task_build_clubs(res):
        from pipelines.p06.main import build_clubs
        build_clubs(res["pl_clubs"])

    @task
    def task_build_players(res):
        from pipelines.p06.main import build_players
        build_players(res["pl_players"])

    @task
    def task_build_stadiums(res):
        from pipelines.p06.main import build_stadiums
        build_stadiums(res["pl_stadiums"])

    # --- DEFINE DEPENDENCIES ---
    
    # 1. Bronze Layer Extraction
    results = task_ingest_all()
    
    # 2. Silver Layer Transformation
    t_clubs = task_build_clubs(results)
    t_players = task_build_players(results)
    t_stadiums = task_build_stadiums(results)

    # Execution Flow: wait for extraction, then build dimensions in parallel
    results >> [t_clubs, t_players, t_stadiums]
