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
    dag_id="p02_api_football_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["api-football", "bronze", "silver", "quota"],
) as dag:

    @task
    def task_ingest_teams():
        from pipelines.p02.main import ingest_teams
        return ingest_teams()

    @task
    def task_ingest_standings():
        from pipelines.p02.main import ingest_standings
        return ingest_standings()

    @task
    def task_ingest_fixtures():
        from pipelines.p02.main import ingest_fixtures
        return ingest_fixtures()

    @task
    def task_ingest_fixture_details(fixtures):
        from pipelines.p02.main import ingest_fixture_details, load_checkpoint, save_checkpoint
        done = load_checkpoint()
        done = ingest_fixture_details(fixtures, done)
        save_checkpoint(done)
        # Convert set to list for XCom serialization
        return list(done)

    @task
    def task_build_silver(done_list, standings_body):
        from pipelines.p02.main import build_events, build_stats, build_standings
        done = set(done_list)
        build_events(done)
        build_stats(done)
        build_standings(standings_body)

    # 1. Ingest Bronze
    task_ingest_teams()
    standings_body = task_ingest_standings()
    fixtures_body = task_ingest_fixtures()
    
    # 2. Ingest Match Details (with Checkpoints)
    done_list = task_ingest_fixture_details(fixtures_body)
    
    # 3. Transform Silver
    task_build_silver(done_list, standings_body)
