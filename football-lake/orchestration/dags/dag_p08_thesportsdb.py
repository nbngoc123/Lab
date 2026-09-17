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
    dag_id="p08_thesportsdb_binary_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["thesportsdb", "binary", "bronze", "silver"],
) as dag:

    @task
    def task_ingest_teams():
        from pipelines.p08.main import ingest_teams
        return ingest_teams()

    @task
    def task_build_teams_dim(teams: list):
        from pipelines.p08.main import build_teams_dim
        build_teams_dim(teams)

    @task
    def task_ingest_team_media(teams: list):
        from pipelines.p08.main import ingest_team_media
        return ingest_team_media(teams)

    @task
    def task_ingest_players_and_media(teams: list):
        from pipelines.p08.main import ingest_players_and_media
        return ingest_players_and_media(teams)

    @task
    def task_write_manifest(team_manifest: list, player_manifest: list):
        from pipelines.p08.main import write_manifest
        full_manifest = team_manifest + player_manifest
        write_manifest(full_manifest)

    # Define dependencies
    teams_data = task_ingest_teams()
    
    task_build_teams_dim(teams_data)
    
    team_manifest = task_ingest_team_media(teams_data)
    player_manifest = task_ingest_players_and_media(teams_data)
    
    task_write_manifest(team_manifest, player_manifest)
