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
    dag_id="p09_football_data_org",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["football_data_org", "api", "bronze", "silver"],
) as dag:

    @task
    def task_ingest_competitions():
        from pipelines.p09.main import ingest_competitions, build_competitions_dim
        comps = ingest_competitions()
        build_competitions_dim(comps)

    @task
    def task_ingest_league(code: str):
        from pipelines.p09.main import (
            ingest_teams, build_teams_dim, build_players_dim,
            ingest_matches, build_matches_fact,
            ingest_standings, build_standings_fact
        )
        print(f"\n[2/5] teams — {code}")
        teams = ingest_teams(code)
        build_teams_dim(code, teams)
        build_players_dim(code, teams)

        print(f"[3/5] matches — {code}")
        matches = ingest_matches(code)
        build_matches_fact(code, matches)

        print(f"[4/5] standings — {code}")
        standings = ingest_standings(code)
        build_standings_fact(code, standings)

    @task
    def task_summary():
        from pipelines.p09.main import summary
        summary("bronze/football_data_org/")
        summary("silver/dim/fdo_teams/")
        summary("silver/matches/fdo_matches/")

    # Define dependencies
    comps_task = task_ingest_competitions()
    
    # 12 giải free tier hay dùng nhất; PL là trọng tâm của bộ 8 nguồn kia
    COMPETITIONS = ["PL", "PD", "BL1", "SA", "FL1", "CL"]
    
    league_tasks = []
    for code in COMPETITIONS:
        league_tasks.append(task_ingest_league.override(task_id=f"ingest_league_{code.lower()}")(code))
        
    summary_task = task_summary()
    
    comps_task >> league_tasks >> summary_task
