import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

with DAG(
    dag_id="p19_understat_xg",
    start_date=datetime(2023, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["understat", "xg", "bronze", "silver"],
) as dag:

    @task
    def task_ingest_players(league: str):
        from pipelines.p19.main import get_client, ingest_players, build_player_xg
        client = get_client()
        data = ingest_players(client, league)
        build_player_xg(data, league)

    @task
    def task_ingest_teams(league: str):
        from pipelines.p19.main import get_client, ingest_teams, build_team_xg
        client = get_client()
        data = ingest_teams(client, league)
        build_team_xg(data, league)

    @task
    def task_ingest_matches(league: str):
        from pipelines.p19.main import get_client, ingest_matches, build_match_xg
        client = get_client()
        data = ingest_matches(client, league)
        build_match_xg(data, league)

    @task
    def task_summary():
        from pipelines.p19.main import summary
        summary("bronze/understat/")
        summary("silver/players/understat_player_xg/")
        summary("silver/teams/understat_team_xg/")

    LEAGUES = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1"]

    all_tasks = []
    for league in LEAGUES:
        p = task_ingest_players.override(task_id=f"players_{league.lower()}")(league)
        t = task_ingest_teams.override(task_id=f"teams_{league.lower()}")(league)
        m = task_ingest_matches.override(task_id=f"matches_{league.lower()}")(league)
        all_tasks.extend([p, t, m])

    task_summary() << all_tasks
