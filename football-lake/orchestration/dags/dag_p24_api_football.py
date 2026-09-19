"""
DAG p24: API-Football Full Pipeline
Schedule: @daily
Thay thế hoàn toàn p02 — lấy toàn bộ data EPL từ API-Sports.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'dag_p24_api_football',
    default_args=default_args,
    description='API-Football Full: Teams, Standings, Fixtures, Events, Lineups, Stats, Top Scorers',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['football_lake', 'api_football', 'epl'],
) as dag:

    run_pipeline = BashOperator(
        task_id='run_p24_full_pipeline',
        bash_command='python /opt/project/pipelines/p24/main.py',
    )
