from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

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

    @task
    def run_p24_full_pipeline():
        from pipelines.p24.main import run_pipeline
        run_pipeline()

    run_p24_full_pipeline()
