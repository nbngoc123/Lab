from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from common.assets import odds_bronze

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_ingest_odds',
    default_args=default_args,
    description='Ingest betting odds from The Odds API (Hourly)',
    schedule='@hourly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['betting', 'odds', 'hourly', 'p22'],
) as dag:

    @task(outlets=[odds_bronze])
    def task_ingest_odds():
        from pipelines.p22.main import run_pipeline
        run_pipeline()

    task_ingest_odds()
