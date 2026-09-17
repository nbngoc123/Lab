from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'p24_api_football',
    default_args=default_args,
    description='Ingest live scores and fixtures from API-Football (Daily)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fixtures', 'live', 'daily', 'p24', 'api-football'],
) as dag:

    @task
    def task_ingest_api_football():
        from pipelines.p24.main import run_pipeline
        run_pipeline()

    task_ingest_api_football()
