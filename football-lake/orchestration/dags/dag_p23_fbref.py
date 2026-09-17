from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    'p23_fbref_stats',
    default_args=default_args,
    description='Scrape standard squad stats from FBref (Weekly)',
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['stats', 'scraping', 'weekly', 'p23', 'fbref'],
) as dag:

    @task
    def task_ingest_fbref():
        from pipelines.p23.main import run_pipeline
        run_pipeline()

    task_ingest_fbref()
