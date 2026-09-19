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
    'p21_youtube_highlights',
    default_args=default_args,
    description='Ingest YouTube highlights and comments (Daily)',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nlp', 'text', 'daily', 'p21', 'youtube'],
) as dag:

    @task
    def task_ingest_youtube():
        from pipelines.p21.main import run_pipeline
        run_pipeline()

    task_ingest_youtube()
