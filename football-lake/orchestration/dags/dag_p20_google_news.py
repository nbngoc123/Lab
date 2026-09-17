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
    'p20_google_news_rss',
    default_args=default_args,
    description='Ingest Google News RSS (Daily)',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nlp', 'text', 'daily', 'p20', 'rss'],
) as dag:

    @task
    def task_ingest_news():
        from pipelines.p20.main import run_news_pipeline
        run_news_pipeline()

    task_ingest_news()
