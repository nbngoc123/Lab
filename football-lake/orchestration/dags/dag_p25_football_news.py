from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_p25_football_news',
    default_args=default_args,
    description='Cào tin tức từ Football News Aggregator (RapidAPI)',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['football_lake', 'news', 'daily'],
) as dag:

    @task
    def run_p25_pipeline():
        from pipelines.p25.main import run_pipeline
        run_pipeline()

    run_p25_pipeline()
