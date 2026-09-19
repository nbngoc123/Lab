from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'p20_wikipedia_text',
    default_args=default_args,
    description='Ingest Wikipedia football articles (Weekly)',
    schedule='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nlp', 'text', 'weekly', 'p20'],
) as dag:

    @task
    def task_ingest_wikipedia():
        from pipelines.p20.main import run_wikipedia_pipeline
        run_wikipedia_pipeline()

    task_ingest_wikipedia()
