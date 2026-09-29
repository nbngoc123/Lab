from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from common.assets import odds_raw_in

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
    tags=['betting', 'odds', 'hourly', 'p22', 'bronze'],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p22.main import get_partitions
        return get_partitions()

    @task(outlets=[odds_raw_in])
    def task_ingest(partition: dict):
        from pipelines.p22.main import ingest
        return ingest(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
