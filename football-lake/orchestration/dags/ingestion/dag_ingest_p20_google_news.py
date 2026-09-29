from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_raw = Asset("minio://football-lake/raw/google_news")

with DAG(
    dag_id="dag_ingest_p20_google_news",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["ingestion", "bronze", "google_news"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p20.main import get_partitions_news
        return get_partitions_news()

    @task(outlets=[asset_raw])
    def task_ingest(partition: dict):
        from pipelines.p20.main import ingest_news
        return ingest_news(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
