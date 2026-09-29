from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_raw = Asset("minio://football-lake/raw/wikimedia")

with DAG(
    dag_id="dag_ingest_p10_wikimedia",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["ingestion", "bronze", "wikimedia"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p10.main import get_partitions
        return get_partitions()

    @task(outlets=[asset_raw])
    def task_ingest(partition: dict):
        from pipelines.p10.main import ingest
        return ingest(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
