from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset
from common.assets import wikimedia

asset_raw = wikimedia

with DAG(
    dag_id="dag_ingest_p10_wikimedia",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    default_args={"pool": "ingestion_pool", "retries": 3, "retry_delay": timedelta(minutes=2)},
    max_active_tasks=2,
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
