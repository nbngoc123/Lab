from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_raw = Asset("minio://football-lake/raw/wikipedia")

with DAG(
    dag_id="dag_ingest_p20_wikipedia",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={"pool": "ingestion_pool"},
    tags=["ingestion", "bronze", "wikipedia"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p20.main import get_partitions_wiki
        return get_partitions_wiki()

    @task(outlets=[asset_raw])
    def task_ingest(partition: dict):
        from pipelines.p20.main import ingest_wiki
        return ingest_wiki(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
