from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_wikidata_raw = Asset("minio://football-lake/raw/wikidata")

with DAG(
    dag_id="dag_ingest_p06_wikidata",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={"pool": "ingestion_pool"},
    tags=["ingestion", "bronze", "wikidata"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p06.main import get_partitions
        return get_partitions()

    @task(outlets=[asset_wikidata_raw])
    def task_ingest(partition: dict):
        from pipelines.p06.main import ingest
        return ingest(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
