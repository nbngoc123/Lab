from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_raw = Asset("minio://football-lake/raw/thesportsdb")

with DAG(
    dag_id="dag_ingest_p08_thesportsdb",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    default_args={"pool": "ingestion_pool"},
    tags=["ingestion", "bronze", "thesportsdb"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p08.main import get_partitions
        return get_partitions()

    @task(outlets=[asset_raw])
    def task_ingest(partition: dict):
        import logging
        from airflow.exceptions import AirflowSkipException
        logger = logging.getLogger("airflow.task")
        try:
            from pipelines.p08.main import ingest
            return ingest(partition)
        except Exception as e:
            logger.warning(
                "Nguồn TheSportsDB là nguồn phụ và hiện đang gặp sự cố (%s). Bỏ qua partition %s.",
                e, partition
            )
            raise AirflowSkipException(f"TheSportsDB API không khả dụng ({e}). Bỏ qua partition này.")

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
