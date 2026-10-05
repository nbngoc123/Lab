from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from common.assets import openliga

try:                                    # Airflow 3.x
    from airflow.sdk.exceptions import AirflowSkipException
except ImportError:                     # Airflow 2.x
    from airflow.exceptions import AirflowSkipException

# ==============================================================================
# p26 - OpenLigaDB CDC:  Postgres football_source -> Debezium -> Kafka -> MinIO Bronze
# - Bronze = nhật ký sự kiện CDC (JSON.gz, append-only, gồm cả op='d') ở raw/openliga/<bảng>/...
# - max_active_runs=1: chỉ 1 consumer trong group tại một thời điểm (tránh rebalance giữa 2 run)
# - Không có sự kiện mới => task SKIPPED => không phát Asset => dbt staging không chạy thừa
# ==============================================================================
with DAG(
    dag_id="dag_ingest_p26_openliga",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    default_args={"pool": "ingestion_pool", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["ingestion", "bronze", "cdc", "openliga"],
) as dag:

    @task(outlets=[openliga], execution_timeout=timedelta(minutes=14))
    def task_ingest_cdc():
        from pipelines.p26.main import ingest
        stats = ingest()
        if stats["events"] == 0:
            raise AirflowSkipException("Không có sự kiện CDC mới")
        return {"events": stats["events"], "files": stats["files"], "by_table": stats["by_table"]}

    task_ingest_cdc()
