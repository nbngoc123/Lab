from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Asset

asset_football_data_bronze = Asset("minio://football-lake/bronze/football_data_couk")

with DAG(
    dag_id="dag_ingest_p03_football_data",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["ingestion", "bronze", "football_data"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.p03.main import get_partitions
        return get_partitions()

    @task(outlets=[asset_football_data_bronze])
    def task_ingest(partition: dict):
        from pipelines.p03.main import ingest
        return ingest(partition)

    # 1. Trích xuất danh sách phân mảnh dữ liệu (Division + Season)
    partitions = task_get_partitions()
    
    # 2. Sinh task song song cho từng file CSV (Dynamic Task Mapping)
    ingest_tasks = task_ingest.expand(partition=partitions)
