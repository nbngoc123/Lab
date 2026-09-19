from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="p08_thesportsdb_binary_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule="0 0 1 * *",   # Hàng tháng ngày 1 (ảnh thay đổi ít)
    catchup=False,
    tags=["thesportsdb", "binary", "bronze", "silver", "multi-league"],
) as dag:

    @task
    def run_p08_pipeline():
        from pipelines.p08.main import run_pipeline
        run_pipeline()

    run_p08_pipeline()
