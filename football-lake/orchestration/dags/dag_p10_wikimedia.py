from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="p10_wikimedia_pageviews",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["wikimedia", "pageviews", "bronze", "silver", "timeseries"],
) as dag:

    @task(execution_timeout=None)
    def run_p10_pipeline():
        from pipelines.p10.main import run_pipeline
        run_pipeline()

    run_p10_pipeline()
