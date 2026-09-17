import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Thiết lập mặc định cho DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'p09_football_data_org',
    default_args=default_args,
    description='Cào dữ liệu từ API v4 của football-data.org (Dims & Facts)',
    schedule_interval='@daily',
    start_date=datetime(2026, 9, 10),
    catchup=False,
    tags=['bronze', 'silver', 'football_data_org', 'api'],
) as dag:

    def run_football_data_ingest():
        # Gọi trực tiếp module pipeline để chạy
        from pipelines.p09.main import run_pipeline
        run_pipeline()

    task_ingest = PythonOperator(
        task_id='ingest_all_data',
        python_callable=run_football_data_ingest,
        execution_timeout=timedelta(minutes=30),
    )

    task_ingest
