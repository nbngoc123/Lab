"""DAG: Load dữ liệu đã transform (Parquet trên MinIO) vào PostgreSQL schema 'dwh'.
Chạy sau khi dag_dbt_core hoàn tất.
"""
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {"owner": "airflow", "retries": 1, "pool": "heavy_pool"}

with DAG(
    dag_id="dag_load_dwh",
    description="Load Parquet từ MinIO vào PostgreSQL schema dwh (Superset) và kích hoạt ML Inference",
    schedule="0 3 * * *",     # chạy lúc 03:00 hàng ngày, sau dag_dbt_core (02:xx)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["dwh", "load"],
) as dag:

    def _load(**ctx):
        import sys, os
        sys.path.insert(0, "/opt/project")
        from warehouse.load_to_pg import load_all
        results = load_all()
        failed = [t for t, c in results.items() if c < 0]
        if failed:
            raise RuntimeError(f"Các bảng lỗi khi load: {failed}")

    load_task = PythonOperator(
        task_id="load_parquet_to_postgres",
        python_callable=_load,
    )

    trigger_ml_inference = TriggerDagRunOperator(
        task_id="trigger_ml_inference",
        trigger_dag_id="dag_ml_inference",
        wait_for_completion=False,
    )

    trigger_text_inference = TriggerDagRunOperator(
        task_id="trigger_text_inference",
        trigger_dag_id="dag_text_inference",
        wait_for_completion=False,
    )

    load_task >> [trigger_ml_inference, trigger_text_inference]
