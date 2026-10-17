"""
DAG: Football Text Intelligence Inference (Topic, Sentiment, Anomaly & Entity Extraction).
Nạp 3 mô hình NLP (Topic Clustering, Weak-Supervision Sentiment, IsolationForest Anomaly)
từ MLflow Server (qua Tailscale) hoặc Local Cache, thực hiện suy luận trên 4 nguồn tin tức/video/comment,
tính toán xu hướng 7 ngày, cảnh báo bùng phát tiêu cực, và nạp 10 bảng vào PostgreSQL schema 'dwh'.
"""
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "pool": "heavy_pool",
}

with DAG(
    dag_id="dag_text_inference",
    description="Batch Text Intelligence (Topic, Sentiment, Anomaly, Entity) từ MLflow vào DWH/Superset",
    schedule="45 3 * * *",     # Chạy lúc 03:45 hàng ngày, sau khi DWH load xong
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["ml", "text", "nlp", "inference", "superset"],
) as dag:

    def _run_text_inference(**ctx):
        import sys
        sys.path.insert(0, "/opt/project")
        from warehouse.text_inference import run_text_inference

        result = run_text_inference(publish=True)
        print("Manifest hoàn thành:", result)

    text_task = PythonOperator(
        task_id="run_text_intelligence_inference",
        python_callable=_run_text_inference,
    )
