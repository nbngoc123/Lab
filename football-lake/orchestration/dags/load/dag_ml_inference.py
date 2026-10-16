"""
DAG: ML Model Inference & Explainability (LightGBM + SHAP).
Tải mô hình từ MLflow Tracking Server (qua mạng ảo Tailscale) hoặc Local Cache Fallback,
thực hiện suy luận kết quả trận đấu (1X2), tính toán SHAP Drivers, và ghi vào
PostgreSQL DWH schema 'dwh' phục vụ demo trên Apache Superset.
"""
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "airflow",
    "retries": 2,
    "pool": "heavy_pool",
}

with DAG(
    dag_id="dag_ml_inference",
    description="Batch Inference & SHAP Explainability từ MLflow vào DWH/Superset",
    schedule="30 3 * * *",     # Chạy lúc 03:30 hàng ngày, ngay sau dag_load_dwh (03:00)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["ml", "inference", "mlflow", "superset"],
) as dag:

    def _run_inference(**ctx):
        import sys
        sys.path.insert(0, "/opt/project")
        from warehouse.ml_inference import run_pipeline

        # Chạy inference cho toàn bộ các trận đấu cần dự đoán và kiểm định
        run_pipeline(split_filter=None, limit=None)

    inference_task = PythonOperator(
        task_id="predict_and_explain_matches",
        python_callable=_run_inference,
    )
