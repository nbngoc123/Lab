"""DAG: Transform và xây dựng Mart BI (Superset) bằng dbt và nạp vào PostgreSQL.
- Chạy tự động sau khi dag_dbt_core cập nhật fact_match (Asset).
- Chỉ build các model BI (mart_bi_team_match, mart_bi_team_season, mart_bi_match_analysis, mart_bi_player_season).
- Không build ML models -> cực kỳ nhẹ và nhanh (chỉ mất ~5-10s).
- Tự động nạp các bảng BI vào PostgreSQL schema 'dwh' (database 'football_dwh') để Superset có số liệu mới.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from common.assets import fact_match, mart_bi

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "pool": "dbt_pool",
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_dbt_mart_bi",
    description="Xây dựng Mart BI cho Superset và nạp vào PostgreSQL DWH",
    schedule=[fact_match],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args=default_args,
    tags=["transformation", "dbt", "mart", "bi", "superset"],
) as dag:

    # 1. Chạy dbt build cho riêng thư mục mart/bi
    dbt_build_bi = BashOperator(
        task_id="dbt_build_mart_bi",
        bash_command="cd /opt/project/dbt && dbt build --select path:models/mart/bi --profiles-dir .",
        env={"DBT_TARGET_PATH": "/tmp/dbt/dag_dbt_mart_bi/target", "DBT_LOG_PATH": "/tmp/dbt/dag_dbt_mart_bi/logs"},
        append_env=True,
    )

    # 2. Nạp dữ liệu Parquet vừa tạo vào PostgreSQL DWH
    def _load_bi_to_postgres(**ctx):
        import sys
        sys.path.insert(0, "/opt/project")
        from warehouse.load_to_pg import load_all, TABLES

        # Chỉ nạp các bảng Core và BI
        bi_tables = {k: v for k, v in TABLES.items() if not k.startswith("mart_ml_")}
        results = load_all(bi_tables)
        failed = [t for t, c in results.items() if c < 0]
        if failed:
            raise RuntimeError(f"Các bảng BI lỗi khi load: {failed}")

    load_to_postgres = PythonOperator(
        task_id="load_bi_to_postgres",
        python_callable=_load_bi_to_postgres,
        outlets=[mart_bi],
    )

    dbt_build_bi >> load_to_postgres
