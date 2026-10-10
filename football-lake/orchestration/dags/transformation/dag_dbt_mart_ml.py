from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import fact_match

# ==============================================================================
# Mart ML: đọc core + staging (Parquet trên MinIO) -> dwh/mart_ml_*.parquet
# - Chạy sau mỗi lần dag_dbt_core cập nhật fact_match (Asset)
# - team_alias + int_team_alias phải nằm trong lượt chạy: view/seed không phải Parquet nên không tự có trong DuckDB in-memory
# - Test kèm theo gồm assert_ml_no_label_leakage (cột f_ nào tương quan > 0.55 với kết quả thì FAIL)
# ==============================================================================
with DAG(
    dag_id="dag_dbt_mart_ml",
    schedule=[fact_match],
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["transformation", "dbt", "mart", "ml"],
) as dag:
    BashOperator(
        task_id="dbt_build_mart_ml",
        bash_command="cd /opt/project/dbt && dbt build --select team_alias int_team_alias path:models/mart --profiles-dir .",
        env={"DBT_TARGET_PATH": "/tmp/dbt/dag_dbt_mart_ml/target", "DBT_LOG_PATH": "/tmp/dbt/dag_dbt_mart_ml/logs"},
        append_env=True,
    )
