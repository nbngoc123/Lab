from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import dim_team, dim_player, fact_match

# ==============================================================================
# CORE (dbt - DuckDB): dim_team, dim_player, dim_venue, dim_competition, dim_date,
#                      fact_match, fact_team_match, fact_player_season
# Chạy mỗi ngày sau khi các DAG staging đã cập nhật. Core đọc thẳng Parquet staging
# từ MinIO nhờ register_upstream_external_models() ở on-run-start.
# ==============================================================================
with DAG(
    dag_id="dag_dbt_core",
    schedule="0 2 * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transformation", "dbt", "core", "duckdb"],
) as dag:

    # Seed + core trong CÙNG 1 lần chạy: DuckDB nằm trong bộ nhớ nên bảng seed team_alias chỉ tồn tại trong lần chạy đó.
    # Staging KHÔNG được dựng lại ở đây: core đọc thẳng Parquet ở dwh/staging/ (do các DAG dbt_staging_* ghi).
    core = BashOperator(
        task_id="dbt_build_core",
        bash_command="cd /opt/project/dbt && dbt build --select team_alias path:models/core --profiles-dir .",
        env={"DBT_TARGET_PATH": "/tmp/dbt/dag_dbt_core/target", "DBT_LOG_PATH": "/tmp/dbt/dag_dbt_core/logs"},
        append_env=True,
        outlets=[dim_team, dim_player, fact_match],
    )

    create_bi = BashOperator(
        task_id="create_bi_views",
        bash_command="cd /opt/project && python create_bi_views.py",
    )

    core >> create_bi
