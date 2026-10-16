from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import (
    dim_team, dim_player, fact_match,
    stg_football_data_co_uk, stg_football_data_org,
    stg_understat, stg_openliga, stg_api_football,
    stg_open_meteo, stg_wikidata, stg_thesportsdb
)

# Điều kiện kích hoạt: TẤT CẢ các nguồn Staging cốt lõi đều phải hoàn tất
core_schedule = (
    stg_football_data_co_uk
    & stg_football_data_org
    & stg_understat
    & stg_openliga
    & stg_api_football
    & stg_open_meteo
    & stg_wikidata
    & stg_thesportsdb
)

# ==============================================================================
# CORE (dbt - DuckDB): dim_team, dim_player, dim_venue, dim_competition, dim_date,
#                      fact_match, fact_team_match, fact_player_season
# Kích hoạt tự động khi toàn bộ các nguồn Staging phục vụ Core đã cập nhật xong.
# Core đọc thẳng Parquet staging từ MinIO nhờ register_upstream_external_models() ở on-run-start.
# ==============================================================================
with DAG(
    dag_id="dag_dbt_core",
    description="Xây dựng Core DWH khi toàn bộ các nguồn Staging cốt lõi hoàn tất",
    schedule=core_schedule,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "pool": "heavy_pool"},
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
