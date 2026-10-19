import itertools
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import (
    dim_team, dim_player, fact_match,
    stg_football_data_co_uk, stg_football_data_org,
    stg_understat, stg_openliga, stg_api_football,
    stg_open_meteo, stg_wikidata, stg_thesportsdb
)

# Danh sách 8 nguồn Staging cốt lõi
core_assets = [
    stg_football_data_co_uk,
    stg_football_data_org,
    stg_understat,
    stg_openliga,
    stg_api_football,
    stg_open_meteo,
    stg_wikidata,
    stg_thesportsdb,
]

# Điều kiện kích hoạt: Chỉ cần từ 3 nguồn Staging bất kỳ trở lên cập nhật
# (Tạo bằng tổ hợp C(8, 3) = 56 bộ 3 nối nhau bằng toán tử OR |)
_triplets = [a & b & c for a, b, c in itertools.combinations(core_assets, 3)]
core_schedule = _triplets[0]
for _t in _triplets[1:]:
    core_schedule |= _t

# ==============================================================================
# CORE (dbt - DuckDB): dim_team, dim_player, dim_venue, dim_competition, dim_date,
#                      fact_match, fact_team_match, fact_player_season
# Kích hoạt tự động khi có từ 3 nguồn Staging phục vụ Core đã cập nhật xong.
# Core đọc thẳng Parquet staging từ MinIO nhờ register_upstream_external_models() ở on-run-start.
# ==============================================================================
with DAG(
    dag_id="dag_dbt_core",
    description="Xây dựng Core DWH khi có ít nhất 3 nguồn Staging cốt lõi cập nhật",
    schedule=core_schedule,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5), "pool": "dbt_pool", "execution_timeout": timedelta(minutes=15)},
    tags=["transformation", "dbt", "core", "duckdb"],
) as dag:

    # Seed + core trong CÙNG 1 lần chạy: DuckDB nằm trong bộ nhớ nên bảng seed team_alias chỉ tồn tại trong lần chạy đó.
    # Staging KHÔNG được dựng lại ở đây: core đọc thẳng Parquet ở dwh/staging/ (do các DAG dbt_staging_* ghi).
    core = BashOperator(
        task_id="dbt_build_core",
        bash_command="cd /opt/project/dbt && dbt build --select team_alias path:models/core --exclude tag:bi --profiles-dir .",
        env={"DBT_TARGET_PATH": "/tmp/dbt/dag_dbt_core/target", "DBT_LOG_PATH": "/tmp/dbt/dag_dbt_core/logs"},
        append_env=True,
        outlets=[dim_team, dim_player, fact_match],
    )

    create_bi = BashOperator(
        task_id="create_bi_views",
        bash_command="cd /opt/project && python create_bi_views.py",
    )

    core >> create_bi
