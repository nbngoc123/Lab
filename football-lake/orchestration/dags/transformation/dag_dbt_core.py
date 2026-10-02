from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import dim_team, dim_player, fact_match

# ==============================================================================
# CORE (dbt - DuckDB): dim_team, dim_player, dim_venue, dim_competition, dim_date,
#                      fact_match, fact_team_match, fact_player_season
# Chạy mỗi ngày sau khi các DAG staging đã cập nhật. Staging là view/table đọc thẳng raw trên
# MinIO nên không cần chờ asset cụ thể; nguồn nào chưa có file raw thì staging trả bảng rỗng.
# ==============================================================================
with DAG(
    dag_id="dag_dbt_core",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["transformation", "dbt", "core", "duckdb"],
) as dag:

    seed = BashOperator(
        task_id="dbt_seed_team_alias",
        bash_command="cd /opt/project/dbt && dbt seed --select team_alias --profiles-dir .",
    )

    core = BashOperator(
        task_id="dbt_build_core",
        bash_command="cd /opt/project/dbt && dbt build --select path:models/core --profiles-dir .",
        outlets=[dim_team, dim_player, fact_match],
    )

    create_bi = BashOperator(
        task_id="create_bi_views",
        bash_command="cd /opt/project && python create_bi_views.py",
    )

    seed >> core >> create_bi
