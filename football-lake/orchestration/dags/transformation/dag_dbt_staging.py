from datetime import datetime
from airflow import DAG
from airflow.decorators import task

from common.assets import (
    football_fixtures_raw,
    football_teams_raw,
    football_players_raw,
    stg_fixtures,
    stg_teams
)

with DAG(
    'dag_dbt_staging',
    # DAG này chạy khi các RAW assets đã có mặt
    schedule=(football_fixtures_raw & football_teams_raw & football_players_raw),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['transformation', 'dbt', 'staging'],
) as dag:

    from airflow.operators.bash import BashOperator

    run_dbt_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='cd /opt/project/dbt && dbt build --select staging --profiles-dir .',
        outlets=[stg_fixtures, stg_teams]
    )

    run_dbt_staging
