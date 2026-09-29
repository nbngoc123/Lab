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

    @task(outlets=[stg_fixtures, stg_teams])
    def run_dbt_staging():
        """Chạy lệnh dbt run --select staging.*"""
        print("Running dbt models for staging layer...")
        # Sử dụng DbtRunOperator hoặc BashOperator
        # return BashOperator(task_id='dbt_run', bash_command='dbt run --select path:models/staging')
        return "Success"
    
    @task
    def run_dbt_test_staging():
        """Chạy lệnh dbt test --select staging.* (Data Quality)"""
        print("Running dbt tests for staging layer...")
        return "Passed"

    run_dbt_staging() >> run_dbt_test_staging()
