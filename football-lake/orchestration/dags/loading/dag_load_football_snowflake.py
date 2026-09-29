from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

from common.assets import (
    football_fixtures_bronze,
    football_teams_bronze,
    football_players_bronze,
    football_fixtures_raw,
    football_teams_raw,
    football_players_raw
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_load_football_snowflake',
    default_args=default_args,
    description='Loading: Đẩy dữ liệu từ MinIO Bronze vào Snowflake RAW',
    # Chạy DAG này khi CẢ BA assets bronze được cập nhật (Asset Dependency)
    schedule=(football_fixtures_bronze & football_teams_bronze & football_players_bronze),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['loading', 'snowflake', 'raw'],
) as dag:

    @task(outlets=[football_fixtures_raw])
    def load_fixtures_to_raw():
        """Load file Parquet/JSON từ MinIO vào Snowflake RAW.FIXTURES."""
        print("Loading fixtures to Snowflake RAW...")
        # Code sử dụng CopyFromExternalStageToSnowflakeOperator hoặc custom Python code
        return "Success"

    @task(outlets=[football_teams_raw])
    def load_teams_to_raw():
        """Load file Parquet/JSON từ MinIO vào Snowflake RAW.TEAMS."""
        print("Loading teams to Snowflake RAW...")
        return "Success"

    @task(outlets=[football_players_raw])
    def load_players_to_raw():
        """Load file Parquet/JSON từ MinIO vào Snowflake RAW.PLAYERS."""
        print("Loading players to Snowflake RAW...")
        return "Success"

    # Các task chạy song song
    load_fixtures_to_raw()
    load_teams_to_raw()
    load_players_to_raw()
