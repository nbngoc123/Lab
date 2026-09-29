from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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

# Câu lệnh COPY INTO cho từng bảng. Giả sử Snowflake đã có:
# - Database: FOOTBALL_DWH
# - Schema: RAW
# - Stage: MINIO_BRONZE_STAGE (kết nối tới s3://football-lake/bronze)
# - File format: JSON_GZ_FORMAT (TYPE = 'JSON', COMPRESSION = 'GZIP')

COPY_FIXTURES_SQL = """
COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_FIXTURES (RAW_DATA, INGEST_TIMESTAMP)
FROM (
    SELECT $1, CURRENT_TIMESTAMP() 
    FROM @FOOTBALL_DWH.RAW.MINIO_BRONZE_STAGE/api_football/fixtures/
)
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO')
ON_ERROR = 'CONTINUE'
"""

COPY_TEAMS_SQL = """
COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_TEAMS (RAW_DATA, INGEST_TIMESTAMP)
FROM (
    SELECT $1, CURRENT_TIMESTAMP() 
    FROM @FOOTBALL_DWH.RAW.MINIO_BRONZE_STAGE/api_football/teams/
)
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO')
ON_ERROR = 'CONTINUE'
"""

COPY_PLAYERS_SQL = """
COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_PLAYERS (RAW_DATA, INGEST_TIMESTAMP)
FROM (
    SELECT $1, CURRENT_TIMESTAMP() 
    FROM @FOOTBALL_DWH.RAW.MINIO_BRONZE_STAGE/api_football/players_summary/
)
FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO')
ON_ERROR = 'CONTINUE'
"""

with DAG(
    'dag_load_football_snowflake',
    default_args=default_args,
    description='Loading: Đẩy dữ liệu từ MinIO Bronze vào Snowflake RAW (COPY INTO)',
    # DAG này tự động chạy khi CẢ BA assets bronze được cập nhật hoàn tất
    schedule=(football_fixtures_bronze & football_teams_bronze & football_players_bronze),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['loading', 'snowflake', 'raw', 'api_football'],
) as dag:

    load_fixtures_to_raw = SQLExecuteQueryOperator(
        task_id="load_fixtures_to_raw",
        conn_id="snowflake_default", # Tên connection cấu hình trên giao diện Airflow
        sql=COPY_FIXTURES_SQL,
        outlets=[football_fixtures_raw]
    )

    load_teams_to_raw = SQLExecuteQueryOperator(
        task_id="load_teams_to_raw",
        conn_id="snowflake_default",
        sql=COPY_TEAMS_SQL,
        outlets=[football_teams_raw]
    )

    load_players_to_raw = SQLExecuteQueryOperator(
        task_id="load_players_to_raw",
        conn_id="snowflake_default",
        sql=COPY_PLAYERS_SQL,
        outlets=[football_players_raw]
    )

    # Chạy song song 3 task
    [load_fixtures_to_raw, load_teams_to_raw, load_players_to_raw]
