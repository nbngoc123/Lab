from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import duckdb
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def transform_bronze_to_silver():
    print("Transforming OpenLiga CDC Data (Filtering season >= 2024)...")
    
    # Kết nối DuckDB
    db_dir = '/opt/project/dbt'
    os.makedirs(db_dir, exist_ok=True)
    db_path = f'{db_dir}/superset_bi.duckdb'
    con = duckdb.connect(db_path)
    
    # Cấu hình đọc S3 (MinIO)
    endpoint = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    con.execute("CREATE SCHEMA IF NOT EXISTS silver_openliga;")
    
    # 1. Transform Matches
    # Đọc tất cả parquet trong bronze/matches/, lọc season >= 2024, và lấy bản ghi mới nhất (dedup) theo matchid
    try:
        con.execute("""
            CREATE OR REPLACE TABLE silver_openliga.matches AS
            WITH raw_data AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY matchid ORDER BY _id DESC) as rn
                FROM read_parquet('s3://football-lake/bronze/matches/*.parquet')
                WHERE season >= 2024
            )
            SELECT * EXCLUDE (rn)
            FROM raw_data
            WHERE rn = 1;
        """)
        print("Transformed matches successfully.")
    except Exception as e:
        print(f"No match data yet or error: {e}")
        
    # 2. Transform Goals
    try:
        con.execute("""
            CREATE OR REPLACE TABLE silver_openliga.goals AS
            WITH raw_data AS (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY goalid ORDER BY _id DESC) as rn
                FROM read_parquet('s3://football-lake/bronze/goals/*.parquet')
                -- Filter join with matches if we only want goals from season >= 2024
            )
            SELECT * EXCLUDE (rn)
            FROM raw_data
            WHERE rn = 1
              AND matchid IN (SELECT matchid FROM silver_openliga.matches);
        """)
        print("Transformed goals successfully.")
    except Exception as e:
        print(f"No goal data yet or error: {e}")

    con.close()
    print("Transformation Complete!")

with DAG(
    'p26_transform_openliga_cdc',
    default_args=default_args,
    description='TRANSFORM: Xử lý CDC Parquet, lọc >= 2024, lưu vào Silver DuckDB',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['transform', 'silver', 'football']
) as dag:

    transform_task = PythonOperator(
        task_id='transform_bronze_to_silver',
        python_callable=transform_bronze_to_silver,
    )
