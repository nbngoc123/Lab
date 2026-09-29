import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Asset

# ==============================================================================
# SCRIPT CHUYỂN TIẾP (MIDDLEMAN): MINIO -> PYTHON -> SNOWFLAKE INTERNAL STAGE
# Bỏ qua giới hạn Endpoint bị block của Snowflake khi dùng s3compat.
# ==============================================================================

SOURCES = [
    ("p03_football_data_co_uk", "raw/football_data_co_uk", "FOOTBALL_DATA_CO_UK", "football_data_co_uk"),
    ("p06_wikidata", "raw/wikidata", "WIKIDATA", "wikidata"),
    ("p08_thesportsdb", "raw/thesportsdb", "THESPORTSDB", "thesportsdb"),
    ("p09_football_data_org", "raw/football_data_org", "FOOTBALL_DATA_ORG", "football_data_org"),
    ("p10_wikimedia", "raw/wikimedia", "WIKIMEDIA", "wikimedia"),
    ("p13_open_meteo", "raw/open_meteo", "OPEN_METEO", "open_meteo"),
    ("p16_physioroom", "raw/physioroom", "PHYSIOROOM", "physioroom"),
    ("p19_understat", "raw/understat", "UNDERSTAT", "understat"),
    ("p20_wikipedia", "raw/wikipedia", "WIKIPEDIA", "wikipedia"),
    ("p20_google_news", "raw/google_news", "GOOGLE_NEWS", "google_news"),
    ("p21_youtube", "raw/youtube", "YOUTUBE", "youtube"),
    ("p22_odds", "raw/odds", "ODDS_API", "odds"),
    ("p25_football_news", "raw/football_news", "FOOTBALL_NEWS", "football_news"),
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transfer_minio_to_stage(prefix: str):
    """Tải tất cả file từ MinIO thuộc prefix, lưu tạm, rồi PUT lên Internal Stage."""
    from lake.minio_io import list_keys, read_bytes
    from pathlib import Path
    
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    tmp_dir = Path("/tmp/snowflake_upload")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        count = 0
        for key, size in list_keys(prefix):
            # Tải file từ MinIO xuống disk của Airflow
            file_data = read_bytes(key)
            local_file = tmp_dir / key.replace("/", "_")
            with open(local_file, "wb") as f:
                f.write(file_data)
                
            # PUT lên Snowflake Internal Stage (@MINIO_RAW_STAGE/prefix/)
            # Dùng file:// tuyệt đối
            put_sql = f"PUT file://{local_file.resolve()} @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/{prefix} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            cursor.execute(put_sql)
            
            # Xóa file local sau khi PUT xong
            local_file.unlink()
            count += 1
            
        print(f"✅ Đã transfer thành công {count} files từ {prefix} lên Internal Stage.")
    finally:
        cursor.close()
        conn.close()

# ---------------------------------------------------------
# Tạo 13 DAGs động
# ---------------------------------------------------------
for dag_suffix, minio_prefix, sf_table, asset_name in SOURCES:
    dag_id = f"dag_load_{dag_suffix}_snowflake"
    in_asset = Asset(f"minio://football-lake/raw/{asset_name}")
    out_asset = Asset(f"snowflake://my_account/FOOTBALL_DWH/RAW/{sf_table}")
    
    dag = DAG(
        dag_id=dag_id, 
        default_args=default_args, 
        schedule=[in_asset],
        start_date=datetime(2023, 1, 1), 
        catchup=False, 
        tags=["loading", "snowflake", "raw", dag_suffix],
    )
    
    with dag:
        @task(task_id="transfer_to_internal_stage")
        def run_transfer(p=minio_prefix):
            transfer_minio_to_stage(p)
            
        load_task = SQLExecuteQueryOperator(
            task_id="copy_into_table", 
            conn_id="snowflake_default", 
            sql=f"""
            COPY INTO FOOTBALL_DWH.RAW.{sf_table} (RAW_DATA, INGEST_TIMESTAMP)
            FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/{minio_prefix}/)
            FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'
            """, 
            outlets=[out_asset]
        )
        
        run_transfer() >> load_task

    globals()[dag_id] = dag

# ---------------------------------------------------------
# P24 (API Football) - DAG thủ công vì có 3 tables
# ---------------------------------------------------------
from common.assets import football_fixtures_raw_in, football_teams_raw_in, football_players_raw_in
from common.assets import football_fixtures_raw, football_teams_raw, football_players_raw

dag_p24 = DAG(
    dag_id="dag_load_p24_api_football_snowflake", 
    default_args=default_args,
    schedule=(football_fixtures_raw_in & football_teams_raw_in & football_players_raw_in),
    start_date=datetime(2023, 1, 1), 
    catchup=False, 
    tags=["loading", "snowflake", "raw", "p24_api_football"],
)

with dag_p24:
    @task(task_id="transfer_fixtures")
    def tf_fixtures(): transfer_minio_to_stage("raw/api_football/fixtures")
    
    @task(task_id="transfer_teams")
    def tf_teams(): transfer_minio_to_stage("raw/api_football/teams")
    
    @task(task_id="transfer_players")
    def tf_players(): transfer_minio_to_stage("raw/api_football/players_summary")

    load_f = SQLExecuteQueryOperator(
        task_id="copy_fixtures", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_FIXTURES (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/raw/api_football/fixtures/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_fixtures_raw]
    )
    load_t = SQLExecuteQueryOperator(
        task_id="copy_teams", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_TEAMS (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/raw/api_football/teams/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_teams_raw]
    )
    load_p = SQLExecuteQueryOperator(
        task_id="copy_players", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_PLAYERS (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/raw/api_football/players_summary/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_players_raw]
    )
    
    tf_fixtures() >> load_f
    tf_teams() >> load_t
    tf_players() >> load_p

globals()["dag_load_p24_api_football_snowflake"] = dag_p24
