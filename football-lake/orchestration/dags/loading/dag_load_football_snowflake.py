import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset

# Khai báo các nguồn dữ liệu cần load
# Format: (DAG ID suffix, MinIO Prefix, Snowflake Table, Asset Name)
SOURCES = [
    ("p03_football_data_co_uk", "football_data_co_uk", "FOOTBALL_DATA_CO_UK", "football_data_co_uk"),
    ("p06_wikidata", "wikidata", "WIKIDATA", "wikidata"),
    ("p08_thesportsdb", "thesportsdb", "THESPORTSDB", "thesportsdb"),
    ("p09_football_data_org", "football_data_org", "FOOTBALL_DATA_ORG", "football_data_org"),
    ("p10_wikimedia", "wikimedia", "WIKIMEDIA", "wikimedia"),
    ("p13_open_meteo", "open_meteo", "OPEN_METEO", "open_meteo"),
    ("p16_physioroom", "physioroom", "PHYSIOROOM", "physioroom"),
    ("p19_understat", "understat", "UNDERSTAT", "understat"),
    ("p20_wikipedia", "wikipedia_articles", "WIKIPEDIA", "wikipedia"),
    ("p20_google_news", "rss/google_news", "GOOGLE_NEWS", "google_news"),
    ("p21_youtube", "youtube", "YOUTUBE", "youtube"),
    ("p22_odds", "odds", "ODDS_API", "odds"),
    ("p25_football_news", "football_news", "FOOTBALL_NEWS", "football_news"),
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 1. Tự động sinh ra các DAG Load cho 13 nguồn đơn lẻ (Dynamic DAG Generation)
for dag_suffix, minio_prefix, sf_table, asset_name in SOURCES:
    dag_id = f"dag_load_{dag_suffix}_snowflake"
    
    # Kích hoạt DAG này khi Ingest ném dữ liệu vào Bronze MinIO
    in_asset = Asset(f"minio://football-lake/raw/{asset_name}")
    out_asset = Asset(f"snowflake://FOOTBALL_DWH/RAW/{sf_table}")
    
    sql_query = f"""
    COPY INTO FOOTBALL_DWH.RAW.{sf_table} (RAW_DATA, INGEST_TIMESTAMP)
    FROM (
        SELECT $1, CURRENT_TIMESTAMP() 
        FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/{minio_prefix}/
    )
    FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO')
    ON_ERROR = 'CONTINUE'
    """

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=[in_asset],
        start_date=datetime(2023, 1, 1),
        catchup=False,
        tags=["loading", "snowflake", "raw", dag_suffix],
    )

    with dag:
        load_task = SQLExecuteQueryOperator(
            task_id="load_to_snowflake",
            conn_id="snowflake_default",
            sql=sql_query,
            outlets=[out_asset]
        )
    
    # Đăng ký DAG vào global namespace để Airflow nhận diện
    globals()[dag_id] = dag


# 2. XỬ LÝ RIÊNG P24 (API-Football) VÌ NÓ CÓ 3 BẢNG CON GỘP CHUNG
in_fixtures = Asset("minio://football-lake/raw/api_football_fixtures")
in_teams = Asset("minio://football-lake/raw/api_football_teams")
in_players = Asset("minio://football-lake/raw/api_football_players")

out_fixtures = Asset("snowflake://FOOTBALL_DWH/RAW/API_FOOTBALL_FIXTURES")
out_teams = Asset("snowflake://FOOTBALL_DWH/RAW/API_FOOTBALL_TEAMS")
out_players = Asset("snowflake://FOOTBALL_DWH/RAW/API_FOOTBALL_PLAYERS")

dag_p24 = DAG(
    dag_id="dag_load_p24_api_football_snowflake",
    default_args=default_args,
    schedule=(in_fixtures & in_teams & in_players),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["loading", "snowflake", "raw", "p24_api_football"],
)

with dag_p24:
    load_f = SQLExecuteQueryOperator(
        task_id="load_fixtures",
        conn_id="snowflake_default",
        sql="""
        COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_FIXTURES (RAW_DATA, INGEST_TIMESTAMP)
        FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/fixtures/)
        FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'
        """,
        outlets=[out_fixtures]
    )
    
    load_t = SQLExecuteQueryOperator(
        task_id="load_teams",
        conn_id="snowflake_default",
        sql="""
        COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_TEAMS (RAW_DATA, INGEST_TIMESTAMP)
        FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/teams/)
        FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'
        """,
        outlets=[out_teams]
    )
    
    load_p = SQLExecuteQueryOperator(
        task_id="load_players",
        conn_id="snowflake_default",
        sql="""
        COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_PLAYERS (RAW_DATA, INGEST_TIMESTAMP)
        FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/players_summary/)
        FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'
        """,
        outlets=[out_players]
    )
    
    [load_f, load_t, load_p]

globals()["dag_load_p24_api_football_snowflake"] = dag_p24
