import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Asset

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

for dag_suffix, minio_prefix, sf_table, asset_name in SOURCES:
    dag_id = f"dag_load_{dag_suffix}_snowflake"
    in_asset = Asset(f"minio://football-lake/raw/{asset_name}")
    out_asset = Asset(f"snowflake://my_account/FOOTBALL_DWH/RAW/{sf_table}")
    
    sql_query = f'''
    COPY INTO FOOTBALL_DWH.RAW.{sf_table} (RAW_DATA, INGEST_TIMESTAMP)
    FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/{minio_prefix}/)
    FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'
    '''
    dag = DAG(
        dag_id=dag_id, default_args=default_args, schedule=[in_asset],
        start_date=datetime(2023, 1, 1), catchup=False, tags=["loading", "snowflake", "raw", dag_suffix],
    )
    with dag:
        load_task = SQLExecuteQueryOperator(task_id="load_to_snowflake", conn_id="snowflake_default", sql=sql_query, outlets=[out_asset])
    globals()[dag_id] = dag

# P24
from common.assets import football_fixtures_raw_in, football_teams_raw_in, football_players_raw_in
from common.assets import football_fixtures_raw, football_teams_raw, football_players_raw

dag_p24 = DAG(
    dag_id="dag_load_p24_api_football_snowflake", default_args=default_args,
    schedule=(football_fixtures_raw_in & football_teams_raw_in & football_players_raw_in),
    start_date=datetime(2023, 1, 1), catchup=False, tags=["loading", "snowflake", "raw", "p24_api_football"],
)

with dag_p24:
    load_f = SQLExecuteQueryOperator(
        task_id="load_fixtures", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_FIXTURES (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/fixtures/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_fixtures_raw]
    )
    load_t = SQLExecuteQueryOperator(
        task_id="load_teams", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_TEAMS (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/teams/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_teams_raw]
    )
    load_p = SQLExecuteQueryOperator(
        task_id="load_players", conn_id="snowflake_default",
        sql="COPY INTO FOOTBALL_DWH.RAW.API_FOOTBALL_PLAYERS (RAW_DATA, INGEST_TIMESTAMP) FROM (SELECT $1, CURRENT_TIMESTAMP() FROM @FOOTBALL_DWH.RAW.MINIO_RAW_STAGE/api_football/players_summary/) FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'AUTO') ON_ERROR = 'CONTINUE'",
        outlets=[football_players_raw]
    )
globals()["dag_load_p24_api_football_snowflake"] = dag_p24
