from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from common.assets import (
    football_data_co_uk, wikidata, thesportsdb, football_data_org, 
    wikimedia, open_meteo, physioroom, understat, wikipedia, 
    google_news, youtube, odds_api, football_news,
    api_football_fixtures, api_football_teams, api_football_players, openliga,
    stg_football_data_co_uk, stg_wikidata, stg_thesportsdb, stg_football_data_org,
    stg_wikimedia, stg_open_meteo, stg_understat, stg_wikipedia,
    stg_google_news, stg_youtube, stg_football_news, stg_openliga,
    stg_api_football
)

def dbt_env(dag_id: str) -> dict:
    """Mỗi DAG có target/log riêng ở /tmp (không ghi vào bind-mount của host => không Permission denied,
    và các DAG chạy song song không đạp lên manifest/partial_parse của nhau)."""
    return {"DBT_TARGET_PATH": f"/tmp/dbt/{dag_id}/target", "DBT_LOG_PATH": f"/tmp/dbt/{dag_id}/logs"}


# ==============================================================================
# TẠO DAG ĐỘNG (DYNAMIC DAGs) CHO TỪNG NGUỒN ĐỘC LẬP
# Mỗi DAG chỉ kích hoạt khi MinIO Asset tương ứng của nó có dữ liệu mới,
# và khi hoàn thành sẽ phát sinh Staging Asset tương ứng (outlets).
# ==============================================================================

SOURCES = [
    ("p03_football_data_co_uk", football_data_co_uk, "stg_football_data_co_uk", stg_football_data_co_uk),
    ("p06_wikidata", wikidata, "stg_wikidata stg_wikidata_clubs stg_wikidata_players stg_wikidata_stadiums stg_wikidata_managers", stg_wikidata),
    ("p08_thesportsdb", thesportsdb, "stg_thesportsdb_teams stg_thesportsdb_players stg_thesportsdb_honors stg_thesportsdb_former_teams", stg_thesportsdb),
    ("p09_football_data_org", football_data_org, "stg_football_data_org_competitions stg_football_data_org_teams stg_football_data_org_players stg_football_data_org_matches stg_football_data_org_standings", stg_football_data_org),
    ("p10_wikimedia", wikimedia, "stg_wikimedia_pageviews stg_wikimedia_top_daily", stg_wikimedia),
    ("p13_open_meteo", open_meteo, "stg_open_meteo stg_open_meteo_forecast", stg_open_meteo),
    # ("p16_physioroom", physioroom, "stg_physioroom", None),
    ("p19_understat", understat, "stg_understat_matches stg_understat_players stg_understat_shots stg_understat_team_matches stg_understat_teams", stg_understat),
    ("p20_wikipedia", wikipedia, "stg_wikipedia", stg_wikipedia),
    ("p20_google_news", google_news, "stg_google_news", stg_google_news),
    ("p21_youtube", youtube, "stg_youtube_videos stg_youtube_comments", stg_youtube),
    # ("p22_odds", odds_api, "stg_odds_base stg_odds_h2h stg_odds_spreads stg_odds_totals", None),
    ("p25_football_news", football_news, "stg_football_news", stg_football_news),
    ("p26_openliga", openliga, "stg_openliga_leagues stg_openliga_teams stg_openliga_groups stg_openliga_locations stg_openliga_match_results stg_openliga_matches stg_openliga_goals", stg_openliga),
]

for source_name, in_asset, dbt_models, out_asset in SOURCES:
    if not dbt_models:
        continue # Bỏ qua nếu chưa khai báo dbt models cụ thể
        
    dag_id = f"dag_dbt_staging_{source_name}"
    
    dag = DAG(
        dag_id=dag_id,
        # Lắng nghe duy nhất 1 asset của luồng này (MDS chuẩn mực)
        schedule=[in_asset],
        start_date=datetime(2023, 1, 1),
        catchup=False,
        max_active_runs=1,   # 1 nguồn không chạy chồng chính nó
        max_active_tasks=1,
        default_args={"pool": "heavy_pool"},
        tags=["transformation", "dbt", "staging", "duckdb", source_name],
    )
    
    with dag:
        # Chạy dbt CHỈ CHO CÁC BẢNG STAGING CỦA NGUỒN NÀY VÀ PHÁT SINH OUTLET ASSET
        BashOperator(
            task_id=f"dbt_run_{source_name}",
            bash_command=f"cd /opt/project/dbt && dbt build --select {dbt_models} --profiles-dir .",
            env=dbt_env(dag_id), append_env=True,
            outlets=[out_asset] if out_asset else [],
        )
        
    # Đăng ký DAG vào Global scope của Airflow
    globals()[dag_id] = dag


# ==============================================================================
# P24 (API Football) - DAG kết hợp nhiều Assets
# Vì nó chia làm 3 endpoints khác nhau nên ta gom lại thành 1 DAG
# ==============================================================================
dag_p24 = DAG(
    dag_id="dag_dbt_staging_p24_api_football",
    schedule=(api_football_fixtures & api_football_teams & api_football_players),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"pool": "heavy_pool"},
    tags=["transformation", "dbt", "staging", "duckdb", "p24_api_football"],
)

with dag_p24:
    BashOperator(
        task_id="dbt_run_p24_api_football",
        bash_command="cd /opt/project/dbt && dbt build --select stg_fixtures stg_teams stg_players stg_api_football_standings --profiles-dir .",
        env=dbt_env("dag_dbt_staging_p24_api_football"), append_env=True,
        outlets=[stg_api_football],
    )

globals()["dag_dbt_staging_p24_api_football"] = dag_p24


# ==============================================================================
# BOOTSTRAP / LÀM MỚI TOÀN BỘ STAGING (chạy tay): dựng mọi file Parquet ở dwh/staging/ trong 1 lần.
# Cần chạy 1 lần trước dag_dbt_core (core đọc staging từ Parquet, không tự dựng lại staging).
# ==============================================================================
dag_all = DAG(
    dag_id="dag_dbt_staging_all",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"pool": "heavy_pool"},
    tags=["transformation", "dbt", "staging", "duckdb", "manual"],
)

with dag_all:
    BashOperator(
        task_id="dbt_build_all_staging",
        bash_command="cd /opt/project/dbt && dbt build --select path:models/staging --profiles-dir .",
        env=dbt_env("dag_dbt_staging_all"), append_env=True,
    )

globals()["dag_dbt_staging_all"] = dag_all
