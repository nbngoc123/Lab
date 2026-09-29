from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

from common.assets import (
    football_fixtures_raw_in, 
    football_teams_raw_in, 
    football_players_raw_in
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_ingest_api_football',
    default_args=default_args,
    description='Ingestion: Lấy dữ liệu API-Football lưu vào MinIO Bronze',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_tasks=1,  # BẮT BUỘC: Chạy tuần tự để không bị Rate Limit (10 req/phút)
    tags=['ingestion', 'api_football', 'bronze'],
) as dag:

    @task
    def get_target_leagues():
        """Lấy danh sách các giải đấu cần fetch dữ liệu."""
        return [39, 140, 135, 78, 61]  # Anh, Tây Ban Nha, Ý, Đức, Pháp

    @task(outlets=[football_teams_raw_in])
    def fetch_teams(league_id: int):
        from pipelines.p24.main import ingest_teams
        return ingest_teams(league_id)

    @task(outlets=[football_fixtures_raw_in])
    def fetch_fixtures(league_id: int):
        from pipelines.p24.main import ingest_fixtures
        return ingest_fixtures(league_id)

    @task(outlets=[football_players_raw_in])
    def fetch_players(league_id: int):
        from pipelines.p24.main import ingest_players
        return ingest_players(league_id)

    leagues = get_target_leagues()
    
    teams_tasks = fetch_teams.expand(league_id=leagues)
    fixtures_tasks = fetch_fixtures.expand(league_id=leagues)
    players_tasks = fetch_players.expand(league_id=leagues)
