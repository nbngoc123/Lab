from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# Import các Asset đã định nghĩa
from common.assets import (
    football_fixtures_bronze, 
    football_teams_bronze, 
    football_players_bronze
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
    tags=['ingestion', 'api_football', 'bronze'],
) as dag:

    @task
    def get_target_leagues():
        """Lấy danh sách các giải đấu cần fetch dữ liệu."""
        return [39, 140, 135, 78, 61]  # Anh, Tây Ban Nha, Ý, Đức, Pháp

    @task(outlets=[football_teams_bronze])
    def fetch_teams(league_id: int):
        """Lấy thông tin đội bóng của một giải. Phát ra Asset: teams_bronze."""
        print(f"Fetching teams for league {league_id}...")
        # Gọi code Python ingestion thực tế ở đây:
        # from pipelines.p24.main import ingest_teams
        # return ingest_teams(league_id=league_id)
        return f"Teams fetched for {league_id}"

    @task(outlets=[football_fixtures_bronze])
    def fetch_fixtures(league_id: int):
        """Lấy lịch thi đấu/kết quả. Phát ra Asset: fixtures_bronze."""
        print(f"Fetching fixtures for league {league_id}...")
        return f"Fixtures fetched for {league_id}"

    @task(outlets=[football_players_bronze])
    def fetch_players(league_id: int):
        """Lấy thông tin cầu thủ. Phát ra Asset: players_bronze."""
        print(f"Fetching players for league {league_id}...")
        return f"Players fetched for {league_id}"

    # 1. Lấy danh sách giải đấu
    leagues = get_target_leagues()
    
    # 2. DYNAMIC TASK MAPPING: Airflow sẽ tự tạo N tasks song song cho mỗi giải đấu
    teams_tasks = fetch_teams.expand(league_id=leagues)
    fixtures_tasks = fetch_fixtures.expand(league_id=leagues)
    players_tasks = fetch_players.expand(league_id=leagues)

