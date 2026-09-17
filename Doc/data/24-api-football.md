# P24: API-Football (Live Scores & Fixtures)

## 1. Giới thiệu
**API-Football (v3)** là API phổ biến nhất trên RapidAPI dành cho dữ liệu bóng đá. Nó chuyên trị các mảng Live Score, Fixtures (Lịch thi đấu), Lineups (Đội hình ra sân trước trận), và Events (Thẻ phạt, thay người, VAR).

## 2. Nguồn dữ liệu
- **API Endpoint**: `https://v3.football.api-sports.io/fixtures?league=39&season=2024` (Lấy của giải EPL).
- **Authentication**: Yêu cầu `RAPIDAPI_KEY` (hoặc `x-apisports-key`) trong file `.env`. Đăng ký miễn phí được 100 requests/ngày.
- **Headers**: Bắt buộc truyền `x-rapidapi-host` và `x-rapidapi-key`.

## 3. Kiến trúc Data Lake
- **Bronze Layer**: `bronze/api_football/epl_fixtures/ingest_date={YYYY-MM-DD}/fixtures.json.gz`
- **Silver Layer**:
  - `silver/matches/api_fixtures/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema*: fixture_id, date, status (FT/HT/Live), home_team_id, home_team, away_team_id, away_team, home_goals, away_goals.
  *(Có thể mở rộng thêm bảng `api_lineups` nếu có nhu cầu)*

## 4. Airflow Automation
- **DAG**: `dag_p24_api_football`
- **Schedule**: `@daily` (Chạy mỗi ngày một lần để cập nhật tỉ số các trận vừa diễn ra trong ngày hôm trước).
