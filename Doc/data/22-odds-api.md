# P22: The Odds API (Betting & Kèo nhà cái)

## 1. Giới thiệu
Pipeline này kết nối với **The Odds API** (`the-odds-api.com`) để lấy dữ liệu tỉ lệ cá cược trước trận đấu của toàn bộ các nhà cái lớn (Pinnacle, Betfair, DraftKings...). Đây là dữ liệu không thể thiếu nếu bạn muốn build model dự đoán kết quả hoặc đánh Arbitrage.

## 2. Nguồn dữ liệu
- **API Endpoint**: `https://api.the-odds-api.com/v4/sports/soccer_epl/odds/`
- **Authentication**: Yêu cầu `ODDS_API_KEY` trong file `.env`. (Đăng ký free được 500 requests/tháng).
- **Regions**: `uk` (Các nhà cái Anh Quốc).
- **Markets**:
  - `h2h` (Head-to-Head): Tỉ lệ kèo Châu Âu (Thắng/Hòa/Thua).
  - `spreads`: Kèo chấp Châu Á (Handicap).
  - `totals`: Kèo Tài/Xỉu (Over/Under).

## 3. Kiến trúc Data Lake
- **Bronze Layer**: `bronze/odds/epl/ingest_date={YYYY-MM-DD}/odds.json.gz` (Lưu cục JSON chứa tất cả bookmakers).
- **Silver Layer**: 
  - `silver/betting/odds_h2h/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema H2H*: match_id, home_team, away_team, commence_time, bookmaker, last_update, odds_home, odds_draw, odds_away.

## 4. Airflow Automation
- **DAG**: `dag_p22_odds_api`
- **Schedule**: `@hourly` (Lấy mỗi giờ một lần để theo dõi biến động kèo — Line Movement).
