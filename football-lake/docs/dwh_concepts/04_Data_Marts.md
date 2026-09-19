# Hướng dẫn triển khai Data Marts (Chợ dữ liệu)

## 1. Khái niệm
**Data Mart** là một "phiên bản thu nhỏ" của Data Warehouse. Thay vì để tất cả nhân viên phân tích lao vào một cái kho dữ liệu khổng lồ (với hàng chục bảng Dim/Fact hoặc OBT), bạn cắt kho dữ liệu đó thành các mảnh nhỏ (chợ) chuyên biệt cho từng phòng ban (Ví dụ: Chợ dữ liệu Cá cược, Chợ dữ liệu Báo chí).

## 2. Ưu điểm
*   **Bảo mật & Phân quyền:** Team phân tích báo chí (Google News/Wiki) không cần xem dữ liệu Odds (Nhà cái).
*   **Dễ hiểu:** Người dùng cuối không bị "ngợp" trước hàng trăm cột dữ liệu không liên quan đến công việc của họ.

## 3. Cách triển khai bằng DuckDB

Trong DuckDB, có 2 cách phổ biến để tạo Data Mart:
1.  **Dùng View (Khung nhìn logic):** Tạo ra một khung nhìn (VIEW) từ bảng OBT lớn. View không tốn dung lượng ổ cứng.
2.  **Xuất ra file Parquet riêng biệt:** Lưu riêng các file `.parquet` ra từng thư mục khác nhau.

Dưới đây là cách triển khai bằng **View**.

```python
import duckdb

con = duckdb.connect('football_dwh.duckdb')

# 1. Tạo Data Mart cho Team Cá Cược (Betting Analytics)
# Chỉ chứa thông tin trận đấu và tỷ lệ cược (Bỏ qua cầu thủ, tin tức...)
sql_betting_mart = """
CREATE OR REPLACE VIEW Mart_Betting AS
SELECT 
    m.match_date,
    th.team_name AS home,
    ta.team_name AS away,
    o.bookmaker_name,
    o.odds_home_win,
    o.odds_away_win
FROM 'datalake/silver/matches/*.parquet' m
JOIN 'datalake/silver/odds/*.parquet' o ON m.match_id = o.match_id
JOIN 'datalake/silver/teams/*.parquet' th ON m.home_team_id = th.team_id
JOIN 'datalake/silver/teams/*.parquet' ta ON m.away_team_id = ta.team_id;
"""
con.execute(sql_betting_mart)


# 2. Tạo Data Mart cho Team Phân tích Phong độ (Performance Analytics)
# Chỉ lấy tỷ số và số bàn thắng, không lấy tỷ lệ cược nhà cái
sql_performance_mart = """
CREATE OR REPLACE VIEW Mart_Performance AS
SELECT 
    m.match_date,
    th.team_name AS home,
    ta.team_name AS away,
    m.home_goals,
    m.away_goals
FROM 'datalake/silver/matches/*.parquet' m
JOIN 'datalake/silver/teams/*.parquet' th ON m.home_team_id = th.team_id
JOIN 'datalake/silver/teams/*.parquet' ta ON m.away_team_id = ta.team_id;
"""
con.execute(sql_performance_mart)

con.close()
```

## 4. Kết quả
Bạn đã có 2 View (`Mart_Betting` và `Mart_Performance`) đóng vai trò là 2 Data Mart. Khi kết nối Power BI vào file `football_dwh.duckdb`, bạn có thể cấp quyền để người dùng chỉ thấy đúng View thuộc về chuyên môn của họ.
