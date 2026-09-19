# Hướng dẫn triển khai Feature Tables / Feature Store

## 1. Khái niệm
**Feature Tables** là những bảng đặc biệt nằm trong Data Warehouse (hoặc Feature Store) chuyên dùng để phục vụ các mô hình Machine Learning (AI). Dữ liệu trong này không phải là dữ liệu gốc, mà là các "đặc trưng" (features) đã được tính toán phức tạp (Feature Engineering).

## 2. Ưu điểm cho Data Mining
*   Máy học (Machine Learning) không tự hiểu được "Phong độ đội bóng". Bạn phải định lượng nó thành con số (ví dụ: `win_rate_last_5_games`).
*   Việc tính sẵn các features này và lưu vào Data Warehouse giúp Data Scientist chỉ việc load data vào là train model ngay, không phải viết lại code tính toán mỗi lần chạy.

## 3. Cách triển khai bằng DuckDB (SQL Window Functions)

Dưới đây là cách dùng DuckDB SQL (hỗ trợ hàm Window Functions cực mạnh) để tính toán Chuỗi thắng và Trung bình bàn thắng.

```python
import duckdb

con = duckdb.connect('football_dwh.duckdb')

# Tính toán các Feature từ bảng Fact (hoặc bảng OBT)
sql_feature_engineering = """
CREATE OR REPLACE TABLE ML_Team_Features AS
WITH Match_Stats AS (
    SELECT 
        match_date,
        home_team_id AS team_id,
        home_goals AS goals_scored,
        away_goals AS goals_conceded,
        CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END AS is_win
    FROM 'datalake/silver/matches/*.parquet'
    
    UNION ALL
    
    SELECT 
        match_date,
        away_team_id AS team_id,
        away_goals AS goals_scored,
        home_goals AS goals_conceded,
        CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END AS is_win
    FROM 'datalake/silver/matches/*.parquet'
)
SELECT 
    team_id,
    match_date,
    -- Tính tổng bàn thắng trong 5 trận gần nhất
    SUM(goals_scored) OVER (
        PARTITION BY team_id 
        ORDER BY match_date 
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS sum_goals_last_5_games,
    
    -- Tính tỷ lệ thắng trong 5 trận gần nhất
    AVG(is_win) OVER (
        PARTITION BY team_id 
        ORDER BY match_date 
        ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
    ) AS win_rate_last_5_games
    
FROM Match_Stats;
"""

con.execute(sql_feature_engineering)

con.close()
```

## 4. Kết quả
Bảng `ML_Team_Features` sẽ chứa các cột cực kỳ giá trị như tỷ lệ thắng 5 trận gần nhất, phong độ ghi bàn. Khi đưa vào các model phân loại (như Random Forest, XGBoost) để dự đoán kết quả trận tiếp theo, độ chính xác sẽ cao hơn rất nhiều.
