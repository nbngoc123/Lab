# Hướng dẫn triển khai One Big Table (OBT) 

## 1. Khái niệm
**One Big Table (OBT)** là phương pháp thiết kế Data Warehouse thay thế cho Star Schema truyền thống (Dim/Fact). Thay vì chia nhỏ dữ liệu thành nhiều bảng và yêu cầu người dùng phải tự `JOIN` khi truy vấn, OBT sẽ gom (denormalize) tất cả các cột có liên quan vào một bảng duy nhất khổng lồ.

## 2. Ưu điểm trong kiến trúc của bạn
*   **Rất phù hợp với DuckDB:** DuckDB là database dạng cột (Columnar Database). Nó đọc và scan dữ liệu trên bảng OBT siêu nhanh mà không bị tốn dung lượng như database dạng dòng (PostgreSQL).
*   **Dễ sử dụng cho BI và ML:** Đưa 1 bảng OBT vào Power BI hoặc Pandas thì người dùng không cần biết cách viết SQL JOIN phức tạp.

## 3. Cách triển khai bằng DuckDB (Python)

Giả sử ở lớp Silver bạn có 3 file Parquet riêng biệt: `matches.parquet`, `teams.parquet`, `odds.parquet`. Bước này sẽ gộp cả 3 lại.

```python
import duckdb

# Kết nối DuckDB
con = duckdb.connect('football_dwh.duckdb')

# 1. Viết câu SQL JOIN tất cả các bảng Silver thành 1 bảng OBT duy nhất ở lớp Gold
sql_query = """
CREATE OR REPLACE TABLE OBT_Match_Master AS
SELECT 
    -- Thông tin trận đấu (Từ matches)
    m.match_id,
    m.match_date,
    m.league_name,
    m.home_goals,
    m.away_goals,
    
    -- Thông tin đội bóng (Từ teams)
    th.team_name AS home_team_name,
    th.stadium AS home_stadium,
    ta.team_name AS away_team_name,
    
    -- Thông tin tỷ lệ cược (Từ odds)
    o.bookmaker_name,
    o.odds_home_win,
    o.odds_draw,
    o.odds_away_win
    
FROM 'datalake/silver/matches/*.parquet' m
LEFT JOIN 'datalake/silver/teams/*.parquet' th ON m.home_team_id = th.team_id
LEFT JOIN 'datalake/silver/teams/*.parquet' ta ON m.away_team_id = ta.team_id
LEFT JOIN 'datalake/silver/odds/*.parquet' o ON m.match_id = o.match_id;
"""

con.execute(sql_query)

# 2. Bạn cũng có thể xuất thẳng bảng OBT này ra thành 1 file Parquet khổng lồ cho Data Scientist dùng
con.execute("COPY OBT_Match_Master TO 'datalake/gold/obt_match_master.parquet' (FORMAT PARQUET);")

con.close()
```

## 4. Kết quả
Bạn sẽ có 1 bảng `OBT_Match_Master` chứa đầy đủ ID trận đấu, tên hai đội, tên sân vận động, tỷ số, và tỷ lệ cược nằm trên cùng 1 hàng. Việc phân tích sau đó trở nên cực kỳ dễ dàng.
