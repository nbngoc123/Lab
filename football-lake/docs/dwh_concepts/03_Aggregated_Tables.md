# Hướng dẫn triển khai Aggregated Tables (Bảng tổng hợp)

## 1. Khái niệm
**Aggregated Tables** (hay Roll-up Tables) là các bảng tính toán trước kết quả gom nhóm (GROUP BY). Thay vì lưu chi tiết từng sự kiện (từng trận đấu), bảng này lưu tổng số, trung bình, hoặc số đếm theo một chiều thời gian hoặc không gian nào đó (ví dụ: Theo tháng, theo mùa giải, theo đội bóng).

## 2. Ưu điểm
*   **Tốc độ Dashboard (Power BI/Tableau) bàn thờ:** Các công cụ BI cực ghét việc phải tính tổng hàng triệu dòng mỗi khi người dùng bấm Filter (Lọc). Nếu bạn tính sẵn (Pre-aggregate) trong DuckDB và chỉ đưa bảng kết quả vào BI, báo cáo sẽ load tức thì (Sub-second response).

## 3. Cách triển khai bằng DuckDB

Ví dụ bạn muốn xây dựng một Dashboard báo cáo tổng quan tình hình của từng đội bóng trong giải theo từng tháng. Bạn sẽ tạo một bảng `Agg_Team_Monthly_Stats`.

```python
import duckdb

con = duckdb.connect('football_dwh.duckdb')

# Tạo bảng Aggregated tính toán tổng số trận và bàn thắng theo Đội và Tháng
sql_aggregate = """
CREATE OR REPLACE TABLE Agg_Team_Monthly_Stats AS
SELECT 
    th.team_name,
    EXTRACT(MONTH FROM m.match_date) AS match_month,
    EXTRACT(YEAR FROM m.match_date) AS match_year,
    COUNT(m.match_id) AS total_matches_played,
    SUM(m.home_goals) AS total_goals_scored
FROM 'datalake/silver/matches/*.parquet' m
JOIN 'datalake/silver/teams/*.parquet' th ON m.home_team_id = th.team_id
GROUP BY 
    th.team_name,
    EXTRACT(YEAR FROM m.match_date),
    EXTRACT(MONTH FROM m.match_date);
"""

con.execute(sql_aggregate)

con.close()
```

## 4. Kết quả
Bảng `Agg_Team_Monthly_Stats` giờ đây chỉ có vài trăm dòng (thay vì hàng ngàn dòng chi tiết của bảng matches). Bất kỳ truy vấn nào hỏi "Tháng 12 năm 2023 đội Arsenal ghi bao nhiêu bàn" sẽ trả về kết quả gần như ngay lập tức (0.01 giây) vì nó đã được tính sẵn.
