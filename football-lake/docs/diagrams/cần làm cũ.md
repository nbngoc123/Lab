Giai đoạn 2: Thiết kế Data Warehouse (Star Schema)
Mục tiêu: Quy hoạch cấu trúc cơ sở dữ liệu để phân tích.

 Xác định các bảng Dimension (Dim): Liệt kê các thông tin danh mục từ dữ liệu Silver.
Ví dụ: Dim_Team, Dim_League, Dim_Date.
 Xác định các bảng Fact (Sự kiện): Liệt kê các bảng chứa con số đo lường.
Ví dụ: Fact_Match (lưu tỷ số), Fact_Odds (lưu tỷ lệ cược).
 Vẽ sơ đồ ERD: Vẽ nháp (ra giấy hoặc dùng tool như draw.io) cách các bảng Fact nối với Dim qua các khóa (Primary Key / Foreign Key).








Giai đoạn 3: Xây dựng lớp Gold (ELT với DuckDB)
Mục tiêu: Chạy SQL để tạo Data Warehouse thực tế.

 Cài đặt thư viện: Cài duckdb vào môi trường Python của bạn (pip install duckdb).
 Viết file build_dwh.py:
Dùng DuckDB đọc trực tiếp các file .parquet từ bucket silver.
Dùng câu lệnh SQL (CREATE TABLE ... AS SELECT) để tách dữ liệu thành các bảng Dim và Fact đã thiết kế ở GĐ 2.
 Lưu lớp Gold: Lưu kết quả thành 1 file football_dwh.duckdb duy nhất hoặc các file Parquet phân tích, để phục vụ cho các tool phía sau.









Giai đoạn 4: Tự động hóa với Apache Airflow (Orchestration)
Mục tiêu: Chạy pipeline tự động từ đầu đến cuối.

 Thiết lập DAGs: Nối các bước lại với nhau trong Airflow DAG.
Task 1: Kéo API -> Bronze.
Task 2: Pandas Clean -> Silver.
Task 3: DuckDB Transform -> Gold.
 Lên lịch chạy (Schedule): Đặt lịch cho DAG chạy hàng ngày hoặc sau mỗi vòng đấu.