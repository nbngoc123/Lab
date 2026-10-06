# Cách dbt chạy trên Airflow (đọc trước khi sửa compose/profile/DAG)

| Lớp | Ai ghi | Ở đâu | Ai đọc |
|---|---|---|---|
| raw (Bronze) | các `dag_ingest_*` | `s3://football-lake/raw/...` | staging |
| staging | `dag_dbt_staging_<nguồn>` (mỗi nguồn 1 DAG, kích hoạt bằng Asset) | `dwh/staging/<stg_x>.parquet` | core, ML/NLP, BI |
| core | `dag_dbt_core` (02:00) | `dwh/<model>.parquet` | BI, ML |
| BI | `create_bi_views` | `dbt/superset_bi.duckdb` (view trỏ vào Parquet) | Superset |

## Quy tắc
1. **Không có file `.duckdb` dùng chung cho dbt.** Mỗi lần chạy dbt dùng DuckDB `:memory:`; kết quả nằm ở MinIO.
   File `.duckdb` dùng chung = khoá khi nhiều DAG chạy song song (`Could not set lock on file`) và lỗi quyền
   ghi trên bind-mount (container Airflow chạy uid 50000, thư mục dự án thuộc user khác).
2. **Không ghi vào thư mục dự án.** `target/`, `logs/` ở `/tmp/dbt/<dag_id>/` (đặt trong từng DAG), file tạm DuckDB ở `/tmp/duckdb_tmp`.
3. **Core không dựng lại staging.** Core đọc Parquet của staging nhờ `on-run-start: register_upstream_external_models()`.
   Vì vậy phải có staging trước: chạy DAG `dag_dbt_staging_all` (thủ công) một lần, rồi `dag_dbt_core`.
4. Seed `team_alias` nằm trong bộ nhớ nên **phải build cùng lượt với core**: `dbt build --select team_alias path:models/core`.
5. Vị trí file do macro `external_location` (macros/external_location.sql) quyết định. Cấu hình `+external_location`/`{name}`
   không có tác dụng trong dbt-duckdb. Staging -> `dwh/staging/`, core giữ `dwh/` như cũ.
6. Staging của nguồn chưa có dữ liệu có đúng 1 dòng giữ chỗ toàn NULL (hành vi của materialization `external`).
   Core không bị ảnh hưởng; nếu đọc staging trực tiếp (ML/BI) hãy lọc `where <khoá> is not null`.
