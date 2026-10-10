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

## Mart ML (`dbt/models/mart`, DAG `dag_dbt_mart_ml`)
- Đọc **core** (fact_match, fact_team_match, dim_*, fact_goal, fact_player_season) và **staging** (pageviews, text, understat shots).
- Quy ước cột: `f_*` = đặc trưng chỉ dùng thông tin trước giờ đá, `y_*` = nhãn/số liệu sau trận. Huấn luyện chỉ lấy `f_*` (xem `ml/training/train.py`).
- Bảng chính `mart_ml_match_features`; bảng phụ `mart_ml_match_attention` (lượt xem Wikipedia, join theo match_key);
  kho văn bản `mart_ml_text_docs` **độc lập** (không nối đội/trận, dùng cho NLP); `mart_ml_goals`, `mart_ml_shots`, `mart_ml_player_season_features`.
- Test `assert_ml_no_label_leakage` chặn rò rỉ: đặc trưng nào tương quan > 0.55 với hiệu số bàn thắng thì build FAIL
  (đã bắt được cột forecast Understat thực chất tính từ xG của chính trận: tương quan 0.64).
- Độ phủ đo trên dữ liệu thật: odds chỉ 6 giải lớn + Championship; xG chỉ giải Understat; thời tiết chỉ EPL/Championship;
  ~9.000 trận OpenLiga (BL2/BL3/giải khu vực) chỉ có đặc trưng phong độ. Đặc trưng NULL = nguồn không phủ, không phải 0.
