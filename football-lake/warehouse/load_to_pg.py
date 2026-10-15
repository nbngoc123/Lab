"""
Load dữ liệu từ MinIO (Parquet) vào PostgreSQL schema 'dwh'.

Cách hoạt động:
  DuckDB đọc Parquet từ MinIO (qua httpfs) rồi ghi thẳng vào Postgres
  qua postgres_scanner extension. Không cần load hết vào RAM.

Chạy thủ công: python warehouse/load_to_pg.py
Chạy qua Airflow: dag_load_dwh.py gọi hàm load_all()
"""
import os
import duckdb

# ── Cấu hình ──────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000") \
    .replace("http://", "").replace("https://", "")
MINIO_AK  = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SK  = os.getenv("MINIO_SECRET_KEY",  "minioadmin123")
BUCKET    = os.getenv("MINIO_BUCKET",       "football-lake")

PG_HOST   = os.getenv("PG_HOST",     "postgres")
PG_PORT   = os.getenv("PG_PORT",     "5432")
PG_DB     = os.getenv("PG_DB",       "airflow")   # dùng chung DB airflow, schema riêng
PG_USER   = os.getenv("PG_USER",     "airflow")
PG_PASS   = os.getenv("PG_PASSWORD", "airflow")
PG_SCHEMA = os.getenv("PG_DWH_SCHEMA", "dwh")

# ── Danh sách bảng cần load ────────────────────────────────────────────────────
# key = tên bảng trong Postgres, value = đường dẫn Parquet trong MinIO (wildcard ok)
TABLES = {
    # === Core (dim/fact) ===
    "dim_team":        "dwh/dim_team.parquet",
    "dim_competition": "dwh/dim_competition.parquet",
    "dim_player":      "dwh/dim_player.parquet",
    "dim_venue":       "dwh/dim_venue.parquet",
    "dim_date":        "dwh/dim_date.parquet",
    "fact_match":      "dwh/fact_match.parquet",
    "fact_team_match": "dwh/fact_team_match.parquet",
    "fact_goal":       "dwh/fact_goal.parquet",
    # === Mart ML ===
    "mart_ml_match_features":      "dwh/mart_ml_match_features.parquet",
    "mart_ml_team_match_features": "dwh/mart_ml_team_match_features.parquet",
    "mart_ml_goals":               "dwh/mart_ml_goals.parquet",
}


def _make_con() -> duckdb.DuckDBPyConnection:
    """Tạo kết nối DuckDB in-memory, cài extensions và cấu hình credentials."""
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs;  LOAD httpfs;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='{MINIO_AK}';
        SET s3_secret_access_key='{MINIO_SK}';
    """)
    return con


def _attach_postgres(con: duckdb.DuckDBPyConnection) -> None:
    """Gắn Postgres vào DuckDB như một database ngoài."""
    con.execute(f"""
        ATTACH 'host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS}'
        AS pg (TYPE POSTGRES);
    """)
    # Đảm bảo schema tồn tại
    con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{PG_SCHEMA};")


def load_table(con: duckdb.DuckDBPyConnection, table: str, parquet_path: str) -> int:
    """Load 1 bảng từ MinIO vào Postgres. Trả về số dòng đã ghi."""
    s3_path = f"s3://{BUCKET}/{parquet_path}"
    pg_table = f"pg.{PG_SCHEMA}.{table}"
    con.execute(f"DROP TABLE IF EXISTS {pg_table};")
    con.execute(f"CREATE TABLE {pg_table} AS SELECT * FROM read_parquet('{s3_path}');")
    count = con.execute(f"SELECT COUNT(*) FROM {pg_table}").fetchone()[0]
    return count


def load_all(tables: dict = None) -> dict:
    """Load tất cả bảng. Trả về dict {table: row_count}."""
    tables = tables or TABLES
    print(f"[*] Kết nối DuckDB → MinIO ({MINIO_ENDPOINT}) → Postgres ({PG_HOST}/{PG_DB}.{PG_SCHEMA})")
    con = _make_con()
    _attach_postgres(con)

    results = {}
    for table, path in tables.items():
        try:
            count = load_table(con, table, path)
            results[table] = count
            print(f"  ✓ {table}: {count:,} dòng")
        except Exception as e:
            results[table] = -1
            print(f"  ! {table}: LỖI – {e}")

    con.close()
    failed = [t for t, c in results.items() if c < 0]
    if failed:
        print(f"\n[!] {len(failed)} bảng lỗi: {failed}")
    else:
        print(f"\n[✓] Hoàn tất! {len(results)} bảng → Postgres schema '{PG_SCHEMA}'")
    return results


if __name__ == "__main__":
    load_all()
