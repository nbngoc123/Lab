import os
import time
import json
import boto3
import duckdb
import requests
from prometheus_client import start_http_server, Gauge

# --- Cấu hình Metrics ---
LAKE_FILE_AGE = Gauge("lake_file_age_seconds", "Độ trễ của file mới nhất (giây)", ["layer", "source"])
LAKE_FILE_SIZE = Gauge("lake_file_size_bytes", "Dung lượng file mới nhất (bytes)", ["layer", "source"])
LAKE_TABLE_ROWS = Gauge("lake_table_rows", "Số dòng thật sự của bảng (trừ placeholder)", ["table"])
DEBEZIUM_STATUS = Gauge("debezium_connector_status", "Trạng thái connector CDC (1=RUNNING, 0=FAILED)", ["connector"])
DBT_TEST_FAILURES = Gauge("dbt_test_failures", "Số lượng test dbt thất bại", ["model"])

# --- Cấu hình Kết nối ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "football-lake")
DEBEZIUM_URL = os.getenv("DEBEZIUM_URL", "http://debezium:8083")
DBT_TARGET_DIR = os.getenv("DBT_TARGET_DIR", "/tmp/dbt/dag_dbt_core/target") # Chỗ này cấu hình mount volume

s3_client = boto3.client('s3', endpoint_url=MINIO_ENDPOINT, aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET)

def check_minio_freshness():
    """Kiểm tra độ tươi của file trong MinIO"""
    try:
        # Lấy tất cả object trong bucket
        response = s3_client.list_objects_v2(Bucket=MINIO_BUCKET)
        if 'Contents' not in response:
            return

        now = time.time()
        for obj in response['Contents']:
            key = obj['Key']
            # Phân tích đường dẫn: raw/p26_openliga/..., dwh/staging/..., dwh/...
            parts = key.split('/')
            layer = parts[0]
            if layer == 'raw' and len(parts) > 1:
                source = parts[1]
            elif layer == 'dwh' and len(parts) > 1:
                source = 'staging' if parts[1] == 'staging' else 'core'
            else:
                continue

            last_modified = obj['LastModified'].timestamp()
            age = now - last_modified
            size = obj['Size']

            LAKE_FILE_AGE.labels(layer=layer, source=source).set(age)
            LAKE_FILE_SIZE.labels(layer=layer, source=source).set(size)
    except Exception as e:
        print(f"Error checking MinIO: {e}")

def check_duckdb_rows():
    """Dùng DuckDB đọc file Parquet đếm số dòng (rất nhanh vì không load lên RAM)"""
    try:
        # Tự kết nối s3
        con = duckdb.connect(':memory:')
        con.execute(f"INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{MINIO_ENDPOINT.replace('http://','')}';")
        con.execute(f"SET s3_access_key_id='{MINIO_ACCESS}';")
        con.execute(f"SET s3_secret_access_key='{MINIO_SECRET}';")
        con.execute("SET s3_url_style='path';")

        # Quét dwh/staging
        tables = ['stg_openliga_matches', 'dim_team', 'fact_match'] # Có thể tự động list file
        for table in tables:
            try:
                # Đếm số dòng mà bỏ qua dòng placeholder (thường có khóa chính là NULL)
                # Ví dụ query đếm count(*)
                # Ở đây đếm thử trực tiếp
                query = f"SELECT count(*) FROM read_parquet('s3://{MINIO_BUCKET}/dwh/staging/{table}.parquet') WHERE ingest_date IS NOT NULL"
                if not table.startswith('stg_'):
                    query = f"SELECT count(*) FROM read_parquet('s3://{MINIO_BUCKET}/dwh/{table}.parquet')"
                
                res = con.execute(query).fetchone()[0]
                LAKE_TABLE_ROWS.labels(table=table).set(res)
            except Exception:
                LAKE_TABLE_ROWS.labels(table=table).set(0) # File chưa có hoặc lỗi
    except Exception as e:
        print(f"Error checking DuckDB: {e}")

def check_debezium():
    """Kiểm tra CDC Debezium"""
    try:
        res = requests.get(f"{DEBEZIUM_URL}/connectors", timeout=5)
        if res.status_code == 200:
            connectors = res.json()
            for conn in connectors:
                status_res = requests.get(f"{DEBEZIUM_URL}/connectors/{conn}/status", timeout=5).json()
                state = status_res.get('connector', {}).get('state', 'UNKNOWN')
                val = 1 if state == 'RUNNING' else 0
                DEBEZIUM_STATUS.labels(connector=conn).set(val)
    except Exception as e:
        print(f"Error checking Debezium: {e}")

def scan():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Bắt đầu quét metrics...")
    check_minio_freshness()
    check_duckdb_rows()
    check_debezium()

if __name__ == '__main__':
    start_http_server(9108)
    print("Lake Exporter đang chạy tại port 9108...")
    while True:
        scan()
        time.sleep(120) # 2 phút quét 1 lần
