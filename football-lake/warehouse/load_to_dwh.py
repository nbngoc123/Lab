import os
import duckdb
from dotenv import load_dotenv

# 1. Load các biến môi trường (credentials) từ file .env
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(env_path)

# 2. Kết nối tới file DuckDB local (kho dữ liệu của bạn)
db_path = os.path.join(os.path.dirname(__file__), 'football_dwh.duckdb')
print(f"[*] Kết nối tới Data Warehouse: {db_path}")
con = duckdb.connect(db_path)

# 3. Cài đặt và cấu hình Extension S3 cho DuckDB để đọc MinIO
print("[*] Đang cấu hình kết nối MinIO...")
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

# Lấy cấu hình từ .env
minio_endpoint = os.getenv('MINIO_ENDPOINT', 'http://localhost:9000').replace('http://', '').replace('https://', '')
minio_ak = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
minio_sk = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')

# Tạo Secret trong DuckDB (Cách chuẩn xác và bảo mật của bản DuckDB mới nhất)
con.execute(f"""
    CREATE OR REPLACE SECRET minio_secret (
        TYPE S3,
        KEY_ID '{minio_ak}',
        SECRET '{minio_sk}',
        ENDPOINT '{minio_endpoint}',
        URL_STYLE 'path',
        USE_SSL 'false'
    );
""")

# 4. Danh sách các bảng trong tầng Gold mà bạn muốn nạp
tables_to_load = [
    # OBT
    {"table_name": "obt_match_360", "path": "s3://football-lake/gold-features/gold/obt/obt_match_360.parquet"},
    # Data Marts (bạn có thể bổ sung mart_player, mart_team... vào đây)
    {"table_name": "mart_match", "path": "s3://football-lake/gold-features/gold/data_mart/mart_match.parquet"},
    {"table_name": "mart_market", "path": "s3://football-lake/gold-features/gold/data_mart/mart_market.parquet"}
]

# 5. Thực thi quá trình nạp (CREATE TABLE AS SELECT ...)
print("[*] Bắt đầu nạp dữ liệu từ MinIO vào Data Warehouse...")
for tbl in tables_to_load:
    table_name = tbl['table_name']
    s3_path = tbl['path']
    try:
        print(f"  -> Đang nạp bảng: {table_name}")
        # Xóa bảng cũ nếu đã tồn tại và tạo bảng mới bằng dữ liệu tải từ MinIO
        con.execute(f"DROP TABLE IF EXISTS {table_name};")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{s3_path}');")
        
        # In ra số lượng dòng vừa nạp
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"     [+] Thành công! Bảng '{table_name}' có {count} dòng.")
    except Exception as e:
        print(f"     [!] Lỗi khi nạp '{table_name}': File có thể chưa tồn tại trên MinIO hoặc sai đường dẫn.")
        print(f"         Chi tiết lỗi: {e}")

print("\n[*] Hoàn tất nạp dữ liệu!")
print(f"[*] Bây giờ bạn có thể dùng các công cụ BI kết nối thẳng vào file: {db_path} để vẽ biểu đồ.")
con.close()
