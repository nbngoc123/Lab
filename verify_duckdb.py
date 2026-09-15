import duckdb
import s3fs

def verify_duckdb():
    print("=== BẮT ĐẦU KIỂM CHỨNG DỮ LIỆU BẰNG DUCKDB ===\n")
    
    # 1. Cấu hình MinIO
    S3_ENDPOINT = "localhost:9000"
    S3_KEY = "minioadmin"
    S3_SECRET = "minioadmin123"
    
    # 2. Khởi tạo S3FileSystem để đếm file trong tầng raw
    print("1. Liệt kê các object trong tầng 'raw':")
    try:
        fs = s3fs.S3FileSystem(key=S3_KEY, secret=S3_SECRET, 
                               client_kwargs={"endpoint_url": f"http://{S3_ENDPOINT}"})
        objects = fs.find("raw")
        for obj in objects:
            print(f" - {obj}")
    except Exception as e:
        print(f"Lỗi khi liệt kê file trên MinIO: {e}")
        
    print("\n-------------------------------------------------\n")
        
    # 3. Khởi tạo DuckDB và cấu hình plugin httpfs (truy cập S3 trực tiếp)
    print("2. Truy vấn trực tiếp file Parquet trên MinIO bằng DuckDB:")
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Tạo Secret để DuckDB có quyền đọc từ MinIO
    con.execute(f'''
        CREATE OR REPLACE SECRET minio (
            TYPE s3, 
            KEY_ID '{S3_KEY}',
            SECRET '{S3_SECRET}', 
            ENDPOINT '{S3_ENDPOINT}',
            USE_SSL false, 
            URL_STYLE 'path'
        );
    ''')
    
    try:
        # Đường dẫn tới file dữ liệu bạn cần kiểm chứng (Sửa lại tên file nếu cần)
        parquet_file = "s3://raw/nyc_taxi/yellow_taxi.parquet"
        
        # Câu 1: Đếm tổng số chuyến
        total_rows = con.execute(f"SELECT count(*) FROM read_parquet('{parquet_file}')").fetchone()[0]
        print(f"Số chuyến: {total_rows}\n")
        
        # Câu 2: Tổng hợp số chuyến và giá trị trung bình theo payment_type
        print("Bảng tổng hợp theo payment_type:")
        query = f'''
            SELECT 
                payment_type, 
                count(*) as so_chuyen, 
                round(avg(total_amount), 2) as tb_tien
            FROM read_parquet('{parquet_file}')
            GROUP BY 1 
            ORDER BY 2 DESC
        '''
        df_result = con.execute(query).df()
        
        # In DataFrame ra dạng bảng dễ nhìn
        print(df_result.to_string(index=False))
        
    except Exception as e:
        print(f"⚠️ Lỗi truy vấn (Vui lòng đảm bảo file '{parquet_file}' đã được Ingest vào MinIO thành công):")
        print(e)

if __name__ == "__main__":
    verify_duckdb()
