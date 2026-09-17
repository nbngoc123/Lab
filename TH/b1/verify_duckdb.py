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
        # Sử dụng file bóng đá của bạn (E0.csv) thay vì taxi
        csv_file = "s3://raw/E0.csv"
        
        # Câu 1: Đếm tổng số trận đấu
        total_rows = con.execute(f"SELECT count(*) FROM read_csv_auto('{csv_file}')").fetchone()[0]
        print(f"Tổng số trận đấu (Ngoại hạng Anh E0): {total_rows}\n")
        
        # Câu 2: Thống kê tổng số trận và trung bình bàn thắng sân nhà (FTHG) theo Tên Đội (HomeTeam)
        # Tương đương với việc đếm chuyến và total_amount của bài Taxi
        print("Bảng thống kê số trận và trung bình bàn thắng sân nhà theo Đội:")
        query = f'''
            SELECT 
                HomeTeam as doi_nha, 
                count(*) as so_tran, 
                round(avg(FTHG), 2) as tb_ban_thang
            FROM read_csv_auto('{csv_file}')
            GROUP BY 1 
            ORDER BY 3 DESC
            LIMIT 10
        '''
        df_result = con.execute(query).df()
        
        # In DataFrame ra dạng bảng dễ nhìn
        print(df_result.to_string(index=False))
        
    except Exception as e:
        print(f"⚠️ Lỗi truy vấn (Vui lòng đảm bảo file '{csv_file}' có tồn tại):")
        print(e)

if __name__ == "__main__":
    verify_duckdb()
