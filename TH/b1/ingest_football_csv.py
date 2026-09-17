import pandas as pd
import sys

def ingest_football_data():
    print("Bắt đầu tải dữ liệu bóng đá Ngoại hạng Anh 2023-24 (từ football.csv GitHub)...")
    url = "https://raw.githubusercontent.com/footballcsv/england/master/2023-24/england.csv"
    
    try:
        # 1. Đọc dữ liệu thô (CSV) từ web
        df = pd.read_csv(url)
        print(f"Đã tải xong {len(df)} trận đấu.")
        
        # Hiển thị thử 3 dòng đầu
        print("\n--- Dữ liệu mẫu ---")
        print(df.head(3))
        print("-------------------\n")
        
        # 2. Cấu hình kết nối đến MinIO (Data Lake)
        # Nếu chạy code này bên ngoài Docker (trên máy ảo), dùng localhost:9000
        S3_OPTS = {
            "key": "minioadmin",
            "secret": "minioadmin123",
            "client_kwargs": {"endpoint_url": "http://localhost:9000"}
        }
        
        # 3. Lưu dữ liệu dưới định dạng Parquet siêu nhẹ vào bucket 'raw'
        s3_path = "s3://raw/football/england_2023_24.parquet"
        print(f"Đang đẩy dữ liệu vào MinIO tại bucket: {s3_path}...")
        
        df.to_parquet(s3_path, storage_options=S3_OPTS, index=False)
        print("✅ Hoàn tất! Ingestion thành công.")
        
    except Exception as e:
        print(f"❌ Lỗi Ingestion: {e}")
        sys.exit(1)

if __name__ == "__main__":
    ingest_football_data()
