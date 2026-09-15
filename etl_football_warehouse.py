import pandas as pd
from sqlalchemy import create_engine
import s3fs
import sys

def run_etl():
    print("Bắt đầu quá trình ETL: MinIO (Raw) -> PostgreSQL (Warehouse)")
    
    # 1. CẤU HÌNH KẾT NỐI
    # Kết nối tới MinIO
    S3_OPTS = {
        "key": "minioadmin",
        "secret": "minioadmin123",
        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
    }
    
    # Kết nối tới PostgreSQL (Data Warehouse)
    # Định dạng: postgresql+psycopg2://user:password@host:port/dbname
    POSTGRES_URI = "postgresql+psycopg2://dataeng:dataeng123@localhost:5432/dwh"
    engine = create_engine(POSTGRES_URI)
    
    # 2. EXTRACT (ĐỌC DỮ LIỆU TỪ MINIO)
    # Dựa trên ảnh chụp của bạn, có 3 file trong bucket raw: E0.csv, E1.csv, E2.csv
    files_to_process = {
        "Premier League": "s3://raw/E0.csv",
        "Championship": "s3://raw/E1.csv",
        "League One": "s3://raw/E2.csv"
    }
    
    all_data = []
    
    for league, path in files_to_process.items():
        try:
            print(f"Đang đọc dữ liệu {league} từ {path}...")
            # Đọc thẳng CSV từ S3 (MinIO)
            df = pd.read_csv(path, storage_options=S3_OPTS)
            # Gắn thêm nhãn (label) để phân biệt giải đấu khi gộp chung bảng
            df['League_Name'] = league
            all_data.append(df)
        except Exception as e:
            print(f"⚠️ Bỏ qua {path} do lỗi: {e}")
    
    if not all_data:
        print("❌ Không có dữ liệu nào được đọc. Vui lòng kiểm tra lại MinIO.")
        sys.exit(1)
        
    # 3. TRANSFORM (LÀM SẠCH VÀ CHUẨN HÓA DỮ LIỆU)
    print("Đang xử lý (Transform) dữ liệu...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Chọn ra các cột quan trọng thay vì lấy tất cả các cột thừa
    # (Tên cột này dựa theo chuẩn file CSV của Football-Data.co.uk)
    cols_to_keep = ['League_Name', 'Div', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'Referee']
    
    # Chỉ giữ các cột thực sự có trong file (tránh báo lỗi nếu file thiếu cột Time hay Referee)
    existing_cols = [col for col in cols_to_keep if col in combined_df.columns]
    clean_df = combined_df[existing_cols].copy()
    
    # Đổi tên cột thành chuẩn Database (chữ thường, có gạch dưới)
    clean_df.rename(columns={
        'League_Name': 'league_name',
        'Div': 'division',
        'Date': 'match_date',
        'Time': 'match_time',
        'HomeTeam': 'home_team',
        'AwayTeam': 'away_team',
        'FTHG': 'full_time_home_goals',  # Full Time Home Team Goals
        'FTAG': 'full_time_away_goals',  # Full Time Away Team Goals
        'FTR': 'full_time_result'        # Full Time Result (H=Home Win, D=Draw, A=Away Win)
    }, inplace=True)
    
    # Ép kiểu dữ liệu cho cột ngày tháng
    if 'match_date' in clean_df.columns:
        # File anh thường dùng định dạng dd/mm/yyyy nên dayfirst=True
        clean_df['match_date'] = pd.to_datetime(clean_df['match_date'], dayfirst=True, errors='coerce')
        
    print(f"Đã làm sạch xong. Tổng số dòng: {len(clean_df)}")
    print("\n--- Dữ liệu chuẩn bị nạp vào Database ---")
    print(clean_df.head(3))
    print("-----------------------------------------\n")
    
    # 4. LOAD (GHI DỮ LIỆU VÀO DATA WAREHOUSE)
    table_name = "fact_match_results"
    print(f"Đang ghi dữ liệu vào PostgreSQL - bảng '{table_name}'...")
    
    try:
        # Ghi đè bảng nếu đã tồn tại (if_exists='replace')
        # Nếu muốn cộng dồn dữ liệu, dùng if_exists='append'
        clean_df.to_sql(
            name=table_name,
            con=engine,
            if_exists='replace',
            index=False
        )
        print("✅ HOÀN TẤT! Dữ liệu đã sẵn sàng trong Data Warehouse để vẽ Dashboard.")
    except Exception as e:
        print(f"❌ Lỗi khi ghi vào PostgreSQL: {e}")

if __name__ == "__main__":
    run_etl()
