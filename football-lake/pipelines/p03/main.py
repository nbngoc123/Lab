"""Ingest Football-Data.co.uk: Chỉ lấy CSV raw -> MinIO (Bronze Layer)."""
import os
from lake.minio_io import put_bytes, exists, S3, BUCKET
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "football-data.co.uk"
BASE = "https://www.football-data.co.uk/mmz4281"

# Danh sách mặc định nếu không truyền từ DAG
DEFAULT_DIVISIONS = ["E0"] if TEST_MODE else ["E0", "E1", "SP1", "D1", "I1", "F1"]
DEFAULT_SEASONS = ["2526"] if TEST_MODE else ["2021", "2122", "2223", "2324", "2425", "2526"]

def get_partitions() -> list[dict]:
    """Tạo danh sách các partitions để Airflow map."""
    partitions = []
    for div in DEFAULT_DIVISIONS:
        for season in DEFAULT_SEASONS:
            partitions.append({"division": div, "season": season})
    return partitions

def ingest(partition: dict) -> str:
    """
    Task thực thi: tải 1 file CSV cho 1 giải đấu & mùa giải cụ thể.
    """
    div = partition["division"]
    season = partition["season"]
    key = f"raw/football_data_couk/{div}/season={season}/{div}.csv"
    
    if exists(key):
        print(f"  · {div}/{season} đã có trong Data Lake, bỏ qua tải lại.")
        return key
        
    url = f"{BASE}/{season}/{div}.csv"
    try:
        content = get(url).content
    except Exception as e:
        raise RuntimeError(f"Lỗi tải {url}: {e}")
        
    put_bytes(key, content, SRC, content_type="text/csv",
              meta={"division": div, "season": season, "source_url": url})
    print(f"  ✓ {div}/{season} -> {key}")
    return key
