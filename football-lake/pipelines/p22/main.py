"""
p22: The Odds API (Betting Odds)
Chỉ Ingest (Bronze Layer) - Lấy tỉ lệ kèo H2H, Spread (Handicap), Totals (Over/Under) từ các nhà cái.
Yêu cầu: ODDS_API_KEY trong .env
"""
import os
from lake.minio_io import put_json_gz, exists, today, S3, BUCKET
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "the-odds-api"
D = today()

DEFAULT_SPORTS = ["soccer_epl"] if TEST_MODE else [
    "soccer_epl", 
    "soccer_spain_la_liga", 
    "soccer_italy_serie_a", 
    "soccer_germany_bundesliga", 
    "soccer_france_ligue_one"
]

def get_partitions() -> list[dict]:
    """Tạo danh sách các partitions để Airflow map (theo sport)."""
    return [{"sport": sport} for sport in DEFAULT_SPORTS]

def ingest(partition: dict) -> str:
    """Task thực thi: tải file json cho 1 giải đấu cụ thể."""
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise ValueError("Thiếu ODDS_API_KEY trong .env")
        
    sport = partition["sport"]
    key = f"raw/odds/{sport}/ingest_date={D}/odds.json.gz"
    
    if exists(key):
        print(f"  · {sport} đã có trong Data Lake ngày {D}, bỏ qua tải lại.")
        return key

    regions = "uk,eu"
    markets = "h2h,spreads,totals"
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={api_key}&regions={regions}&markets={markets}"
    
    print(f"  [Odds] Fetching from {sport}...")
    try:
        data = get(url, timeout=15).json()
        if isinstance(data, dict) and "message" in data:
            raise RuntimeError(f"Lỗi API: {data['message']}")
            
        if TEST_MODE and isinstance(data, list):
            data = data[:2]
            
        put_json_gz(
            key,
            data, SRC, meta={"count": len(data) if isinstance(data, list) else 0, "sport": sport}
        )
        print(f"  ✓ {sport} -> {key}")
        return key
    except Exception as e:
        raise RuntimeError(f"Lỗi khi crawl The Odds API cho {sport}: {e}")
