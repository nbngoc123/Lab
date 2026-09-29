"""
p24: API-Football (Chỉ Ingest vào Bronze Layer)
Lấy dữ liệu v3.football.api-sports.io lưu MinIO. Hỗ trợ Dynamic Mapping theo giải đấu.
"""
import os
import time
import json
import hashlib
from urllib.parse import urlparse
from lake.minio_io import put_json_gz, put_bytes, exists, today, S3, BUCKET
from lake.http import SESSION

API_KEY  = os.getenv("API_FOOTBALL_KEY", "")
HEADERS  = {"x-apisports-key": API_KEY}
BASE     = "https://v3.football.api-sports.io"
SRC      = "api-football"
D        = today()
SEASON   = 2024
SEASONS  = [2022, 2023, 2024]

def call_api(path: str, params: dict) -> dict:
    """Gọi API, kèm delay để tránh Rate Limit (10 req/phút)."""
    if not API_KEY:
        raise ValueError("Thiếu API_FOOTBALL_KEY trong .env")

    r = SESSION.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=30)
    
    # Free tier cho phép 10 req/phút => Delay 6.2s mỗi request
    time.sleep(6.2)
    
    # Xử lý lỗi API (Hết Quota ngày hoặc Rate limit)
    if r.status_code in (429, 403):
        raise RuntimeError(f"Lỗi API (Status {r.status_code}): {r.text}")
        
    r.raise_for_status()
    body = r.json()

    if body.get("errors") and body["errors"] != []:
        raise RuntimeError(f"API trả về lỗi trong body: {body['errors']}")
        
    return body

# ---------------------------------------------------------
# CÁC HÀM INGEST ĐỂ AIRFLOW GỌI (Theo từng league_id)
# ---------------------------------------------------------

def ingest_teams(league_id: int) -> str:
    key = f"raw/api_football/teams/league={league_id}/season={SEASON}/teams.json.gz"
    if exists(key):
        print(f"  · Teams cho giải {league_id} đã có, bỏ qua")
        return key
        
    body = call_api("/teams", {"league": league_id, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"], "league": league_id})
    print(f"  ✓ Đã lấy teams cho giải {league_id}")
    return key

def ingest_fixtures(league_id: int) -> str:
    # Lấy fixtures (lịch thi đấu & kết quả) cho giải đấu hiện tại
    key = f"raw/api_football/fixtures/league={league_id}/season={SEASON}/ingest_date={D}/fixtures.json.gz"
    if exists(key):
        return key

    body = call_api("/fixtures", {"league": league_id, "season": SEASON})
    put_json_gz(key, body, SRC, meta={"count": body["results"], "league": league_id})
    print(f"  ✓ Đã lấy fixtures cho giải {league_id}")
    
    # Lấy luôn Standings
    std_key = f"raw/api_football/standings/league={league_id}/season={SEASON}/ingest_date={D}/standings.json.gz"
    std_body = call_api("/standings", {"league": league_id, "season": SEASON})
    put_json_gz(std_key, std_body, SRC, meta={"league": league_id})
    print(f"  ✓ Đã lấy standings cho giải {league_id}")
    
    return key

def ingest_players(league_id: int) -> str:
    # API-Football lấy players rất tốn quota vì phải phân trang.
    # Nên cẩn thận khi dùng trong vòng lặp. Để ví dụ, lấy page 1.
    key = f"raw/api_football/players_summary/league={league_id}/season={SEASON}/ingest_date={D}/players.json.gz"
    if exists(key):
        return key
        
    body = call_api("/players", {"league": league_id, "season": SEASON, "page": 1})
    put_json_gz(key, body, SRC, meta={"count": body["results"], "league": league_id})
    print(f"  ✓ Đã lấy players summary cho giải {league_id}")
    return key
