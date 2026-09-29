"""
p25: Football News Aggregator Live
Lấy tin tức bóng đá cập nhật liên tục từ RapidAPI.
Yêu cầu: RAPIDAPI_KEY trong .env
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.
"""
import gzip
import io
import json
import os
from lake.minio_io import put_bytes, exists, today
from lake.http import get

SRC = "football-news"
D = today()
TEST_MODE = os.getenv("TEST_MODE") == "1"

def put_json_gz(key: str, obj, source: str, meta=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def get_partitions() -> list[dict]:
    return [{"query": "Premier League"}]

def ingest(partition: dict) -> str:
    query = partition["query"]
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("  ! Thiếu RAPIDAPI_KEY trong .env")
        return ""
        
    safe_q = query.replace(' ', '_').lower()
    key = f"bronze/football_news/search_{safe_q}/ingest_date={D}/news.json.gz"
    
    if exists(key):
        print(f"  · [News API] '{query}' đã có.")
        return key

    url = f"https://football-news-aggregator-live.p.rapidapi.com/news/search?query={query.replace(' ', '+')}"
    
    headers = {
        'x-rapidapi-host': "football-news-aggregator-live.p.rapidapi.com",
        'x-rapidapi-key': api_key
    }
    
    try:
        response = get(url, headers=headers, timeout=15).json()
        if not response.get("success"):
            print(f"  ! Lỗi API: {response.get('message', 'Unknown Error')}")
            return ""
            
        data = response.get("data", [])
        if TEST_MODE and data:
            data = data[:5]
            
        put_json_gz(key, data, SRC, meta={"count": len(data)})
        print(f"  ✓ [News API] '{query}': {len(data)} tin tức")
        return key
    except Exception as e:
        print(f"  ! Lỗi khi crawl Football News: {e}")
        return ""
