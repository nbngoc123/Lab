"""
p25: Football News Aggregator Live
Lấy tin tức bóng đá cập nhật liên tục từ RapidAPI.
Yêu cầu: RAPIDAPI_KEY trong .env
"""
import gzip
import io
import json
import os
import pandas as pd
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import get

SRC = "football-news"
D = today()
TEST_MODE = False

def put_json_gz(key: str, obj, source: str, meta=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def ingest_news():
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("  ! Thiếu RAPIDAPI_KEY trong .env")
        return []
        
    query = "Premier League"
    url = f"https://football-news-aggregator-live.p.rapidapi.com/news/search?query={query.replace(' ', '+')}"
    
    headers = {
        'x-rapidapi-host': "football-news-aggregator-live.p.rapidapi.com",
        'x-rapidapi-key': api_key
    }
    
    print(f"  [News API] Fetching news for '{query}'...")
    try:
        response = get(url, headers=headers, timeout=15).json()
        if not response.get("success"):
            print(f"  ! Lỗi API: {response.get('message', 'Unknown Error')}")
            return []
            
        data = response.get("data", [])
        if TEST_MODE and data:
            data = data[:5]
            
        put_json_gz(
            f"bronze/football_news/search_epl/ingest_date={D}/news.json.gz",
            data, SRC, meta={"count": len(data)}
        )
        return data
    except Exception as e:
        print(f"  ! Lỗi khi crawl Football News: {e}")
        return []

def build_silver(data):
    if not data: return
    
    news_list = []
    for item in data:
        news_list.append({
            "title": item.get("title"),
            "url": item.get("url"),
            "image": item.get("image"),
            "source": item.get("source"),
            "published_at": item.get("publishedAt"),
        })
                    
    if news_list:
        df = pd.DataFrame(news_list)
        df["ingest_date"] = D
        put_parquet(
            f"silver/news/football_news_agg/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} news articles -> silver")

def run_pipeline():
    print("[1/2] Football News Aggregator (Search)")
    data = ingest_news()
    
    print("\n[2/2] Silver build")
    build_silver(data)
    
    summary("bronze/football_news/")
    summary("silver/news/football_news_agg/")

if __name__ == "__main__":
    run_pipeline()
