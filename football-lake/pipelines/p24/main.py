"""
p24: API-Football (Live Scores & Lineups)
Lấy kết quả trận đấu, đội hình ra sân, phong độ, thẻ phạt.
Yêu cầu: RAPIDAPI_KEY trong .env
"""
import gzip
import io
import json
import os
import urllib.parse
from datetime import datetime, timezone

import pandas as pd
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import get

SRC = "api-football"
D = today()
TEST_MODE = False

def put_json_gz(key: str, obj, source: str, meta=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def ingest_fixtures():
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key:
        print("  ! Thiếu RAPIDAPI_KEY trong .env")
        return []
        
    league_id = 39 # EPL
    season = 2024
    url = f"https://v3.football.api-sports.io/fixtures?league={league_id}&season={season}"
    
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': api_key
    }
    
    print(f"  [API-Football] Fetching fixtures...")
    try:
        response = get(url, headers=headers, timeout=15).json()
        if response.get("errors"):
            print(f"  ! Lỗi API: {response['errors']}")
            return []
            
        data = response.get("response", [])
        if TEST_MODE and data:
            data = data[:5]
            
        put_json_gz(
            f"bronze/api_football/epl_fixtures/ingest_date={D}/fixtures.json.gz",
            data, SRC, meta={"count": len(data)}
        )
        return data
    except Exception as e:
        print(f"  ! Lỗi khi crawl API-Football: {e}")
        return []

def build_silver(data):
    if not data: return
    
    fixtures = []
    for item in data:
        fixt = item.get("fixture", {})
        teams = item.get("teams", {})
        goals = item.get("goals", {})
        score = item.get("score", {})
        
        fixtures.append({
            "fixture_id": fixt.get("id"),
            "date": fixt.get("date"),
            "status": fixt.get("status", {}).get("long"),
            "home_team_id": teams.get("home", {}).get("id"),
            "home_team": teams.get("home", {}).get("name"),
            "away_team_id": teams.get("away", {}).get("id"),
            "away_team": teams.get("away", {}).get("name"),
            "home_goals": goals.get("home"),
            "away_goals": goals.get("away"),
        })
                    
    if fixtures:
        df = pd.DataFrame(fixtures)
        df["ingest_date"] = D
        put_parquet(
            f"silver/matches/api_fixtures/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} fixtures -> silver")

def run_pipeline():
    print("[1/2] API-Football (Fixtures)")
    data = ingest_fixtures()
    
    print("\n[2/2] Silver build")
    build_silver(data)
    
    summary("bronze/api_football/")
    summary("silver/matches/api_fixtures/")

if __name__ == "__main__":
    run_pipeline()
