"""
p22: The Odds API (Betting Odds)
Lấy tỉ lệ kèo H2H, Spread (Handicap), Totals (Over/Under) từ các nhà cái.
Yêu cầu: ODDS_API_KEY trong .env
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

SRC = "the-odds-api"
D = today()
TEST_MODE = False

def put_json_gz(key: str, obj, source: str, meta=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def ingest_odds():
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("  ! Thiếu ODDS_API_KEY trong .env")
        return []
        
    sport = "soccer_epl"
    regions = "uk,eu"
    markets = "h2h,spreads,totals"
    url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/?apiKey={api_key}&regions={regions}&markets={markets}"
    
    print(f"  [Odds] Fetching from {sport}...")
    try:
        data = get(url, timeout=15).json()
        if "message" in data and not isinstance(data, list):
            print(f"  ! Lỗi API: {data['message']}")
            return []
            
        if TEST_MODE and data:
            data = data[:2]
            
        put_json_gz(
            f"bronze/odds/epl/ingest_date={D}/odds.json.gz",
            data, SRC, meta={"count": len(data)}
        )
        return data
    except Exception as e:
        print(f"  ! Lỗi khi crawl The Odds API: {e}")
        return []

def build_silver(data):
    if not data: return
    
    h2h_records = []
    for match in data:
        match_id = match.get("id")
        home_team = match.get("home_team")
        away_team = match.get("away_team")
        commence_time = match.get("commence_time")
        
        for bookmaker in match.get("bookmakers", []):
            bm_title = bookmaker.get("title")
            last_update = bookmaker.get("last_update")
            
            for market in bookmaker.get("markets", []):
                if market.get("key") == "h2h":
                    outcomes = market.get("outcomes", [])
                    odds_home = odds_away = odds_draw = None
                    for oc in outcomes:
                        if oc.get("name") == home_team: odds_home = oc.get("price")
                        elif oc.get("name") == away_team: odds_away = oc.get("price")
                        elif oc.get("name") == "Draw": odds_draw = oc.get("price")
                    
                    h2h_records.append({
                        "match_id": match_id,
                        "home_team": home_team,
                        "away_team": away_team,
                        "commence_time": commence_time,
                        "bookmaker": bm_title,
                        "last_update": last_update,
                        "odds_home": odds_home,
                        "odds_draw": odds_draw,
                        "odds_away": odds_away,
                    })
                    
    if h2h_records:
        df = pd.DataFrame(h2h_records)
        df["ingest_date"] = D
        put_parquet(
            f"silver/betting/odds_h2h/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)}
        )
        print(f"  ✓ {len(df)} records H2H -> silver")

def run_pipeline():
    print("[1/2] The Odds API")
    data = ingest_odds()
    
    print("\n[2/2] Silver build")
    build_silver(data)
    
    summary("bronze/odds/")
    summary("silver/betting/odds_h2h/")

if __name__ == "__main__":
    run_pipeline()
