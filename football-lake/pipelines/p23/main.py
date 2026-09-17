"""
p23: FBref (Thống kê Cầu thủ & Đội bóng)
Sử dụng pandas.read_html để lấy dữ liệu từ FBref.
Có rate limit nghiêm ngặt, cần delay (sleep 4-5s) giữa các request.
"""
import gzip
import io
import time
import pandas as pd
from lake.minio_io import put_bytes, put_parquet, today, summary
import urllib.request

SRC = "fbref"
D = today()
TEST_MODE = False

# URL mẫu cho EPL Stats (Bảng tổng hợp)
FBREF_EPL_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"

def ingest_fbref():
    print(f"  [FBref] Fetching EPL stats...")
    try:
        # Cần headers để tránh 403 Forbidden
        req = urllib.request.Request(
            FBREF_EPL_URL, 
            data=None, 
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
        )
        html = urllib.request.urlopen(req).read()
        
        # Parse HTML tables
        tables = pd.read_html(io.BytesIO(html))
        
        # FBref EPL page usually has:
        # tables[0]: Regular season standings
        # tables[2]: Squad Standard Stats
        
        standings = tables[0]
        squad_stats = tables[2]
        
        # Save raw CSV to bronze
        standings_csv = standings.to_csv(index=False).encode('utf-8')
        squad_stats_csv = squad_stats.to_csv(index=False).encode('utf-8')
        
        put_bytes(
            f"bronze/fbref/epl_standings/ingest_date={D}/standings.csv",
            standings_csv, SRC, content_type="text/csv", meta={"rows": len(standings)}
        )
        put_bytes(
            f"bronze/fbref/epl_squad_stats/ingest_date={D}/squad_stats.csv",
            squad_stats_csv, SRC, content_type="text/csv", meta={"rows": len(squad_stats)}
        )
        
        time.sleep(4) # Rate limit rule of FBref
        return standings, squad_stats
        
    except Exception as e:
        print(f"  ! Lỗi khi crawl FBref: {e}")
        return None, None

def build_silver(standings, squad_stats):
    if standings is not None and not standings.empty:
        # Clean multi-index columns if present
        if isinstance(standings.columns, pd.MultiIndex):
            standings.columns = ['_'.join(col).strip() for col in standings.columns.values]
            
        standings["ingest_date"] = D
        # Ensure string columns
        standings.columns = [str(c).replace(" ", "_").replace("/", "_").lower() for c in standings.columns]
        
        put_parquet(
            f"silver/teams/fbref_standings/ingest_date={D}/part-0.parquet",
            standings, SRC, meta={"rows": len(standings)}
        )
        print(f"  ✓ {len(standings)} standings -> silver")

    if squad_stats is not None and not squad_stats.empty:
        if isinstance(squad_stats.columns, pd.MultiIndex):
            squad_stats.columns = ['_'.join(col).strip() for col in squad_stats.columns.values]
            
        squad_stats["ingest_date"] = D
        squad_stats.columns = [str(c).replace(" ", "_").replace("/", "_").lower() for c in squad_stats.columns]
        
        put_parquet(
            f"silver/teams/fbref_squad_stats/ingest_date={D}/part-0.parquet",
            squad_stats, SRC, meta={"rows": len(squad_stats)}
        )
        print(f"  ✓ {len(squad_stats)} squad_stats -> silver")

def run_pipeline():
    print("[1/2] FBref Web Scrape")
    standings, squad_stats = ingest_fbref()
    
    print("\n[2/2] Silver build")
    build_silver(standings, squad_stats)
    
    summary("bronze/fbref/")
    summary("silver/teams/fbref_standings/")
    summary("silver/teams/fbref_squad_stats/")

if __name__ == "__main__":
    run_pipeline()
