"""
Ingest TheSportsDB (p08): metadata JSON + ảnh nhị phân + manifest.
Chỉ lấy Bronze (Dynamic Task Mapping).
"""
import os
import time
from lake.minio_io import put_json_gz, exists, today
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"
SRC  = "thesportsdb"
D    = today()
KEY  = os.getenv("THESPORTSDB_KEY", "3")
BASE = f"https://www.thesportsdb.com/api/v1/json/{KEY}"

LEAGUES = [{"name": "English Premier League", "slug": "EPL"}] if TEST_MODE else [
    {"name": "English Premier League",       "slug": "EPL"},
    {"name": "English League Championship",  "slug": "Championship"},
    {"name": "Spanish La Liga",              "slug": "LaLiga"},
    {"name": "German Bundesliga",            "slug": "Bundesliga"},
]

def get_partitions() -> list[dict]:
    return LEAGUES

def ingest(partition: dict) -> str:
    league_name = partition["name"]
    slug = partition["slug"]
    key = f"raw/thesportsdb/teams/league={slug}/ingest_date={D}/teams.json.gz"
    
    if exists(key):
        return key

    url = f"{BASE}/search_all_teams.php?l={league_name.replace(' ', '%20')}"
    data = get(url, timeout=30).json()
    put_json_gz(key, data, SRC, meta={"league": slug})
    time.sleep(2)
    return key
