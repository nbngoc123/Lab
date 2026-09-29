"""Ingest football-data.org API v4: Chỉ lấy Bronze (Dynamic Mapping)."""
import os
import time
from lake.minio_io import put_json_gz, exists, today
from lake.http import SESSION

BASE = "https://api.football-data.org/v4"
SRC = "football-data.org"
D = today()
TOKEN = os.getenv("FOOTBALL_DATA_TOKEN", "")
HEADERS = {"X-Auth-Token": TOKEN}

TEST_MODE = os.getenv("TEST_MODE") == "1"
COMPETITIONS = ["PL"] if TEST_MODE else ["PL", "PD", "BL1", "SA", "FL1", "CL"]

def get_partitions() -> list[dict]:
    return [{"competition": c} for c in COMPETITIONS]

def call(path: str) -> dict:
    r = SESSION.get(f"{BASE}{path}", headers=HEADERS, timeout=30)
    time.sleep(6.5)
    return r.json()

def ingest(partition: dict) -> str:
    comp = partition["competition"]
    key = f"bronze/football_data_org/matches/comp={comp}/ingest_date={D}/matches.json.gz"
    
    if exists(key):
        return key

    data = call(f"/competitions/{comp}/matches")
    put_json_gz(key, data, SRC, meta={"competition": comp})
    return key
