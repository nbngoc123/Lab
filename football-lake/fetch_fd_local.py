#!/usr/bin/env python3
"""Chạy trên MÁY CỦA BẠN (không phải Azure): tải CSV football-data.co.uk về thư mục ./fd_csv/
    pip install requests
    python fetch_fd_local.py            # 10 mùa E0+E1
    python fetch_fd_local.py --test     # chỉ E0 2425
Kết quả: fd_csv/2425/E0.csv ...  -> copy thư mục fd_csv lên Azure rồi chạy upload_fd_csv.py
"""
import argparse, time
from pathlib import Path
import requests

BASE = "https://www.football-data.co.uk/mmz4281"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Referer": "https://www.football-data.co.uk/englandm.php"}
ALL_SEASONS = ["1516", "1617", "1718", "1819", "1920", "2021", "2122", "2223", "2324", "2425"]

ap = argparse.ArgumentParser(); ap.add_argument("--test", action="store_true")
ap.add_argument("--out", default="fd_csv"); a = ap.parse_args()
divs = ["E0"] if a.test else ["E0", "E1"]
seasons = ["2425"] if a.test else ALL_SEASONS
for s in seasons:
    for d in divs:
        p = Path(a.out) / s / f"{d}.csv"
        if p.exists():
            print("· có sẵn", p); continue
        r = requests.get(f"{BASE}/{s}/{d}.csv", headers=UA, timeout=60)
        if r.status_code != 200:
            print(f"! {s}/{d}: HTTP {r.status_code}"); continue
        p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(r.content)
        print(f"✓ {p} ({len(r.content)/1024:.0f} KB)"); time.sleep(1)
