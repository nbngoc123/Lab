#!/usr/bin/env python3
"""Chạy trong container Azure/airflow: đẩy ./fd_csv/{mùa}/{div}.csv vào bronze để p03 bỏ qua bước tải.
    python pipelines/p03/upload_fd_csv.py [thư_mục, mặc định fd_csv]
Sau đó chạy p03 như bình thường (nó thấy file đã tồn tại nên không gọi HTTP nữa).
"""
import sys
from pathlib import Path

# Thêm path để import lake
sys.path.append("/opt/project")

from lake.minio_io import put_bytes, exists

SRC = "football-data.co.uk"
root = Path(sys.argv[1] if len(sys.argv) > 1 else "fd_csv")
files = sorted(root.glob("*/*.csv"))
if not files:
    sys.exit(f"Không thấy CSV trong {root}/<mùa>/<div>.csv")
for f in files:
    season, div = f.parent.name, f.stem
    key = f"bronze/football_data_couk/{div}/season={season}/{div}.csv"
    if exists(key):
        print("· đã có", key); continue
    put_bytes(key, f.read_bytes(), SRC, content_type="text/csv",
              meta={"division": div, "season": season, "source_url": "manual-upload"})
    print("✓", key)
