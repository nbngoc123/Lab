import os
import sys
from pprint import pprint
import pandas as pd

# Thêm đường dẫn project vào sys.path để import lake module
sys.path.append("/opt/project")

from lake.minio_io import list_keys

def explore_silver():
    print("=== TỔNG QUAN SILVER LAYER ===")
    
    # Lấy danh sách tất cả các object trong thư mục silver/
    keys = list(list_keys("silver/"))
    
    if not keys:
        print("Silver layer hiện đang trống!")
        return
        
    total_size = sum(size for _, size in keys)
    print(f"Tổng số file: {len(keys)}")
    print(f"Tổng dung lượng: {total_size / 1e6:.2f} MB\n")
    
    # Gom nhóm theo loại thư mục (dim, matches, players, standings...)
    categories = {}
    for k, size in keys:
        parts = k.split("/")
        if len(parts) >= 3:
            # silver/dim/wd_clubs/... -> category = "dim/wd_clubs"
            category = f"{parts[1]}/{parts[2]}"
            if category not in categories:
                categories[category] = {"count": 0, "size": 0}
            categories[category]["count"] += 1
            categories[category]["size"] += size
            
    print("=== CHI TIẾT CÁC BẢNG TRONG SILVER ===")
    for cat, stats in sorted(categories.items()):
        print(f"[{cat.upper()}]")
        print(f"  - Số file: {stats['count']}")
        print(f"  - Dung lượng: {stats['size'] / 1e3:.1f} KB")

if __name__ == "__main__":
    explore_silver()
