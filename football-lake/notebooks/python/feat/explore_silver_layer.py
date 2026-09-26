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
    
    print("=== CHI TIẾT TỪNG FILE TRONG SILVER LAYER ===\n")
    import io
    import pandas as pd
    from lake.minio_io import read_bytes
    
    for k, size in keys:
        if not k.endswith(".parquet"):
            continue
            
        print(f"[{k}]")
        print(f"  - Size: {size / 1024:.2f} KB")
        try:
            # Tải file parquet và đọc metadata
            data = read_bytes(k)
            df = pd.read_parquet(io.BytesIO(data))
            
            print(f"  - Shape: {df.shape[0]} rows x {df.shape[1]} columns")
            print(f"  - Columns: {', '.join(df.columns.tolist())}")
            if not df.empty:
                print("  - Sample (row 1):")
                sample = df.head(1).to_dict('records')[0]
                # Cắt ngắn chuỗi nếu quá dài
                for key, val in sample.items():
                    val_str = str(val)
                    if len(val_str) > 60:
                        val_str = val_str[:57] + "..."
                    print(f"      {key}: {val_str}")
        except Exception as e:
            print(f"  - Error reading file: {e}")
        print("-" * 60)

if __name__ == "__main__":
    explore_silver()
