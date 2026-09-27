import io
import pandas as pd
from lake import minio_io as mio
import sys

def inspect_details():
    try:
        files = list(mio.list_keys("silver/"))
    except Exception as e:
        print(f"Không thể kết nối MinIO: {e}")
        sys.exit(1)

    datasets = {}
    for key, size in files:
        if not key.endswith(".parquet"): continue
        parts = key.split("/")
        if len(parts) >= 3:
            ds_name = f"{parts[1]}/{parts[2]}"
            datasets.setdefault(ds_name, []).append((key, size))
            
    print("# BÁO CÁO CHI TIẾT CÁC BẢNG DỮ LIỆU LỚP SILVER\n")
    for ds_name, file_list in sorted(datasets.items()):
        print(f"## 1. Dataset: `{ds_name}`")
        print(f"- **Tổng số file Parquet**: {len(file_list)}")
        
        # Chọn file đầu tiên làm mẫu
        sample_key = file_list[0][0]
        try:
            raw = mio.read_bytes(sample_key)
            df = pd.read_parquet(io.BytesIO(raw))
            
            print(f"- **Số cột**: {len(df.columns)}")
            print(f"- **Tổng số ô Null/Missing trong mẫu**: {df.isna().sum().sum()}")
            print("- **Lược đồ Cột (Schema)**:")
            schema_str = ", ".join([f"`{col}` ({dtype})" for col, dtype in zip(df.columns, df.dtypes)])
            print(f"  {schema_str}")
                
            print("\n- **Dữ liệu mẫu (5 dòng đầu tiên)**:")
            # In ra dưới dạng bảng markdown
            print(df.head(5).to_markdown(index=False))
            print("\n" + "-"*100 + "\n")
        except Exception as e:
            print(f"Lỗi khi đọc {sample_key}: {e}\n")

if __name__ == "__main__":
    inspect_details()
