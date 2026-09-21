import sys
sys.path.append("/opt/project")
import io
import pandas as pd
from lake.minio_io import read_bytes

data = read_bytes('silver/matches/fd_matches/division=E0/season=2024-25/part-0.parquet')
df = pd.read_parquet(io.BytesIO(data))
print("Columns:", list(df.columns))
