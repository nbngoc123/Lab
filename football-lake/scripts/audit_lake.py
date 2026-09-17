# scripts/audit_lake.py
import sys
from pathlib import Path

# Add project root to python path so lake can be imported
sys.path.append(str(Path(__file__).parent.parent))

from lake.minio_io import list_keys, BUCKET
from collections import defaultdict

stats = defaultdict(lambda: [0, 0])
for key, size in list_keys(""):
    parts = key.split("/")
    group = "/".join(parts[:2])       # ví dụ: bronze/fpl
    stats[group][0] += 1
    stats[group][1] += size

print(f"{'prefix':<45} {'objects':>8} {'size (MB)':>12}")
print("-" * 68)
for g in sorted(stats):
    n, s = stats[g]
    print(f"{g:<45} {n:>8} {s/1024/1024:>12.2f}")
