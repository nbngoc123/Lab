import sys
sys.path.append("/opt/project")
from lake.minio_io import list_keys, S3, BUCKET

print("=== KEYS TRONG fd_matches ===")
for k, s in list_keys('silver/matches/fd_matches/'):
    print(k, s)

print("=== KEYS TRONG fd_odds ===")
for k, s in list_keys('silver/odds/fd_odds/'):
    print(k, s)
