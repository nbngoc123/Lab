from minio import Minio
from collections import Counter

c = Minio("20.41.113.183:9000",
          access_key="minioadmin",
          secret_key="minioadmin123",
          secure=False)

print("=== BUCKETS ===")
for b in c.list_buckets():
    print(b.name)

bucket = "football-lake"
objs = list(c.list_objects(bucket, recursive=True))
total = sum(o.size or 0 for o in objs)
print(f"\n=== {bucket}: {len(objs)} objects, {total/1e6:.1f} MB ===")

cnt = Counter("/".join(o.object_name.split("/")[:2]) for o in objs)
for k, v in cnt.most_common(30):
    print(f"{v:>6}  {k}")

print("\n=== 20 object đầu ===")
for o in objs[:20]:
    print(o.object_name, o.size)