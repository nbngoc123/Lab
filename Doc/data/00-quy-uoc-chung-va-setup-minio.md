# 00 — Quy ước chung & Setup MinIO

File này là nền tảng cho 8 pipeline còn lại. Đọc file này trước, các file `01`–`08` đều import chung một module helper mô tả ở đây.

---

## 1. Dựng MinIO bằng Docker Compose

```yaml
# docker-compose.yml
services:
  minio:
    image: quay.io/minio/minio:latest
    container_name: minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # Web Console
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin123
    volumes:
      - ./minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5
```

```bash
docker compose up -d
# Console: http://localhost:9001  (minioadmin / minioadmin123)
```

Tạo bucket + phân quyền bằng `mc`:

```bash
docker run --rm -it --network host --entrypoint sh quay.io/minio/mc -c '
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb -p local/football-lake
mc version enable local/football-lake
mc ls local
'
```

Bật versioning giúp bạn re-run pipeline mà không mất bản ingest cũ — rất hữu ích khi demo idempotency.

---

## 2. Quy ước đặt path trong lake

Một bucket duy nhất: **`football-lake`**. Ba zone:

```
football-lake/
├── bronze/                  # dữ liệu thô, giữ nguyên format gốc
│   └── <source>/<dataset>/ingest_date=YYYY-MM-DD/<file>
├── silver/                  # đã làm sạch, chuẩn schema, luôn là Parquet
│   └── <domain>/<table>/<partition>/part-*.parquet
└── gold/                    # bảng phục vụ BI/ML
    └── <mart>/<table>/*.parquet
```

Ví dụ thực tế:

```
bronze/fpl/bootstrap_static/ingest_date=2026-09-16/bootstrap.json.gz
bronze/football_data_couk/E0/season=2425/E0.csv
bronze/statsbomb/events/competition_id=43/season_id=106/events_7580.json.gz
silver/players/fpl_player_gw/season=2025-26/gw=05/part-0.parquet
gold/analytics/team_form/part-0.parquet
```

**Nguyên tắc bất di bất dịch:**

| Nguyên tắc | Lý do |
|---|---|
| Bronze **không được sửa nội dung** file gốc | Có thể replay lại toàn bộ pipeline từ bronze |
| Partition bằng `ingest_date=` cho nguồn động, bằng khóa nghiệp vụ (`season=`, `competition_id=`) cho nguồn tĩnh | Query engine (DuckDB/Spark/Trino) tự partition-prune |
| Nén gzip cho JSON/CSV ở bronze | Tiết kiệm 70–90% dung lượng |
| Mọi object có metadata `x-amz-meta-source`, `x-amz-meta-ingested-at` | Truy vết nguồn gốc |

---

## 3. Cài đặt môi trường

```bash
python -m venv .venv && source .venv/bin/activate

pip install \
  boto3 requests pandas pyarrow duckdb \
  python-dotenv tenacity tqdm \
  praw feedparser SPARQLWrapper lxml
```

`.env` ở thư mục gốc dự án:

```ini
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
MINIO_BUCKET=football-lake

# điền khi tới file tương ứng
API_FOOTBALL_KEY=
REDDIT_CLIENT_ID=
REDDIT_CLIENT_SECRET=
REDDIT_USER_AGENT=football-lake/0.1 by u/yourname
THESPORTSDB_KEY=3
```

---

## 4. Module helper dùng chung — `lake/minio_io.py`

Tạo file này một lần, cả 8 pipeline đều dùng.

```python
# lake/minio_io.py
import gzip
import hashlib
import io
import json
import os
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("MINIO_BUCKET", "football-lake")


def client():
    """S3 client trỏ vào MinIO. path-style bắt buộc với MinIO local."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


S3 = client()


def today() -> str:
    return date.today().isoformat()


def _meta(source: str, extra: dict | None = None) -> dict:
    m = {
        "source": source,
        "ingested-at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        m.update({k: str(v) for k, v in extra.items()})
    return m


def put_bytes(key: str, data: bytes, source: str,
              content_type="application/octet-stream", meta=None) -> str:
    """Ghi bytes lên MinIO, trả về key. In ra checksum để đối chiếu."""
    S3.put_object(
        Bucket=BUCKET, Key=key, Body=data,
        ContentType=content_type, Metadata=_meta(source, meta),
    )
    sha = hashlib.sha256(data).hexdigest()[:12]
    print(f"  ✓ s3://{BUCKET}/{key}  ({len(data):,} B, sha256:{sha})")
    return key


def put_json_gz(key: str, obj, source: str, meta=None) -> str:
    """Serialize dict/list -> JSON -> gzip -> MinIO. Key nên kết thúc .json.gz"""
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source,
                     content_type="application/gzip", meta=meta)


def put_file(key: str, path: str | Path, source: str,
             content_type="application/octet-stream", meta=None) -> str:
    """Upload file từ đĩa. Dùng multipart tự động cho file lớn."""
    S3.upload_file(
        str(path), BUCKET, key,
        ExtraArgs={"ContentType": content_type, "Metadata": _meta(source, meta)},
    )
    size = Path(path).stat().st_size
    print(f"  ✓ s3://{BUCKET}/{key}  ({size:,} B, from file)")
    return key


def put_parquet(key: str, df, source: str, meta=None) -> str:
    """Ghi pandas DataFrame thành Parquet (snappy) lên MinIO."""
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return put_bytes(key, buf.getvalue(), source,
                     content_type="application/vnd.apache.parquet",
                     meta={**(meta or {}), "rows": len(df)})


def exists(key: str) -> bool:
    try:
        S3.head_object(Bucket=BUCKET, Key=key)
        return True
    except S3.exceptions.ClientError:
        return False


def list_keys(prefix: str):
    paginator = S3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            yield o["Key"], o["Size"]


def read_bytes(key: str) -> bytes:
    return S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def read_json_gz(key: str):
    return json.loads(gzip.decompress(read_bytes(key)).decode("utf-8"))


def summary(prefix: str) -> None:
    """In thống kê nhanh: số object + tổng dung lượng dưới một prefix."""
    n, total = 0, 0
    for _, size in list_keys(prefix):
        n += 1
        total += size
    print(f"\n[summary] {prefix}: {n} objects, {total/1024/1024:.2f} MB")
```

Thêm `lake/__init__.py` rỗng để import được:

```bash
mkdir -p lake && touch lake/__init__.py
```

---

## 5. Mẫu retry chung — dùng lại ở mọi file gọi HTTP

```python
# lake/http.py
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "football-lake/0.1 (educational project)"})


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException,)),
    reraise=True,
)
def get(url, **kw):
    r = SESSION.get(url, timeout=kw.pop("timeout", 30), **kw)
    r.raise_for_status()
    return r
```

---

## 6. Bảng tổng: 8 pipeline và thứ tự nên chạy

| # | File | Nguồn | Kiểu ingest | Output ở bronze |
|---|---|---|---|---|
| 01 | `01-fpl-api.md` | Fantasy PL API | REST không cần auth | JSON.gz |
| 02 | `02-api-football.md` | API-Football | REST có API key, rate-limited | JSON.gz theo fixture |
| 03 | `03-football-data-couk.md` | Football-Data.co.uk | CSV batch nhiều mùa | CSV gốc |
| 04 | `04-statsbomb-open-data.md` | StatsBomb Open Data | Nested JSON khối lớn | JSON.gz |
| 05 | `05-european-soccer-sqlite.md` | European Soccer DB | Database (SQLite) | .sqlite + Parquet mỗi bảng |
| 06 | `06-wikidata-sparql.md` | Wikidata | SPARQL / semantic | JSON + Parquet |
| 07 | `07-reddit-rss-text.md` | Reddit + RSS | Text / unstructured | JSONL.gz |
| 08 | `08-thesportsdb-binary.md` | TheSportsDB | Binary (ảnh) + metadata | PNG/JPG + JSON sidecar |

Chạy `01 → 03 → 08` trước (nhanh, không cần key), rồi tới `04 → 05 → 06`, cuối cùng `02 → 07` (cần đăng ký key).

---

## 7. Kiểm tra toàn lake sau khi chạy xong

```python
# scripts/audit_lake.py
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
```

Kết quả mong đợi sau khi chạy đủ 8 pipeline (số liệu tham khảo):

```
prefix                                         objects    size (MB)
--------------------------------------------------------------------
bronze/api_football                                 45         1.80
bronze/football_data_couk                           66        14.20
bronze/fpl                                          23         3.40
bronze/reddit                                        9         2.10
bronze/rss                                          12         0.90
bronze/statsbomb                                  3550       410.00
bronze/thesportsdb                                 148        22.30
bronze/wikidata                                      4         1.10
silver/...                                          31        58.60
```

Con số cụ thể sẽ khác tùy tham số bạn chọn, nhưng tỷ lệ tương đối (StatsBomb chiếm phần lớn dung lượng, Wikidata ít object nhất) nên trông giống vậy.

---

## 8. Truy vấn lake bằng DuckDB (không cần Spark)

Cách nhanh nhất để chứng minh lake "chạy được":

```python
import duckdb, os
from dotenv import load_dotenv
load_dotenv()

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
  SET s3_endpoint='{os.getenv("MINIO_ENDPOINT").replace("http://","")}';
  SET s3_access_key_id='{os.getenv("MINIO_ACCESS_KEY")}';
  SET s3_secret_access_key='{os.getenv("MINIO_SECRET_KEY")}';
  SET s3_use_ssl=false;
  SET s3_url_style='path';
""")

con.sql("""
  SELECT * FROM read_parquet('s3://football-lake/silver/players/fpl_player_gw/**/*.parquet')
  LIMIT 10
""").show()
```

Giữ snippet này bên cạnh — mỗi file từ 01–08 đều kết bằng một truy vấn kiểm chứng dùng đúng connection này.
