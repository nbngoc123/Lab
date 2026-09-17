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


def _to_ascii(v: str) -> str:
    """Encode về ASCII, thay thế ký tự không hỗ trợ bằng '?' - S3 metadata chỉ nhận ASCII."""
    return str(v).encode("ascii", errors="replace").decode("ascii")


def _meta(source: str, extra: dict | None = None) -> dict:
    m = {
        "source": _to_ascii(source),
        "ingested-at": datetime.now(timezone.utc).isoformat(),
    }
    if extra:
        m.update({_to_ascii(k): _to_ascii(v) for k, v in extra.items()})
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
