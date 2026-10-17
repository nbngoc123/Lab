import os
import logging
from pathlib import Path
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

SOURCES = [
    'mart_ml_text_google_news',
    'mart_ml_text_football_news',
    'mart_ml_text_youtube_videos',
    'mart_ml_text_youtube_comments'
]


def _conn():
    con = duckdb.connect(':memory:')
    con.execute('INSTALL httpfs; LOAD httpfs;')
    ak = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    sk = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    raw_ep = os.getenv('MINIO_ENDPOINT', 'minio:9000')
    endpoint = raw_ep.replace('http://', '').replace('https://', '')
    use_ssl = 'true' if ('https://' in raw_ep or os.getenv('MINIO_USE_SSL', 'false').lower() == 'true') else 'false'
    
    q = lambda x: str(x).replace("'", "''")
    con.execute(f"""
        CREATE SECRET minio_secret (
            TYPE S3,
            KEY_ID '{q(ak)}',
            SECRET '{q(sk)}',
            ENDPOINT '{q(endpoint)}',
            URL_STYLE 'path',
            USE_SSL {use_ssl}
        )
    """)
    return con


def read_sources(cfg):
    data = {}
    if cfg.input_mode not in {'local', 'minio'}:
        raise ValueError('INPUT_MODE must be local/minio')
    
    con = _conn() if cfg.input_mode == 'minio' else None
    try:
        for name in SOURCES:
            path = cfg.local_dir / f'{name}.parquet' if con is None else f"{cfg.s3_root.rstrip('/')}/{name}.parquet"
            if con is None:
                data[name] = pd.read_parquet(path)
            else:
                data[name] = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).df()
            logger.info("✓ Nạp %s: %d dòng", name, len(data[name]))
    finally:
        if con:
            con.close()
    return data


def write_parquets(tables, cfg, batch_id):
    local = cfg.output_dir / batch_id
    local.mkdir(parents=True, exist_ok=True)
    
    # 1. Ghi tệp Parquet cục bộ
    for name, frame in tables.items():
        frame.to_parquet(local / f'{name}.parquet', index=False)
        
    # 2. Đẩy lên MinIO nếu output_mode là minio
    if cfg.output_mode == 'minio':
        import boto3
        from botocore.client import Config
        from urllib.parse import urlparse
        
        raw_ep = os.getenv('MINIO_ENDPOINT', 'minio:9000')
        endpoint = ('https://' if ('https://' in raw_ep or os.getenv('MINIO_USE_SSL', 'false').lower() == 'true') else 'http://') + raw_ep.replace('http://', '').replace('https://', '')
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'),
            config=Config(s3={'addressing_style': 'path'})
        )
        target = urlparse(cfg.output_s3_root)
        if target.scheme != 's3' or not target.netloc:
            raise ValueError('PREDICTION_S3_ROOT must use s3://bucket/prefix')
            
        bucket = target.netloc
        prefix = target.path.strip('/')
        for name in tables:
            key = f"{prefix}/{batch_id}/{name}.parquet" if prefix else f"{batch_id}/{name}.parquet"
            client.upload_file(str(local / f'{name}.parquet'), bucket, key)
            # Đồng thời cập nhật bản snapshot latest lên s3://football-lake/dwh/ cho truy vấn nhanh
            latest_key = f"dwh/{name}.parquet"
            try:
                client.upload_file(str(local / f'{name}.parquet'), bucket, latest_key)
            except Exception as le:
                logger.warning("Không thể lưu latest copy %s: %s", latest_key, le)
                
        logger.info("✓ Đã tải %d bảng Parquet lên MinIO (bucket: %s, batch: %s)", len(tables), bucket, batch_id)

    return local
