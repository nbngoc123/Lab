from dataclasses import dataclass
from pathlib import Path
import os
from dotenv import load_dotenv

@dataclass
class Settings:
    tracking_uri: str
    topic_uri: str
    sentiment_uri: str
    anomaly_uri: str
    input_mode: str
    output_mode: str
    local_dir: Path
    s3_root: str
    output_dir: Path
    output_s3_root: str
    cache_dir: Path
    pg_uri: str
    schema: str
    publish: bool
    reference_date: str
    lookback_days: int
    trend_days: int
    min_trend: int
    share_lift: float
    negative_rate: float
    chunk: int

    @classmethod
    def from_env(cls):
        load_dotenv()
        
        # Tự động lấy Postgres URI từ env dự án nếu chưa khai báo riêng
        pg_host = os.getenv("PG_HOST", "postgres")
        pg_port = os.getenv("PG_PORT", "5432")
        pg_db = os.getenv("PG_DB", "football_dwh")
        pg_user = os.getenv("PG_USER", "airflow")
        pg_pass = os.getenv("PG_PASSWORD", "airflow")
        default_pg_uri = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

        return cls(
            tracking_uri=os.getenv('MLFLOW_TRACKING_URI', 'http://100.116.34.36:5000'),
            topic_uri=os.getenv('TOPIC_MODEL_URI', 'models:/football-text-topic@champion'),
            sentiment_uri=os.getenv('SENTIMENT_MODEL_URI', 'models:/football-text-sentiment@champion'),
            anomaly_uri=os.getenv('ANOMALY_MODEL_URI', 'models:/football-text-anomaly@champion'),
            input_mode=os.getenv('INPUT_MODE', 'minio'),
            output_mode=os.getenv('OUTPUT_MODE', 'minio'),
            local_dir=Path(os.getenv('FOOTBALL_TEXT_LOCAL_DIR', './data')),
            s3_root=os.getenv('FOOTBALL_TEXT_S3_ROOT', 's3://football-lake/dwh'),
            output_dir=Path(os.getenv('PREDICTION_ROOT', './outputs')),
            output_s3_root=os.getenv('PREDICTION_S3_ROOT', 's3://football-lake/ml/text_predictions'),
            cache_dir=Path(os.getenv('TEXT_MODEL_CACHE_DIR', 'warehouse/model_cache/text')),
            pg_uri=os.getenv('POSTGRES_SQLALCHEMY_URI', default_pg_uri),
            schema=os.getenv('DWH_SCHEMA', 'dwh'),
            publish=os.getenv('PUBLISH_TO_DWH', 'true').lower() == 'true',
            reference_date=os.getenv('REFERENCE_DATE', ''),
            lookback_days=int(os.getenv('LOOKBACK_DAYS', '60')),
            trend_days=int(os.getenv('TREND_WINDOW_DAYS', '7')),
            min_trend=int(os.getenv('MIN_TREND_RECENT', '5')),
            share_lift=float(os.getenv('TREND_SHARE_LIFT_MIN', '1.25')),
            negative_rate=float(os.getenv('ALERT_NEGATIVE_RATE', '0.4')),
            chunk=int(os.getenv('CHUNK_SIZE', '2048')),
        )
