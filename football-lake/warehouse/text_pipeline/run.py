import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text

from .config import Settings
from .io import read_sources, write_parquets
from .normalize import normalize_all
from .models import load_models, predict
from .analytics import aggregate
from .publish import publish
from .entities import entity_tables

logger = logging.getLogger(__name__)


def create_superset_views(engine, schema: str):
    """Tự động khởi tạo các View trực quan hóa hỗ trợ Superset Dashboard cho Text Intelligence."""
    views_sql = f"""
    -- 1. View KPI tổng quan Text Intelligence
    CREATE OR REPLACE VIEW "{schema}".v_text_kpi_overview AS
    SELECT
        (SELECT count(*) FROM "{schema}".text_predictions) AS total_analyzed_docs,
        (SELECT count(DISTINCT entity_name) FROM "{schema}".text_entity_mentions) AS total_tracked_entities,
        (SELECT round(cast(avg(sentiment_confidence) * 100 as numeric), 2) FROM "{schema}".text_predictions) AS avg_model_confidence_pct,
        (SELECT count(*) FROM "{schema}".text_negative_alerts) AS active_negative_alerts,
        (SELECT max(published_at) FROM "{schema}".text_predictions) AS latest_document_time;

    -- 2. View Xếp hạng Chủ đề Thảo luận (Topic Leaderboard)
    CREATE OR REPLACE VIEW "{schema}".v_text_topic_rankings AS
    SELECT
        topic_id,
        topic_name,
        scope,
        sum(docs) AS total_docs,
        round(cast(sum(positive)::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS positive_pct,
        round(cast(sum(neutral)::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS neutral_pct,
        round(cast(sum(negative)::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS negative_pct,
        round(cast((sum(positive) - sum(negative))::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS net_sentiment_score
    FROM "{schema}".text_topic_summary
    GROUP BY topic_id, topic_name, scope
    ORDER BY total_docs DESC;

    -- 3. View Bảng Xếp hạng Thực thể Bóng đá (Entity Sentiment Leaderboard)
    CREATE OR REPLACE VIEW "{schema}".v_text_entity_leaderboard AS
    SELECT
        entity_name,
        entity_scope,
        sum(docs) AS total_mentions,
        sum(positive) AS positive_mentions,
        sum(neutral) AS neutral_mentions,
        sum(negative) AS negative_mentions,
        round(cast(sum(positive)::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS positive_rate_pct,
        round(cast(sum(negative)::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS negative_rate_pct,
        round(cast((sum(positive) - sum(negative))::numeric / nullif(sum(docs), 0) * 100 as numeric), 2) AS sentiment_balance_score
    FROM "{schema}".text_entity_daily
    GROUP BY entity_name, entity_scope
    ORDER BY total_mentions DESC;

    -- 4. View Cảnh báo Tiêu cực Đột biến (Negative Burst Alerts Feed)
    CREATE OR REPLACE VIEW "{schema}".v_text_negative_alerts_feed AS
    SELECT
        topic_id,
        topic_name,
        scope,
        source_type,
        docs_recent,
        docs_previous,
        round(cast(share_lift as numeric), 2) AS share_lift_multiplier,
        round(cast(negative_rate_recent * 100 as numeric), 2) AS negative_rate_pct,
        round(cast(trend_score as numeric), 2) AS trend_severity_score,
        reference_date,
        'CẢNH BÁO TIÊU CỰC ĐỘT BIẾN' AS alert_badge
    FROM "{schema}".text_negative_alerts
    ORDER BY trend_score DESC;
    """
    with engine.begin() as conn:
        conn.execute(text(views_sql))
    logger.info("✓ Đã khởi tạo/cập nhật 4 views Superset cho Text Intelligence trong schema '%s'!", schema)


def execute(cfg: Settings):
    started = datetime.now(timezone.utc)
    batch_id = started.strftime('%Y%m%dT%H%M%SZ')
    
    logger.info("=== BẮT ĐẦU PIPELINE SUY LUẬN TEXT INTELLIGENCE (BATCH: %s) ===", batch_id)
    
    # 1. Đọc dữ liệu thô từ MinIO / Local
    raw = read_sources(cfg)
    
    # 2. Chuẩn hóa & làm sạch văn bản, gắn nhãn Scope & Language
    normalized = normalize_all(raw)
    if normalized.empty:
        raise ValueError('Không có tài liệu hợp lệ sau chuẩn hóa!')
    logger.info("✓ Đã chuẩn hóa %d tài liệu từ 4 nguồn.", len(normalized))
    
    # 3. Nạp 3 mô hình (Topic, Sentiment, Anomaly)
    models = load_models(cfg)
    
    # 4. Suy luận Topic & Sentiment
    predictions = predict(normalized, models, cfg)
    logger.info("✓ Hoàn thành suy luận Topic & Sentiment cho %d tài liệu.", len(predictions))
    
    # 5. Phân tích Xu hướng, Cảnh báo tiêu cực & Dị biệt (Anomaly)
    tables, reference = aggregate(predictions, cfg, models.get('anomaly'))
    
    # 6. Trích xuất Thực thể Bóng đá (Entity Recognition & Sentiment)
    alias_csv = os.getenv('ENTITY_ALIASES_CSV', '')
    mentions, entity_daily = entity_tables(predictions, alias_csv)
    tables['text_entity_mentions'] = mentions
    tables['text_entity_daily'] = entity_daily
    
    # Gắn thêm Lineage (nguồn gốc mô hình & batch_id) vào mỗi bảng
    for frame in tables.values():
        frame['topic_model_uri'] = cfg.topic_uri
        frame['sentiment_model_uri'] = cfg.sentiment_uri
        frame['anomaly_model_uri'] = cfg.anomaly_uri or 'not_configured'
        frame['batch_id'] = batch_id
        frame['reference_date'] = reference

    # 7. Lưu kết quả ra Parquet (Local + MinIO Lakehouse)
    output = write_parquets(tables, cfg, batch_id)
    
    manifest = {
        'batch_id': batch_id,
        'reference_date': str(reference.date()),
        'model_uris': {
            'topic': cfg.topic_uri,
            'sentiment': cfg.sentiment_uri,
            'anomaly': cfg.anomaly_uri
        },
        'source_rows': {k: len(v) for k, v in raw.items()},
        'normalized_rows': len(normalized),
        'table_rows': {k: len(v) for k, v in tables.items()},
        'dwh_published': False
    }
    (output / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')

    # 8. Nạp DWH PostgreSQL & Khởi tạo Views phục vụ Superset
    if cfg.publish:
        publish(tables, cfg)
        engine = create_engine(cfg.pg_uri, pool_pre_ping=True)
        try:
            create_superset_views(engine, cfg.schema)
        except Exception as ve:
            logger.warning("Không thể tạo Superset views: %s", ve)
        finally:
            engine.dispose()
            
        manifest['dwh_published'] = True
        (output / 'manifest.json').write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding='utf-8')

    # 9. Ghi nhận Telemetry lên MLflow (Không làm fail pipeline nếu mạng timeout)
    if cfg.tracking_uri:
        try:
            import mlflow
            mlflow.set_tracking_uri(cfg.tracking_uri)
            experiment = os.getenv('FOOTBALL_INFERENCE_EXPERIMENT', 'Football_Text_Intelligence_360')
            mlflow.set_experiment(experiment)
            with mlflow.start_run(run_name=f'inference_{batch_id}'):
                mlflow.set_tags({
                    'pipeline': 'football-text-production',
                    'reference_date': str(reference.date()),
                    'batch_id': batch_id
                })
                mlflow.log_params({
                    'topic_model_uri': cfg.topic_uri,
                    'sentiment_model_uri': cfg.sentiment_uri,
                    'anomaly_model_uri': cfg.anomaly_uri or 'none',
                    'dwh_schema': cfg.schema,
                    'publish_to_dwh': str(cfg.publish),
                    'input_mode': cfg.input_mode,
                    'output_mode': cfg.output_mode
                })
                mlflow.log_metrics({
                    'documents': len(predictions),
                    'processing_seconds': (datetime.now(timezone.utc) - started).total_seconds(),
                    **{f'rows_{k}': len(v) for k, v in tables.items()}
                })
                mlflow.log_dict(manifest, 'production/run_manifest.json')
            logger.info("✓ Đã ghi nhận telemetry lên MLflow Run 'inference_%s'", batch_id)
        except Exception as me:
            logger.warning("Bỏ qua ghi nhận MLflow: %s", me)

    duration = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info("=== HOÀN TẤT THÀNH CÔNG BATCH %s: %d tài liệu (thời gian: %.2fs) ===", batch_id, len(predictions), duration)
    return manifest


def main():
    parser = argparse.ArgumentParser(description='Football Text MLflow Inference -> Parquet -> PostgreSQL')
    parser.add_argument('--publish', action='store_true', help='Nạp dữ liệu vào PostgreSQL DWH')
    parser.add_argument('--dry-run', action='store_true', help='Chỉ chạy suy luận và lưu Parquet, không ghi DWH')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    cfg = Settings.from_env()
    if args.publish:
        cfg.publish = True
    if args.dry_run:
        cfg.publish = False

    res = execute(cfg)
    print(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
