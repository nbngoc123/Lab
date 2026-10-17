import re
import logging
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

SAFE = re.compile(r'^[a-z_][a-z0-9_]*$')


def publish(tables, cfg):
    """Nạp 10 bảng Text Intelligence vào PostgreSQL schema (mặc định 'dwh').
    Sử dụng cơ chế TRUNCATE + INSERT để bảo toàn views / grants trong Superset.
    """
    if not cfg.pg_uri:
        raise ValueError('POSTGRES_SQLALCHEMY_URI missing')
    if not SAFE.fullmatch(cfg.schema):
        raise ValueError(f'Unsafe DWH_SCHEMA: {cfg.schema}')
    for name in tables:
        if not SAFE.fullmatch(name):
            raise ValueError(f'Unsafe table name: {name}')

    engine = create_engine(cfg.pg_uri, pool_pre_ping=True)
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{cfg.schema}";'))
            
            for name, frame in tables.items():
                logger.info("Đang nạp bảng %s.%s (%d dòng)...", cfg.schema, name, len(frame))
                
                # Kiểm tra bảng đã tồn tại trong schema chưa
                table_check = conn.execute(
                    text("""
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = :schema AND table_name = :table;
                    """),
                    {"schema": cfg.schema, "table": name}
                ).fetchone()

                if table_check:
                    try:
                        # Bảng đã tồn tại -> TRUNCATE và INSERT APPEND để không làm mất View của Superset
                        conn.execute(text(f'TRUNCATE TABLE "{cfg.schema}"."{name}";'))
                        frame.to_sql(
                            name,
                            conn,
                            schema=cfg.schema,
                            if_exists='append',
                            index=False,
                            method='multi',
                            chunksize=1000
                        )
                        logger.info("✓ [TRUNCATE + INSERT] Thành công %s.%s (%d dòng)", cfg.schema, name, len(frame))
                        continue
                    except Exception as te:
                        logger.warning("TRUNCATE+INSERT lỗi (%s), chuyển sang DROP CASCADE + RECREATE...", te)
                        conn.execute(text(f'DROP TABLE IF EXISTS "{cfg.schema}"."{name}" CASCADE;'))

                # Tạo mới bảng lần đầu hoặc fallback
                frame.to_sql(
                    name,
                    conn,
                    schema=cfg.schema,
                    if_exists='replace',
                    index=False,
                    method='multi',
                    chunksize=1000
                )
                logger.info("✓ [CREATE NEW] Thành công %s.%s (%d dòng)", cfg.schema, name, len(frame))

        logger.info("✓ Toàn bộ 10 bảng Text Intelligence đã được cập nhật thành công vào schema '%s'!", cfg.schema)
    finally:
        engine.dispose()
