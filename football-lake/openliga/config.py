"""Cấu hình qua biến môi trường (có default chạy được khi chạy từ máy host)."""
import os
from datetime import datetime, timezone


def _int(name, default):
    return int(os.getenv(name, default))


BASE_URL = os.getenv("OPENLIGA_BASE_URL", "https://api.openligadb.de").rstrip("/")
USER_AGENT = os.getenv("OPENLIGA_USER_AGENT", "football-lake-openliga-sync/1.0")

# API giới hạn 60 req/phút/IP -> mặc định dùng 50 để chừa biên an toàn
MAX_REQ_PER_MIN = _int("OPENLIGA_MAX_REQ_PER_MIN", 50)

# Phạm vi mùa giải: 2024 -> hiện tại (season = năm bắt đầu mùa, vd 2025 = 2025/26)
START_SEASON = _int("OPENLIGA_START_SEASON", 2024)
END_SEASON = _int("OPENLIGA_END_SEASON", datetime.now(timezone.utc).year)
SEASONS = list(range(START_SEASON, END_SEASON + 1))
# Chỉ các mùa gần nhất mới còn thay đổi -> chỉ reconcile/poll những mùa này
LIVE_SEASONS = SEASONS[-max(1, _int("OPENLIGA_LIVE_SEASONS", 2)):]

# Postgres "Football Source DB"
PG_HOST = os.getenv("FOOTBALL_PG_HOST", "localhost")
PG_PORT = _int("FOOTBALL_PG_PORT", 5433)          # postgres-cdc map ra 5433
PG_USER = os.getenv("FOOTBALL_PG_USER", "postgres")
PG_PASSWORD = os.getenv("FOOTBALL_PG_PASSWORD", "postgres")
PG_DB = os.getenv("FOOTBALL_PG_DB", "football_source")

RAW_LOG = os.getenv("OPENLIGA_RAW_LOG", "0") == "1"

# Sync worker
TICK_SECONDS = _int("OPENLIGA_TICK_SECONDS", 30)
LIVE_INTERVAL = _int("OPENLIGA_LIVE_INTERVAL", 60)            # nhóm đang có trận: 1 phút
HOT_INTERVAL = _int("OPENLIGA_HOT_INTERVAL", 300)             # nhóm gần đây/sắp tới: 5 phút
RECONCILE_INTERVAL = _int("OPENLIGA_RECONCILE_INTERVAL", 86400)  # full reconcile: mỗi ngày
MAX_TASKS_PER_TICK = _int("OPENLIGA_MAX_TASKS_PER_TICK", 40)
