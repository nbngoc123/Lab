"""Ingest Football-Data.co.uk: Chỉ lấy CSV raw -> MinIO (Bronze Layer)."""
import os
from datetime import date
from lake.minio_io import put_bytes, exists
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"
SRC = "football-data.co.uk"
BASE = "https://www.football-data.co.uk/mmz4281"

DEFAULT_DIVISIONS = ["E0"] if TEST_MODE else ["E0", "E1", "SP1", "D1", "I1", "F1"]
FIRST_SEASON_YEAR = 2020  # chỉ cần lịch sử từ 2020


def _current_season_start(today: date | None = None) -> int:
    """Năm bắt đầu mùa hiện tại (mùa mới từ 1/7)."""
    d = today or date.today()
    return d.year if d.month >= 7 else d.year - 1


def _season_code(year: int) -> str:
    """2020 -> '2021', 2026 -> '2627'."""
    return f"{year % 100:02d}{(year + 1) % 100:02d}"


def _is_live(season: str) -> bool:
    """Mùa đang diễn ra: file cập nhật hằng tuần -> luôn phải tải lại."""
    return season == _season_code(_current_season_start())


def _season_codes() -> list[str]:
    return [_season_code(y) for y in range(FIRST_SEASON_YEAR, _current_season_start() + 1)]


def get_partitions() -> list[dict]:
    """Mỗi partition = 1 cặp (division, season) → Airflow map từng task riêng."""
    if TEST_MODE:
        return [{"division": "E0", "season": _season_code(_current_season_start())}]
    return [{"division": div, "season": s}
            for div in DEFAULT_DIVISIONS
            for s in _season_codes()]


def ingest(partition: dict) -> str:
    """Tải 1 CSV cho 1 giải & 1 mùa. Mùa đã kết thúc: tải 1 lần. Mùa hiện tại: tải lại mỗi lần."""
    div = partition["division"]
    season = partition["season"]
    key = f"raw/football_data_couk/{div}/season={season}/{div}.csv"
    live = _is_live(season)

    if exists(key) and not live:
        print(f"  · {div}/{season} đã có (mùa cũ), bỏ qua.")
        return key

    url = f"{BASE}/{season}/{div}.csv"
    try:
        resp = get(url)
        content = resp.content
    except Exception as e:
        # 404 đầu mùa mới: file chưa được đăng, không crash
        if "404" in str(e) and live:
            print(f"  · {div}/{season}: file mùa mới chưa có ({url}), bỏ qua.")
            return ""
        raise RuntimeError(f"Lỗi tải {url}: {e}")

    # Nếu server trả về HTML thay vì CSV thì không ghi đè
    if not content or content.lstrip()[:1] == b"<":
        print(f"  ! {url} không trả về CSV hợp lệ, bỏ qua.")
        return ""

    put_bytes(key, content, SRC, content_type="text/csv",
              meta={"division": div, "season": season, "source_url": url})
    suffix = " (mùa hiện tại – làm mới)" if live else ""
    print(f"  ✓ {div}/{season} -> {key}{suffix}")
    return key
