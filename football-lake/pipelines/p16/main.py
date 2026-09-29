"""Scrape PhysioRoom Premier League Injury Table -> MinIO (Chỉ lấy Bronze)."""
import pandas as pd
from io import StringIO
from lake.minio_io import put_bytes, put_json_gz, exists, today
from lake.http import SESSION

SRC = "physioroom"
D = today()
URL = "https://www.physioroom.com/advice/premier-league-injury-table/"

def get_partitions() -> list[dict]:
    return [{"url": URL}]

def fetch_page() -> str:
    r = SESSION.get(URL, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    r.raise_for_status()
    html = r.text
    put_bytes(
        f"raw/physioroom/injury_table/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")
    return html

def parse_tables(html: str) -> list:
    """
    Trang có đúng 4 bảng HTML liên tiếp theo thứ tự cố định:
    [0] tổng injuries/CLB, [1] danh sách cầu thủ, [2] loại chấn thương/CLB,
    [3] tần suất loại chấn thương toàn giải.
    pandas.read_html tự parse mọi <table> trong trang theo đúng thứ tự xuất hiện.
    """
    tables = pd.read_html(StringIO(html))
    if len(tables) < 4:
        raise RuntimeError(
            f"Chỉ tìm thấy {len(tables)} bảng, kỳ vọng 4 — "
            f"trang có thể đã đổi cấu trúc, kiểm tra page.html đã lưu ở raw")

    raw_data = [t.to_dict(orient="records") for t in tables[:4]]
    put_json_gz(
        f"raw/physioroom/injury_table/ingest_date={D}/tables_raw.json.gz",
        raw_data, SRC, meta={"n_tables": len(tables)})
    return raw_data

def ingest(partition: dict) -> str:
    key = f"raw/physioroom/injury_table/ingest_date={D}/tables_raw.json.gz"
    if exists(key):
        print(f"  · Bảng chấn thương ngày {D} đã có, bỏ qua.")
        return key
        
    html = fetch_page()
    parse_tables(html)
    return key
