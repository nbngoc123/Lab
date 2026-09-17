"""Ingest FBref: standard + advanced player/squad stats -> MinIO."""
import time
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup, Comment
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import SESSION

SRC = "fbref"
D = today()
SEASON = "2025-2026"
LEAGUE_ID = 9              # Premier League

STAT_TYPES = {
    "standard": "",
    "shooting": "shooting/",
    "passing": "passing/",
    "gca": "gca/",
    "defense": "defense/",
    "possession": "possession/",
    "misc": "misc/",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; football-lake/0.1; "
                         "educational project)"}
MIN_DELAY_SEC = 6.0         # FBref khuyến nghị giãn cách vài giây/request


def fetch_page(path: str) -> str:
    url = f"https://fbref.com/en/comps/{LEAGUE_ID}/{path}Premier-League-Stats"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(MIN_DELAY_SEC)
    return r.text


def extract_all_tables(html: str) -> list:
    """
    Bảng nâng cao của FBref nằm trong HTML comment để né scraper đơn giản.
    Ta lấy mọi <table> hiện trực tiếp + mọi <table> ẩn trong comment.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables = []

    # bảng hiện trực tiếp
    for t in soup.find_all("table"):
        tables.append(str(t))

    # bảng ẩn trong comment
    comments = soup.find_all(string=lambda s: isinstance(s, Comment))
    for c in comments:
        if "<table" in c:
            inner = BeautifulSoup(c, "html.parser")
            for t in inner.find_all("table"):
                tables.append(str(t))

    dfs = []
    for html_table in tables:
        try:
            # Requires lxml or html5lib
            df = pd.read_html(StringIO(html_table))[0]
            dfs.append(df)
        except ValueError:
            continue
    return dfs


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """FBref dùng multi-index cột (nhóm 'Performance', 'Expected'...) -> gộp phẳng."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in col if "Unnamed" not in c]).strip("_")
                     for col in df.columns]
    # loại dòng header lặp lại giữa bảng (FBref chèn header phụ mỗi ~25 dòng)
    if "Rk" in df.columns:
        df = df[df["Rk"] != "Rk"]
    return df


# ---------- BRONZE + SILVER ----------
def ingest_stat_type(name: str, path: str):
    print(f"  · {name}...")
    try:
        html = fetch_page(path)
    except Exception as e:
        print(f"    ! lỗi tải trang {name}: {e}")
        return
        
    put_bytes(
        f"bronze/fbref/stats/stat_type={name}/season={SEASON}"
        f"/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")

    tables = extract_all_tables(html)
    if not tables:
        print(f"    ! không tìm thấy bảng nào cho {name}")
        return

    # bảng lớn nhất theo số dòng thường là Player Stats (nhiều cầu thủ hơn đội)
    tables_sorted = sorted(tables, key=len, reverse=True)
    player_df = clean_columns(tables_sorted[0])
    player_df["stat_type"] = name
    player_df["season"] = SEASON
    player_df["ingest_date"] = D

    put_parquet(
        f"silver/players/fbref_player_stats/stat_type={name}"
        f"/season={SEASON}/part-0.parquet",
        player_df, SRC, meta={"rows": len(player_df)})

    # bảng nhỏ hơn (20 dòng ~ số đội) là Squad Stats, nếu tồn tại
    squad_candidates = [t for t in tables if 15 <= len(t) <= 25]
    if squad_candidates:
        squad_df = clean_columns(squad_candidates[0])
        squad_df["stat_type"] = name
        squad_df["season"] = SEASON
        put_parquet(
            f"silver/teams/fbref_squad_stats/stat_type={name}"
            f"/season={SEASON}/part-0.parquet",
            squad_df, SRC, meta={"rows": len(squad_df)})

    print(f"    ✓ {len(player_df)} dòng cầu thủ")


def ingest_all():
    print(f"[1/1] FBref: {len(STAT_TYPES)} loại stats")
    for name, path in STAT_TYPES.items():
        ingest_stat_type(name, path)
