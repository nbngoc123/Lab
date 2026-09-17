"""Ingest FBref: standard + advanced player/squad stats + match logs -> MinIO."""
import time
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup, Comment
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import SESSION

SRC = "fbref"
D = today()
SEASON = "2025-2026"

# TRUE để chạy thử nhanh (1 giải, vài cầu thủ). Sửa thành FALSE trên production để cào full (sẽ mất ~4 tiếng)
TEST_MODE = True

LEAGUES = {
    9: "Premier-League",
    12: "La-Liga",
    11: "Serie-A",
    20: "Bundesliga",
    13: "Ligue-1"
}

STAT_TYPES = {
    "standard": "",
    "shooting": "shooting/",
    "passing": "passing/",
    "passing_types": "passing_types/",
    "gca": "gca/",
    "defense": "defense/",
    "possession": "possession/",
    "playingtime": "playingtime/",
    "misc": "misc/",
    "keepers": "keepers/",
    "keepersadv": "keepersadv/"
}

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
MIN_DELAY_SEC = 6.0

def fetch_page(url: str) -> str:
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(MIN_DELAY_SEC)
    return r.text


def extract_all_tables_and_players(html: str) -> tuple[list, dict]:
    """
    Trả về (danh_sach_DataFrames, dict_player_ids).
    Bảng nâng cao nằm trong HTML comment.
    Đồng thời quét thẻ <a> để lấy id cầu thủ phục vụ cào match logs.
    """
    soup = BeautifulSoup(html, "html.parser")
    tables_html = []
    player_ids = {}

    def extract_links(html_node):
        for a in html_node.find_all("a", href=True):
            href = a["href"]
            # VD: /en/players/1234abcd/John-Doe
            if "/players/" in href and "/matchlogs/" not in href:
                parts = href.split("/")
                if len(parts) >= 5 and parts[3] == "players":
                    pid = parts[4]
                    name = a.text.strip()
                    if name and pid and name != "Matches":
                        player_ids[name] = pid

    # 1. Bảng hiện trực tiếp
    for t in soup.find_all("table"):
        tables_html.append(str(t))
    extract_links(soup)

    # 2. Bảng ẩn trong comment
    comments = soup.find_all(string=lambda s: isinstance(s, Comment))
    for c in comments:
        if "<table" in c:
            inner = BeautifulSoup(c, "html.parser")
            for t in inner.find_all("table"):
                tables_html.append(str(t))
            extract_links(inner)

    dfs = []
    for html_table in tables_html:
        try:
            df = pd.read_html(StringIO(html_table))[0]
            dfs.append(df)
        except ValueError:
            continue
            
    return dfs, player_ids


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """FBref dùng multi-index cột -> gộp phẳng."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([c for c in col if "Unnamed" not in c]).strip("_")
                     for col in df.columns]
    if "Rk" in df.columns:
        df = df[df["Rk"] != "Rk"]
    return df


def ingest_league_stats(league_id: int, league_name: str) -> dict:
    print(f"\n=== Giải {league_name} ===")
    all_players = {}
    
    stat_items = list(STAT_TYPES.items())
    if TEST_MODE:
        stat_items = stat_items[:2]  # Test 2 loại stats
        
    for name, path in stat_items:
        print(f"  · {name}...")
        url = f"https://fbref.com/en/comps/{league_id}/{path}{league_name}-Stats"
        
        try:
            html = fetch_page(url)
        except Exception as e:
            print(f"    ! lỗi tải trang {name}: {e}")
            continue
            
        put_bytes(
            f"bronze/fbref/stats/league={league_id}/stat_type={name}/season={SEASON}"
            f"/ingest_date={D}/page.html",
            html.encode("utf-8"), SRC, content_type="text/html")

        tables, p_ids = extract_all_tables_and_players(html)
        all_players.update(p_ids)
        
        if not tables:
            print(f"    ! không tìm thấy bảng nào cho {name}")
            continue

        tables_sorted = sorted(tables, key=len, reverse=True)
        player_df = clean_columns(tables_sorted[0])
        player_df["stat_type"] = name
        player_df["league_id"] = league_id
        player_df["season"] = SEASON
        player_df["ingest_date"] = D

        put_parquet(
            f"silver/players/fbref_player_stats/league={league_id}/stat_type={name}"
            f"/season={SEASON}/part-0.parquet",
            player_df, SRC, meta={"rows": len(player_df)})

        squad_candidates = [t for t in tables if 15 <= len(t) <= 25]
        if squad_candidates:
            squad_df = clean_columns(squad_candidates[0])
            squad_df["stat_type"] = name
            squad_df["league_id"] = league_id
            squad_df["season"] = SEASON
            put_parquet(
                f"silver/teams/fbref_squad_stats/league={league_id}/stat_type={name}"
                f"/season={SEASON}/part-0.parquet",
                squad_df, SRC, meta={"rows": len(squad_df)})

        print(f"    ✓ {len(player_df)} dòng cầu thủ")
        
    return all_players


def ingest_player_match_logs(player_id: str, player_name: str):
    print(f"  · [Match Log] {player_name} ({player_id})...")
    url = f"https://fbref.com/en/players/{player_id}/matchlogs/{SEASON}/summary/"
    
    try:
        html = fetch_page(url)
    except Exception as e:
        print(f"    ! lỗi tải match log: {e}")
        return
        
    dfs, _ = extract_all_tables_and_players(html)
    if not dfs:
        return
        
    df = clean_columns(dfs[0])
    df["player_id"] = player_id
    df["player_name"] = player_name
    df["season"] = SEASON
    df["ingest_date"] = D
    
    # Bỏ các dòng là tổng kết giải đấu (Date bị rỗng hoặc chứa chữ không phải ngày)
    if "Date" in df.columns:
        df = df[df["Date"].notna() & df["Date"].str.match(r"^\d{4}-\d{2}-\d{2}")]
        
    if not df.empty:
        put_parquet(
            f"silver/players/fbref_match_logs/player_id={player_id}"
            f"/season={SEASON}/part-0.parquet",
            df, SRC, meta={"rows": len(df)})


def ingest_all():
    print(f"[1/1] Bắt đầu lấy dữ liệu FBref (TEST_MODE={TEST_MODE})")
    
    leagues = list(LEAGUES.items())
    if TEST_MODE:
        leagues = leagues[:1] # Test 1 giải
        
    for lid, lname in leagues:
        players = ingest_league_stats(lid, lname)
        
        print(f"\n=== Lấy Match Logs cầu thủ ({len(players)} người) ===")
        player_items = list(players.items())
        
        if TEST_MODE:
            player_items = player_items[:3] # Test 3 cầu thủ
            
        for name, pid in player_items:
            ingest_player_match_logs(pid, name)
