"""Scrape PhysioRoom Premier League Injury Table -> MinIO."""
import pandas as pd
from io import StringIO
from lake.minio_io import put_bytes, put_json_gz, put_parquet, today, summary, exists
from lake.http import SESSION

SRC = "physioroom"
D = today()
URL = "https://www.physioroom.com/advice/premier-league-injury-table/"


# ---------- BRONZE ----------
def fetch_page() -> str:
    r = SESSION.get(URL, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
    r.raise_for_status()
    html = r.text
    put_bytes(
        f"bronze/physioroom/injury_table/ingest_date={D}/page.html",
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
            f"trang có thể đã đổi cấu trúc, kiểm tra page.html đã lưu ở bronze")

    raw_data = [t.to_dict(orient="records") for t in tables[:4]]
    put_json_gz(
        f"bronze/physioroom/injury_table/ingest_date={D}/tables_raw.json.gz",
        raw_data, SRC, meta={"n_tables": len(tables)})
    return raw_data


# ---------- SILVER ----------
def build_club_summary(t0_raw: list) -> pd.DataFrame:
    t0 = pd.DataFrame(t0_raw)
    df = t0.rename(columns={
        "Team": "team", "Total Injuries": "total_injuries", "Position": "rank"})
    df["ingest_date"] = D
    put_parquet(
        f"silver/dim/pr_club_injury_summary/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


def build_player_injuries(t1_raw: list) -> pd.DataFrame:
    """
    Bảng gốc: 1 dòng/CLB, 2 cột dạng list phân cách dấu phẩy cùng độ dài.
    Explode thành 1 dòng/cầu thủ.
    """
    t1 = pd.DataFrame(t1_raw)
    rows = []
    for _, r in t1.iterrows():
        team = r.get("Team")
        if not team: continue
        
        # Handle NA or empty cases gracefully
        players_raw = str(r.get("Injured Players", ""))
        details_raw = str(r.get("Injury Details", ""))
        
        if players_raw == "nan" or not players_raw:
            continue
            
        players = [p.strip() for p in players_raw.split(",")]
        details = [d.strip() for d in details_raw.split(",")]
        
        if len(players) != len(details):
            print(f"  ! {team}: lệch số lượng player ({len(players)}) "
                  f"vs injury ({len(details)}) — bỏ qua dòng lỗi này")
            continue
            
        for p, d_ in zip(players, details):
            rows.append({"team": team, "player_name": p,
                        "injury_type": d_, "ingest_date": D})
                        
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(
            f"silver/players/pr_player_injuries/ingest_date={D}/part-0.parquet",
            df, SRC, meta={"rows": len(df)})
    return df


def build_injury_frequency(t3_raw: list) -> pd.DataFrame:
    t3 = pd.DataFrame(t3_raw)
    df = t3.rename(columns={"Injury Type": "injury_type", "Count": "count"})
    df["ingest_date"] = D
    put_parquet(
        f"silver/dim/pr_injury_type_frequency/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


def run_pipeline():
    print("[1/3] Bronze: tải trang PhysioRoom")
    html = fetch_page()

    print("[2/3] Bronze: parse bảng")
    tables = parse_tables(html)

    print("[3/3] Silver: build dim tables")
    build_club_summary(tables[0])
    build_player_injuries(tables[1])
    build_injury_frequency(tables[3])

    summary("bronze/physioroom/")
    summary("silver/dim/pr_")
    summary("silver/players/pr_")


if __name__ == "__main__":
    run_pipeline()

