"""Ingest Football-Data.co.uk: CSV nhiều mùa, xử lý schema drift."""
import io
import json
import pandas as pd
from lake.minio_io import (put_bytes, put_parquet, read_bytes, exists,
                           summary, S3, BUCKET)
from lake.http import get
from lake.team_lookup import add_team_key
import os

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "football-data.co.uk"
BASE = "https://www.football-data.co.uk/mmz4281"

Backfill 10 mùa EPL + Championship
Mã mùa: "1516" = 2015/16, "2425" = 2024/25
DIVISIONS = ["E0"] if TEST_MODE else ["E0", "E1"]   # E0=EPL, E1=Championship
SEASONS = ["2425"] if TEST_MODE else [
    "1516", "1617", "1718", "1819", "1920",
    "2021", "2122", "2223", "2324", "2425",
]

CORE = ["Div", "Date", "Time", "HomeTeam", "AwayTeam",
        "FTHG", "FTAG", "FTR", "HTHG", "HTAG", "HTR",
        "HS", "AS", "HST", "AST", "HC", "AC",
        "HY", "AY", "HR", "AR", "Referee"]

ODDS_BOOKS = ["B365", "BW", "IW", "PS", "WH", "VC", "Avg", "Max"]


def _season_label(raw: str) -> str:
    """'2425' → '2024-25' (định dạng chuẩn toàn hệ thống)."""
    return f"20{raw[:2]}-{raw[2:]}"


---------- BRONZE ----------
def download_all() -> dict:
    """Tải raw CSV, không parse. Trả về dict {(div, season): key}."""
    keys, failed = {}, []
    for div in DIVISIONS:
        for season in SEASONS:
            key = f"bronze/football_data_couk/{div}/season={season}/{div}.csv"
            if exists(key):
                print(f"  · {div}/{season} đã có, bỏ qua")
                keys[(div, season)] = key
                continue
            url = f"{BASE}/{season}/{div}.csv"
            try:
                content = get(url).content
            except Exception as e:
                print(f"  ! {div}/{season} lỗi: {e}")
                failed.append((div, season))
                continue
            put_bytes(key, content, SRC, content_type="text/csv",
                      meta={"division": div, "season": season,
                            "source_url": url})
            keys[(div, season)] = key
    if failed:
        print(f"  ! {len(failed)} file không tải được: {failed}")
    return keys


---------- phân tích schema drift ----------
def schema_report(keys: dict):
    report = {}
    all_cols = set()
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key, nrows=1)
        cols = list(df.columns)
        report[f"{div}_{season}"] = {"n_cols": len(cols), "cols": cols}
        all_cols |= set(cols)

    if not report:
        print("  ! Không có file nào được tải về, bỏ qua report.")
        return {}

    # cột nào xuất hiện ở mọi file?
    common = set.intersection(*[set(v["cols"]) for v in report.values()])
    report["_summary"] = {
        "total_distinct_columns": len(all_cols),
        "columns_present_in_all_files": sorted(common),
        "n_common": len(common),
    }
    S3.put_object(
        Bucket=BUCKET, Key="_meta/football_data_couk/schema_report.json",
        Body=json.dumps(report, indent=2).encode(),
        ContentType="application/json")
    print(f"  ✓ schema drift: {len(all_cols)} cột khác nhau, "
          f"{len(common)} cột chung cho mọi file")
    return report


def _read_csv(key: str, **kw) -> pd.DataFrame:
    """CSV của site này encoding lộn xộn + có dòng rỗng cuối file."""
    raw = read_bytes(key)
    for enc in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(raw), encoding=enc,
                               on_bad_lines="skip", **kw)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Không decode được {key}")


def _parse_date(s: pd.Series) -> pd.Series:
    """Mùa cũ dùng dd/mm/yy, mùa mới dd/mm/yyyy. Thử cả hai."""
    d = pd.to_datetime(s, format="%d/%m/%Y", errors="coerce")
    fallback = pd.to_datetime(s, format="%d/%m/%y", errors="coerce")
    return d.fillna(fallback)


---------- SILVER ----------
def build_matches(keys: dict):
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key)
        df = df.dropna(subset=["HomeTeam", "AwayTeam"])   # bỏ dòng rác cuối file
        if df.empty:
            continue

        # chuẩn hoá: cột nào thiếu -> tạo với NaN, giữ schema đồng nhất
        out = df.reindex(columns=CORE).copy()
        out["Date"] = _parse_date(out["Date"])
        out["division"] = div
        season_label = _season_label(season)
        out["season"] = season_label

        for c in ["FTHG", "FTAG", "HTHG", "HTAG", "HS", "AS", "HST", "AST",
                  "HC", "AC", "HY", "AY", "HR", "AR"]:
            out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

        out["total_goals"] = out["FTHG"] + out["FTAG"]
        out["match_id"] = (out["division"] + "_" + season_label + "_"
                           + out["Date"].dt.strftime("%Y%m%d") + "_"
                           + out["HomeTeam"].str.replace(" ", "")
                           + "_" + out["AwayTeam"].str.replace(" ", ""))

        out = out.rename(columns={
            "Date": "match_date", "HomeTeam": "home_team",
            "AwayTeam": "away_team", "FTHG": "home_goals",
            "FTAG": "away_goals", "FTR": "result"})

        # Thêm team_key chuẩn để join với các nguồn khác
        out = add_team_key(out, "home_team", source="fd", out_col="home_team_key")
        out = add_team_key(out, "away_team", source="fd", out_col="away_team_key")

        put_parquet(
            f"silver/matches/fd_matches/division={div}/season={season_label}/part-0.parquet",
            out, SRC, meta={"division": div, "season": season_label})


def build_odds(keys: dict):
    """Tách odds ra bảng riêng, unpivot thành long format."""
    for (div, season), key in sorted(keys.items()):
        df = _read_csv(key).dropna(subset=["HomeTeam", "AwayTeam"])
        if df.empty:
            continue
        season_label = _season_label(season)
        date = _parse_date(df["Date"])
        mid = (div + "_" + season_label + "_"
               + date.dt.strftime("%Y%m%d") + "_"
               + df["HomeTeam"].str.replace(" ", "") + "_"
               + df["AwayTeam"].str.replace(" ", ""))

        rows = []
        missing_books = []
        for book in ODDS_BOOKS:
            cols = [f"{book}H", f"{book}D", f"{book}A"]
            present = [c for c in cols if c in df.columns]
            if len(present) < 3:
                # Issue #5: log rõ ràng thay vì im lặng bỏ qua
                missing_books.append(book)
                continue
            part = pd.DataFrame({
                "match_id": mid,
                "bookmaker": book,
                "odds_home": pd.to_numeric(df[cols[0]], errors="coerce"),
                "odds_draw": pd.to_numeric(df[cols[1]], errors="coerce"),
                "odds_away": pd.to_numeric(df[cols[2]], errors="coerce"),
            })
            rows.append(part.dropna(subset=["odds_home"]))

        if missing_books:
            print(f"  ⚠ {div}/{season_label}: thiếu cột odds cho: {missing_books}")
        if not rows:
            continue
        odds = pd.concat(rows, ignore_index=True)
        # margin của nhà cái = tổng xác suất ngầm - 1
        odds["implied_margin"] = (
            1/odds.odds_home + 1/odds.odds_draw + 1/odds.odds_away - 1).round(4)

        put_parquet(
            f"silver/odds/fd_odds/division={div}/season={season_label}/part-0.parquet",
            odds, SRC)


if __name__ == "__main__":
    print("[1/4] tải CSV")
    keys = download_all()
    print(f"      {len(keys)} file trong lake")

    print("[2/4] phân tích schema drift")
    schema_report(keys)

    print("[3/4] silver: matches")
    build_matches(keys)

    print("[4/4] silver: odds")
    build_odds(keys)

    summary("bronze/football_data_couk/")
    summary("silver/matches/fd_matches/")
    summary("silver/odds/")
