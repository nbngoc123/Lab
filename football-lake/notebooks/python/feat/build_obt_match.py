#!/usr/bin/env python3
"""
build_obt_match.py — silver -> gold/obt/obt_match_360.parquet

ĐÂY LÀ OBT (One Big Table), KHÔNG PHẢI Feature cho Machine Learning.
Grain: 1 dòng = 1 match_id (chỉ các trận ĐÃ ĐÁ, có kết quả -- giống fd_matches ở build_features.py).

Khác biệt quan trọng so với ml/features/../build_features.py:
    - KHÔNG có rolling window (l5/l10), KHÔNG có Elo, KHÔNG có split/is_warm/target_*.
      Những cột đó thuộc Feature layer, không thuộc OBT (xem "Hướng dẫn triển khai OBT" mục 8).
    - Odds/xG/stats ở đây là thông tin THÔ của CHÍNH trận đó (vd "trận này có xG bao nhiêu"),
      không phải giá trị rolling từ các trận trước (khác feature_team_xg/feature_market).
    - Odds nhiều nhà cái được TỔNG HỢP về 1 dòng/trận (trung bình + số nhà cái) thay vì giữ
      nguyên nhiều dòng như silver/odds/fd_odds (đúng nguyên tắc "không phá vỡ Grain" ở mục 6).

Tái dùng trực tiếp từ notebooks/python/build_features.py (không viết lại):
    LocalStore, MinioStore, find_seed, load_alias, map_team, first_present, num,
    load_matches, load_weather, load_af_fixtures, load_af_stats, build_af_bridge.

Đặt tại: football-lake/ml/obt/build_obt_match.py
Chạy:
    python ml/obt/build_obt_match.py                # MinIO
    python ml/obt/build_obt_match.py --root ./lake --seed ./seed/team_alias.csv   # local

ĐỌC silver:
    matches/fd_matches, odds/fd_odds, matches/fd_matches_weather, teams/understat_team_xg,
    matches/af_fixtures, matches/af_match_stats   (dùng lại bridge_fd_af_match để lấy stats CHÍNH trận đó)
GHI:
    gold/obt/obt_match_360.parquet
"""
import sys
import argparse
from pathlib import Path

import duckdb
import pandas as pd

_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parents[2] / "notebooks" / "python"))
from build_features import ( 
    LocalStore, MinioStore, find_seed, load_alias, map_team, first_present, num,
    load_matches, load_weather, load_af_fixtures, load_af_stats, build_af_bridge,
)

OUT = "gold/obt"


def load_odds_summary(store):
    """silver/odds/fd_odds có NHIỀU dòng/trận (1 dòng/bookmaker) -> tổng hợp về 1 dòng/trận
    (trung bình odds thô + số nhà cái), KHÔNG tính xác suất ngụ ý/margin (đó là việc của
    feature_market bên Feature layer, không thuộc OBT)."""
    o = store.read_prefix("silver/odds/fd_odds/")
    cols = ["match_id", "odds_home_avg", "odds_draw_avg", "odds_away_avg", "n_bookmakers"]
    if o.empty:
        print("  ! không có fd_odds -> cột odds trong OBT sẽ NULL")
        return pd.DataFrame(columns=cols)
    o = o.sort_values("_key").drop_duplicates(["match_id", "bookmaker"], keep="last").copy()
    for c in ("odds_home", "odds_draw", "odds_away"):
        o[c] = pd.to_numeric(o[c], errors="coerce")
    g = (o.groupby("match_id")
           .agg(odds_home_avg=("odds_home", "mean"), odds_draw_avg=("odds_draw", "mean"),
                odds_away_avg=("odds_away", "mean"), n_bookmakers=("bookmaker", "nunique"))
           .reset_index())
    print(f"  · odds: tổng hợp {len(o):,} dòng (nhiều nhà cái) -> {len(g):,} trận (1 dòng/trận)")
    return g


def load_team_xg_match(store, alias):
    """xG CỦA CHÍNH trận đó (team_key + match_date, không rolling/không lag) -- khác
    feature_team_xg (rolling l5/l10 từ các trận TRƯỚC). OBT trả lời 'trận này có xG bao nhiêu',
    không phải 'phong độ xG gần đây của đội là bao nhiêu' (đó là Feature)."""
    u = store.read_prefix("silver/teams/understat_team_xg/")
    cols = ["team_key", "match_date", "xg", "xga", "npxg", "ppda"]
    if u.empty:
        print("  ! không có understat_team_xg -> xG trong OBT sẽ NULL")
        return pd.DataFrame(columns=cols)
    if "league" in u.columns:
        u = u[u["league"] == "EPL"]
    u = u.sort_values("_key")
    out = pd.DataFrame({
        "team_key": map_team(u["team_name"], "understat", alias, "understat_team_xg.team_name").values,
        "match_date": pd.to_datetime(u["date"], errors="coerce").dt.date.values,
        "xg": num(u, "xG").values, "xga": num(u, "xGA").values,
        "npxg": num(u, "npxG").values, "ppda": num(u, "ppda").values})
    return out.dropna(subset=["match_date"]).drop_duplicates(["team_key", "match_date"], keep="last")


def build_obt_match(con):
    con.execute("""
    CREATE OR REPLACE TABLE obt_match_360 AS
    SELECT
      m.match_id, m.division, m.season, m.match_date, m.kickoff_hour,
      dayofweek(m.match_date) AS dow, month(m.match_date) AS month,
      m.home_team, m.away_team, m.home_key, m.away_key,
      m.home_goals, m.away_goals, m.result,
      m.home_goals + m.away_goals AS total_goals,
      od.odds_home_avg, od.odds_draw_avg, od.odds_away_avg, od.n_bookmakers,
      wx.temp_c, wx.precip_mm, wx.wind_kmh,
      hx.xg AS home_xg, hx.xga AS home_xga, hx.npxg AS home_npxg, hx.ppda AS home_ppda,
      ax.xg AS away_xg, ax.xga AS away_xga, ax.npxg AS away_npxg, ax.ppda AS away_ppda,
      br.fixture_id AS af_fixture_id,
      hs.possession_pct AS home_possession_pct, hs.shots_on_goal AS home_shots_on_goal,
      hs.total_shots AS home_total_shots, hs.corner_kicks AS home_corner_kicks, hs.fouls AS home_fouls,
      as_.possession_pct AS away_possession_pct, as_.shots_on_goal AS away_shots_on_goal,
      as_.total_shots AS away_total_shots, as_.corner_kicks AS away_corner_kicks, as_.fouls AS away_fouls
    FROM m
    LEFT JOIN odds_summary od ON od.match_id = m.match_id
    LEFT JOIN wx ON wx.match_id = m.match_id
    LEFT JOIN team_xg_match hx ON hx.team_key = m.home_key AND hx.match_date = m.match_date
    LEFT JOIN team_xg_match ax ON ax.team_key = m.away_key AND ax.match_date = m.match_date
    LEFT JOIN bridge_fd_af_match br ON br.match_id = m.match_id
    LEFT JOIN af_stats_raw hs ON hs.fixture_id = br.fixture_id AND hs.team_key = m.home_key
    LEFT JOIN af_stats_raw as_ ON as_.fixture_id = br.fixture_id AND as_.team_key = m.away_key
    ORDER BY m.match_date, m.match_id
    """)


def qa(con):
    n = con.execute("SELECT count(*) FROM obt_match_360").fetchone()[0]
    d = con.execute("SELECT count(DISTINCT match_id) FROM obt_match_360").fetchone()[0]
    assert n == d, f"obt_match_360: match_id trùng ({n} dòng, {d} match_id duy nhất)"
    print(f"\n[QA] obt_match_360: {n:,} dòng, match_id không trùng.")
    cov = con.execute("""
        SELECT round(avg((odds_home_avg IS NOT NULL)::INT), 2) AS odds,
               round(avg((temp_c IS NOT NULL)::INT), 2) AS weather,
               round(avg((home_xg IS NOT NULL)::INT), 2) AS xg,
               round(avg((af_fixture_id IS NOT NULL)::INT), 2) AS af_bridge,
               round(avg((home_possession_pct IS NOT NULL)::INT), 2) AS af_stats
        FROM obt_match_360
    """).df()
    print("  Độ phủ từng nhóm thông tin:")
    print(cov.to_string(index=False))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None)
    ap.add_argument("--division", default="E0")
    ap.add_argument("--seed", default=None)
    if "ipykernel" in sys.modules or "google.colab" in sys.modules:
        a = ap.parse_args([])
    else:
        a = ap.parse_args()

    store = LocalStore(a.root) if a.root else MinioStore()
    alias = load_alias(Path(a.seed) if a.seed else find_seed())

    print("[1/4] đọc silver: fd_matches (spine) + odds + weather")
    m = load_matches(store, alias, a.division)
    odds_summary = load_odds_summary(store)
    wx = load_weather(store)

    print("[2/4] đọc silver: understat_team_xg (xG của chính trận đó, không rolling)")
    team_xg_match = load_team_xg_match(store, alias)

    print("[3/4] đọc silver: af_fixtures + af_match_stats (bắc cầu match_id để lấy stats chính trận)")
    af_fx = load_af_fixtures(store, alias)
    af_stats_raw = load_af_stats(store, alias)

    con = duckdb.connect()
    for name, df in (("m", m), ("odds_summary", odds_summary), ("wx", wx),
                      ("team_xg_match", team_xg_match), ("af_fx", af_fx), ("af_stats_raw", af_stats_raw)):
        con.register(name, df)
    build_af_bridge(con)   # tạo bảng bridge_fd_af_match (fd match_id <-> af fixture_id)

    print("[4/4] JOIN -> obt_match_360")
    build_obt_match(con)
    qa(con)

    df = con.execute("SELECT * FROM obt_match_360").df()
    store.write_parquet(f"{OUT}/obt_match_360.parquet", df)


if __name__ == "__main__":
    main()
