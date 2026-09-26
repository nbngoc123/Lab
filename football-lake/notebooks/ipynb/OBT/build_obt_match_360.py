#!/usr/bin/env python3
"""
build_obt_match_360.py — silver -> gold/obt/obt_match_360.parquet

KHÔNG PHẢI feature ML (không rolling, không quy tắc chống leakage). Đây là bảng OBT
mô tả "1 trận đấu có những thông tin gì?" theo đúng tài liệu OBT (mục 4.1) — dùng cho
Analytics/BI, KHÔNG dùng trực tiếp để train model (dùng gold/features/feature_match_ml
cho mục đích đó).

Đặt tại: football-lake/ml/obt/build_obt_match_360.py
Chạy:
    python ml/obt/build_obt_match_360.py                  # MinIO
    python ml/obt/build_obt_match_360.py --root ./lake     # thư mục local (thử nghiệm)

Grain: 1 dòng = 1 match_id (trận đã đá xong, có tỉ số) — assert cuối script để bắt lỗi
       nếu JOIN nào đó làm phình số dòng.

ĐỌC silver (tái sử dụng loader của build_features.py qua common_obt.py):
    matches/fd_matches                      (p03, bắt buộc — spine)
    odds/fd_odds                            (p03, tùy chọn — market 1x2)
    matches/fd_matches_weather              (p13, tùy chọn — thời tiết)
    matches/understat_match_xg              (p19, tùy chọn — xG THỰC trận, không rolling)
    matches/af_fixtures + af_match_stats    (p24, tùy chọn — thống kê trận THỰC, không rolling)
    players/pr_player_injuries              (p16, tùy chọn — snapshot chấn thương as-of trận)
    text/google_news_articles               (p20, tùy chọn — độ ồn truyền thông 7 ngày trước trận)
    dim/tsdb_teams                          (p08, tùy chọn — mô tả đội: sân, sức chứa, năm thành lập)
    dim/wd_clubs                            (p06, tùy chọn — mô tả đội fallback)
GHI gold/obt/obt_match_360.parquet

KHÔNG đưa vào OBT (đã cân nhắc, xem trao đổi trước khi build):
    - youtube_videos/comments (p21): không có khóa liên kết match_id rõ ràng
      (chỉ là kết quả search theo từ khóa chung, không gắn 1 trận cụ thể).
    - football_news_agg (p25): tương tự, chưa có khóa liên kết trận.
    - odds handicap/totals (p22): chỉ có kèo TRẬN SẮP ĐẤU (API free), gần như toàn NULL
      với trận lịch sử -> có thể bật lại bằng load_odds_handicap() nếu cần cho trận tương lai.
    - fdo_matches (p09): chỉ dùng để ĐỐI CHIẾU/QA với fd_matches, không phải nguồn feature.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

from common_obt import (
    LocalStore, MinioStore, find_seed, load_alias, load_matches, load_odds,
    load_weather, load_team_dim_tsdb, load_team_dim_wikidata,
    load_match_xg_understat, load_af_match_stats_actual, load_injuries, load_news_buzz,
)

OUT = "gold/obt/obt_match_360.parquet"


def build_market_1x2(odds: pd.DataFrame) -> pd.DataFrame:
    """1 trận có thể có nhiều nhà cái -> gộp thành 1 dòng/trận (mục 4.1.4 tài liệu OBT:
    'Không nên JOIN trực tiếp dữ liệu có nhiều bản ghi Odds nếu làm match_id nhân dòng')."""
    cols = ["match_id", "odds_home_avg", "odds_draw_avg", "odds_away_avg", "n_bookmakers"]
    if odds.empty:
        return pd.DataFrame(columns=cols)
    g = (odds.groupby("match_id")
              .agg(odds_home_avg=("odds_home", "mean"), odds_draw_avg=("odds_draw", "mean"),
                   odds_away_avg=("odds_away", "mean"), n_bookmakers=("bookmaker", "nunique"))
              .reset_index())
    return g


def build_injuries_asof(inj: pd.DataFrame, m: pd.DataFrame) -> pd.DataFrame:
    """As-of join: snapshot chấn thương GẦN NHẤT TRƯỚC ngày đá cho mỗi đội (home/away).
    Đây chỉ là bối cảnh mô tả tại thời điểm trận đấu, KHÔNG phải feature rolling ML."""
    out = m[["match_id"]].copy()
    if inj.empty:
        out["home_n_injured_players"] = None; out["home_total_injuries"] = None
        out["away_n_injured_players"] = None; out["away_total_injuries"] = None
        return out
    inj = inj.copy()
    if "total_injuries" not in inj.columns:  # pr_club_injury_summary có thể trống
        inj["total_injuries"] = float("nan")
    inj_sorted = inj.assign(ingest_date=pd.to_datetime(inj["ingest_date"])).sort_values("ingest_date")
    for side, key_col in (("home", "home_key"), ("away", "away_key")):
        mm = (m[["match_id", "match_date", key_col]]
              .rename(columns={key_col: "team_key"}).sort_values("match_date"))
        mm = mm.assign(match_date=pd.to_datetime(mm["match_date"]))
        # merge_asof yêu cầu cột "on" là kiểu số/datetime64, không nhận dtype date object
        merged = pd.merge_asof(mm, inj_sorted, left_on="match_date", right_on="ingest_date",
                                by="team_key", direction="backward")
        merged = merged.rename(columns={"n_injured_players": f"{side}_n_injured_players",
                                        "total_injuries": f"{side}_total_injuries"})
        out = out.merge(merged[["match_id", f"{side}_n_injured_players", f"{side}_total_injuries"]],
                         on="match_id", how="left")
    return out


def build_news_context(news: pd.DataFrame, m: pd.DataFrame, days: int = 7) -> pd.DataFrame:
    """Số bài báo nhắc tới đội trong N ngày trước trận — bối cảnh mô tả, KHÔNG phải feature
    rolling chuẩn ML (OBT không cần áp quy tắc chống leakage nghiêm ngặt như Feature layer,
    vì OBT chỉ mô tả lại, không dùng để dự đoán)."""
    out = m[["match_id"]].copy()
    col_h, col_a = f"home_news_mentions_{days}d", f"away_news_mentions_{days}d"
    if news.empty:
        out[col_h] = None; out[col_a] = None
        return out
    for side, key_col, col in (("home", "home_key", col_h), ("away", "away_key", col_a)):
        vals = []
        for r in m.itertuples():
            tk = getattr(r, key_col)
            lo = pd.Timestamp(r.match_date) - pd.Timedelta(days=days)
            sub = news[(news.team_key == tk) & (news.date < r.match_date) & (news.date >= lo)]
            vals.append(int(sub["n_mentions"].sum()) if not sub.empty else 0)
        out[col] = vals
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None, help="thư mục local thay vì MinIO (thử nghiệm)")
    ap.add_argument("--division", default="E0")
    ap.add_argument("--seed", default=None)
    args = ap.parse_args([] if ("ipykernel" in sys.modules or "google.colab" in sys.modules) else None)

    store = LocalStore(args.root) if args.root else MinioStore()
    alias = load_alias(Path(args.seed) if args.seed else find_seed())

    print("[1/7] spine: fd_matches (p03)")
    m = load_matches(store, alias, args.division)

    print("[2/7] dimension mô tả đội: tsdb_teams (p08) + wd_clubs (p06, fallback)")
    dim_team = load_team_dim_tsdb(store, alias).merge(
        load_team_dim_wikidata(store, alias), on="team_key", how="outer")

    print("[3/7] market 1x2: fd_odds (p03)")
    odds_1x2 = build_market_1x2(load_odds(store))

    print("[4/7] thời tiết: fd_matches_weather (p13)")
    weather = load_weather(store)

    print("[5/7] xG thực trận: understat_match_xg (p19)")
    match_xg = load_match_xg_understat(store, alias, m)

    print("[6/7] thống kê trận thực: af_fixtures + af_match_stats (p24)")
    af_stats = load_af_match_stats_actual(store, alias, m)

    print("[7/7] bối cảnh: chấn thương as-of (p16) + độ ồn truyền thông (p20)")
    injuries = build_injuries_asof(load_injuries(store, alias), m)
    news = build_news_context(load_news_buzz(store, alias), m)

    # ---------------- JOIN cuối cùng — luôn giữ Grain 1 dòng/match_id ----------------
    obt = m.merge(dim_team.add_prefix("home_").rename(columns={"home_team_key": "home_key_2"}),
                  left_on="home_key", right_on="home_key_2", how="left").drop(columns=["home_key_2"])
    obt = obt.merge(dim_team.add_prefix("away_").rename(columns={"away_team_key": "away_key_2"}),
                    left_on="away_key", right_on="away_key_2", how="left").drop(columns=["away_key_2"])
    obt = obt.merge(odds_1x2, on="match_id", how="left")
    obt = obt.merge(weather, on="match_id", how="left")
    obt = obt.merge(match_xg, on="match_id", how="left")
    obt = obt.merge(af_stats, on="match_id", how="left")
    obt = obt.merge(injuries, on="match_id", how="left")
    obt = obt.merge(news, on="match_id", how="left")

    assert obt["match_id"].is_unique, "VỠ GRAIN: match_id bị trùng sau JOIN — kiểm tra dimension đội"
    assert len(obt) == len(m), f"VỠ GRAIN: {len(obt)} dòng OBT != {len(m)} trận gốc từ fd_matches"

    cov = obt.notna().mean().round(2)
    print("\n[QA] Grain OK (1 dòng/match_id). Độ phủ theo nhóm cột:")
    for grp, cols in {
        "dim_team": [c for c in obt.columns if c.startswith(("home_stadium", "away_stadium"))],
        "market": ["odds_home_avg"], "weather": ["temp_c"],
        "xg_actual": ["home_xg_actual"], "stats_af": ["home_possession_pct"],
        "injuries": ["home_n_injured_players"], "news": ["home_news_mentions_7d"],
    }.items():
        cols = [c for c in cols if c in cov.index]
        if cols:
            print(f"  · {grp:10s}: {cov[cols].mean():.0%}")

    print(f"\n✓ obt_match_360: {len(obt):,} dòng, {obt.shape[1]} cột, "
          f"{obt['match_date'].min()} -> {obt['match_date'].max()}")
    store.write_parquet(OUT, obt)


if __name__ == "__main__":
    main()
