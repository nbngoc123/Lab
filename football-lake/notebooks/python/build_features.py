#!/usr/bin/env python3
"""
build_features.py — silver -> gold/features (CHỈ tầng feature).

Đặt tại: football-lake/ml/features/build_features.py
Chạy trong container airflow:
    python ml/features/build_features.py                # MinIO
    python ml/features/build_features.py --root ./lake  # thư mục local (thử nghiệm)

ĐỌC silver:
    matches/fd_matches, odds/fd_odds, teams/understat_team_xg,
    text/wm_pageviews (entity=team), matches/fd_matches_weather   (nhóm gốc, bắt buộc/tùy chọn)
    betting/odds_h2h|odds_spreads|odds_totals                     (p22 — kèo handicap/tài xỉu)
    text/google_news_articles, text/wiki_articles(entity=team)    (p20 — độ ồn truyền thông)
    players/pr_player_injuries, dim/pr_club_injury_summary        (p16 — chấn thương, as-of join)
    matches/af_fixtures, af_match_stats                           (p24 — bắc cầu match_id + rolling stats)
    matches/fdo_matches                                           (p09 — CHỈ đối chiếu/QA, không tạo feature)
GHI gold/features/:
    feature_team_match, feature_league_position, feature_elo, feature_market,
    feature_team_xg, feature_team_attention, feature_match_context,
    feature_market_ah, feature_team_news_buzz, feature_team_injuries,
    feature_team_stats_af, bridge_fd_af_match,
    feature_match_ml (1 dòng/trận + target_*), _feature_catalog.csv
GHI gold/dim/: dim_team_wiki_profile (tĩnh, KHÔNG phải feature ML)
GHI gold/qa/: fdo_vs_fd_matches.csv (đối chiếu p09 vs p03, không phải feature)

CHỐNG RÒ RỈ: feature của trận T chỉ dùng dữ liệu (ngày, match_id) < T.
    - odds/kèo (fd_odds, odds_h2h/spreads/totals): chốt trước giờ bóng lăn -> dùng trực tiếp, không rolling.
    - chấn thương (p16): as-of join ingest_date <= match_date (snapshot GẦN NHẤT TRƯỚC trận).
    - stats trận đấu (p24 af_match_stats): là dữ liệu SAU trận -> chỉ dùng làm rolling l5 từ các trận
      TRƯỚC đó (ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), không bao giờ dùng số liệu của chính trận đó.
"""
import argparse, io, os, sys
from pathlib import Path
import duckdb
import pandas as pd

OUT = "gold/features"
DIM_OUT = "gold/dim"
QA_OUT = "gold/qa"
SRC_TAG = "gold-features"

ROLL_FEATS = ["n_prior", "n_prior_season", "rest_days",
    "pts_l5", "gf_l5", "ga_l5", "sot_l5", "sot_against_l5", "shots_l5",
    "corners_l5", "cards_l5", "win_l5", "cs_l5",
    "pts_l10", "gf_l10", "ga_l10",
    "pts_l5_venue", "gf_l5_venue", "ga_l5_venue", "ppg_season", "gd_pg_season"]
FORM_DIFF = ["pts_l5", "gf_l5", "ga_l5", "sot_l5", "pts_l10", "ppg_season", "gd_pg_season", "pts_l5_venue"]
POS_FEATS = ["played_before", "points_before", "gd_before", "rank_before", "is_top6"]
XG_FEATS = ["xg_l5", "xga_l5", "npxg_l5", "xgd_l5", "ppda_l5", "xg_l10", "xga_l10", "xg_overperf_l10"]
XG_DIFF = ["xg_l5", "xga_l5", "xgd_l5"]
ATT_FEATS = ["pv_7d", "pv_28d", "pv_last1", "pv_ratio_7_28"]
MKT_COLS = ["mkt_p_home", "mkt_p_draw", "mkt_p_away", "mkt_margin", "mkt_n_books",
            "pin_p_home", "pin_p_draw", "pin_p_away"]
ELO_COLS = ["elo_home", "elo_away", "elo_diff", "elo_exp_home"]
CTX_COLS = ["kickoff_hour", "dow", "month", "is_weekend", "temp_c", "precip_mm", "wind_kmh", "is_raining"]
AH_COLS = ["ah_home_price_avg", "ah_away_price_avg", "ah_home_point_avg", "ah_n_books",
           "ou_over_price_avg", "ou_under_price_avg", "ou_line_avg", "ou_n_books"]
NEWS_FEATS = ["news_n7", "news_n28"]
NEWS_DIFF = ["news_n28"]
INJ_FEATS = ["n_injured_players", "total_injuries"]
INJ_DIFF = ["n_injured_players"]
AF_FEATS = ["poss_l5", "sog_l5", "shots_total_l5", "corners_af_l5", "fouls_l5"]
AF_DIFF = ["poss_l5", "sog_l5"]
TARGET_COLS = ["target", "target_home_goals", "target_away_goals", "target_total_goals",
               "target_over25", "target_btts"]
KEY_COLS = ["match_id", "division", "season", "match_date", "home_key", "away_key",
            "home_team", "away_team", "split", "is_warm"]
WARM_MIN_MATCHES = 5


class LocalStore:
    def __init__(self, root): self.root = Path(root)
    def read_prefix(self, prefix):
        dfs = []
        for p in sorted((self.root / prefix).rglob("*.parquet")):
            d = pd.read_parquet(p); d["_key"] = p.relative_to(self.root).as_posix(); dfs.append(d)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    def read_file(self, key):
        p = self.root / key
        return pd.read_parquet(p) if p.exists() else pd.DataFrame()
    def write_parquet(self, key, df):
        p = self.root / key; p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False, compression="zstd"); print(f"  ✓ {key}  ({len(df):,} dòng)")
    def write_csv(self, key, df):
        p = self.root / key; p.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(p, index=False); print(f"  ✓ {key}  ({len(df):,} dòng)")


class MinioStore:
    def __init__(self):
        try:
            from lake import minio_io as mio
        except ImportError:
            import os
            base = Path(__file__).resolve() if '__file__' in globals() else Path.cwd()
            for cand in (base.parents[2] if len(base.parents) >= 2 else base, Path("/opt/project"), base):
                sys.path.insert(0, str(cand))
            from lake import minio_io as mio
        self.mio = mio
    def read_prefix(self, prefix):
        dfs = []
        for key, _ in self.mio.list_keys(prefix):
            if key.endswith(".parquet"):
                d = pd.read_parquet(io.BytesIO(self.mio.read_bytes(key))); d["_key"] = key; dfs.append(d)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    def read_file(self, key):
        try:
            return pd.read_parquet(io.BytesIO(self.mio.read_bytes(key)))
        except Exception:
            return pd.DataFrame()
    def write_parquet(self, key, df):
        self.mio.put_parquet(key, df, SRC_TAG); print(f"  ✓ {key}  ({len(df):,} dòng)")
    def write_csv(self, key, df):
        self.mio.put_bytes(key, df.to_csv(index=False).encode("utf-8"), SRC_TAG, content_type="text/csv")
        print(f"  ✓ {key}  ({len(df):,} dòng)")


# ---------------- alias / team_key
def find_seed():
    if os.environ.get("SEED_PATH"): return Path(os.environ["SEED_PATH"])
    here = Path(__file__).resolve() if '__file__' in globals() else Path.cwd()
    for parent in [here, *here.parents]:
        cand = parent / "seed" / "team_alias.csv"
        if cand.exists(): return cand
    sys.exit("Không tìm thấy seed/team_alias.csv (đặt SEED_PATH hoặc --seed)")


def load_alias(seed):
    frames = []
    for p in (seed, seed.with_name("team_alias_extra.csv")):
        if p.exists():
            frames.append(pd.read_csv(p, comment="#", skipinitialspace=True))
            print(f"  · alias: {p.name} ({len(frames[-1])} dòng)")
    df = pd.concat(frames, ignore_index=True)
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["source", "alias", "team_key"])
    for c in ("source", "alias", "team_key"): df[c] = df[c].astype(str).str.strip()
    df["a"] = df["alias"].str.lower()
    by_src = {(r.source, r.a): r.team_key for r in df.drop_duplicates(["source", "a"]).itertuples()}
    any_src = {}
    for r in df.itertuples(): any_src.setdefault(r.a, r.team_key)
    return by_src, any_src


def map_team(names, source, alias, what):
    by_src, any_src = alias
    def f(n):
        if not isinstance(n, str): return None
        k = n.strip().lower()
        return by_src.get((source, k)) or any_src.get(k)
    keys = names.map(f)
    miss = sorted(set(names[keys.isna()].dropna()))
    if miss:
        print(f"  ⚠ {what}: {len(miss)} tên chưa có trong alias (source={source}) -> giữ tên gốc: {miss}")
        print(f"    thêm dòng  {source},<tên>,<team_key>  vào seed/team_alias.csv")
    return keys.fillna(names)


# ---------------- đọc silver
def first_present(df, cands):
    for c in cands:
        if c in df.columns: return c
    return None


def num(df, col):
    if col and col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").astype("float64")
    return pd.Series(float("nan"), index=df.index, dtype="float64")


def load_matches(store, alias, division):
    raw = store.read_prefix("silver/matches/fd_matches/")
    if raw.empty: sys.exit("silver/matches/fd_matches/ trống — chạy p03 trước")
    print(f"  · fd_matches: {len(raw):,} dòng, cột: {[c for c in raw.columns if c != '_key']}")
    need = {"match_id": ["match_id"], "home_team": ["home_team"], "away_team": ["away_team"],
            "home_goals": ["home_goals"], "away_goals": ["away_goals"],
            "date": ["match_date", "date", "utc_date"], "season": ["season"]}
    col = {}
    for k, cands in need.items():
        col[k] = first_present(raw, cands)
        if not col[k]: sys.exit(f"fd_matches thiếu cột {cands}. Hiện có: {list(raw.columns)}")

    raw = raw.sort_values("_key").drop_duplicates(col["match_id"], keep="last")
    dcol = first_present(raw, ["division", "Div"])
    if dcol:
        raw = raw[raw[dcol] == division]
    else:   # silver thật không có cột division; match_id bắt đầu bằng "E0_..."
        sel = raw[raw[col["match_id"]].astype(str).str.startswith(division + "_")]
        if sel.empty:
            print(f"  ! không có match_id bắt đầu bằng '{division}_' -> KHÔNG lọc giải "
                  f"(competition={sorted(raw['competition'].unique()) if 'competition' in raw.columns else '?'})")
        else:
            raw = sel

    dt = pd.to_datetime(raw[col["date"]], errors="coerce")
    hour = pd.Series(float("nan"), index=raw.index)
    if "Time" in raw.columns:
        hour = pd.to_numeric(raw["Time"].astype(str).str.split(":").str[0], errors="coerce")
    elif dt.dt.hour.nunique() > 1:
        hour = dt.dt.hour.astype(float)

    def keys(name_col, key_col):
        if key_col in raw.columns and raw[key_col].notna().all(): return raw[key_col]
        return map_team(raw[name_col], "fd", alias, f"fd_matches.{name_col}")

    m = pd.DataFrame({
        "match_id": raw[col["match_id"]].values,
        "division": division,
        "season": raw[col["season"]].astype(str).values,
        "match_date": dt.dt.normalize().values,
        "kickoff_hour": hour.values,
        "home_team": raw[col["home_team"]].values, "away_team": raw[col["away_team"]].values,
        "home_key": keys(col["home_team"], "home_team_key").values,
        "away_key": keys(col["away_team"], "away_team_key").values,
        "home_goals": num(raw, col["home_goals"]).values, "away_goals": num(raw, col["away_goals"]).values})
    for out_c, in_c in {"h_shots": "HS", "a_shots": "AS", "h_sot": "HST", "a_sot": "AST",
                        "h_corners": "HC", "a_corners": "AC", "h_yellow": "HY", "a_yellow": "AY",
                        "h_red": "HR", "a_red": "AR"}.items():
        m[out_c] = num(raw, in_c).values
    if "HS" not in raw.columns:
        print("  ! fd_matches KHÔNG có HS/HST/HC/HY... -> feature sút/phạt/thẻ sẽ NULL "
              "(cần thêm các cột này vào silver p03 nếu muốn dùng)")

    m = m.dropna(subset=["match_date", "home_goals", "away_goals"]).copy()
    m["match_date"] = pd.to_datetime(m["match_date"]).dt.date
    m["home_goals"] = m["home_goals"].astype(int)
    m["away_goals"] = m["away_goals"].astype(int)
    # nhãn H/D/A tự tính từ tỉ số (silver thật chỉ có 'winner' = HOME_TEAM/AWAY_TEAM/DRAW)
    m["result"] = "D"
    m.loc[m.home_goals > m.away_goals, "result"] = "H"
    m.loc[m.home_goals < m.away_goals, "result"] = "A"
    if m["match_id"].duplicated().any():
        sys.exit(f"match_id trùng {int(m['match_id'].duplicated().sum())} dòng")
    m = m.sort_values(["match_date", "match_id"]).reset_index(drop=True)
    print(f"  · {len(m):,} trận đã đá ({division}), {m.match_date.min()} -> {m.match_date.max()}, "
          f"mùa: {sorted(m.season.unique())}")
    return m


def load_odds(store):
    o = store.read_prefix("silver/odds/fd_odds/")
    if o.empty:
        print("  ! không có fd_odds -> feature_market rỗng")
        return pd.DataFrame(columns=["match_id", "bookmaker", "odds_home", "odds_draw", "odds_away"])
    o = o.sort_values("_key").drop_duplicates(["match_id", "bookmaker"], keep="last")
    for c in ("odds_home", "odds_draw", "odds_away"): o[c] = pd.to_numeric(o[c], errors="coerce")
    return o[["match_id", "bookmaker", "odds_home", "odds_draw", "odds_away"]]


def load_xg(store, alias):
    u = store.read_prefix("silver/teams/understat_team_xg/")
    cols = ["team_key", "match_date", "xg", "xga", "npxg", "ppda", "scored"]
    if u.empty:
        print("  ! không có understat_team_xg -> feature_team_xg rỗng"); return pd.DataFrame(columns=cols)
    if "league" in u.columns: u = u[u["league"] == "EPL"]
    u = u.sort_values("_key")
    out = pd.DataFrame({
        "team_key": map_team(u["team_name"], "understat", alias, "understat_team_xg").values,
        "match_date": pd.to_datetime(u["date"], errors="coerce").dt.date.values,
        "xg": num(u, "xG").values, "xga": num(u, "xGA").values, "npxg": num(u, "npxG").values,
        "ppda": num(u, "ppda").values, "scored": num(u, "scored").values})
    out = out.dropna(subset=["match_date"]).drop_duplicates(["team_key", "match_date"], keep="last")
    print(f"  · understat_team_xg: {len(out):,} dòng, {out.match_date.min()} -> {out.match_date.max()}")
    return out


def load_pageviews(store, alias):
    p = store.read_prefix("silver/text/wm_pageviews/entity=team/")
    if p.empty:
        print("  ! không có wm_pageviews(team) -> feature_team_attention rỗng")
        return pd.DataFrame(columns=["team_key", "date", "views"])
    lab = "label" if "label" in p.columns else "article"
    out = pd.DataFrame({
        "team_key": map_team(p[lab], "wikidata", alias, "wm_pageviews.label").values,
        "date": pd.to_datetime(p["date"], errors="coerce").dt.date.values,
        "views": num(p, "views").values})
    out = out.dropna(subset=["date", "views"]).drop_duplicates(["team_key", "date"], keep="last")
    print(f"  · wm_pageviews(team): {len(out):,} dòng, {out.date.min()} -> {out.date.max()}")
    return out


def load_weather(store):
    w = store.read_prefix("silver/matches/fd_matches_weather/")
    out_cols = ["match_id", "temp_c", "precip_mm", "wind_kmh"]
    if w.empty:
        print("  ! không có fd_matches_weather -> thời tiết NULL"); return pd.DataFrame(columns=out_cols)
    w = w.sort_values("_key").drop_duplicates("match_id", keep="last")
    t = first_present(w, ["temperature_c", "temp_max", "temp"])
    r = first_present(w, ["precipitation_mm", "precipitation_sum"])
    d = first_present(w, ["windspeed_kmh", "wind_speed_max"])
    return pd.DataFrame({"match_id": w["match_id"].values, "temp_c": num(w, t).values,
                         "precip_mm": num(w, r).values, "wind_kmh": num(w, d).values})


# ---------------- p22: odds handicap (spreads) & totals (over/under)
def load_odds_handicap(store, alias):
    """silver/betting/odds_spreads, odds_totals (p22 - The Odds API).
    match_id của Odds API KHÁC hẳn 'E0_...' của fd_matches -> join theo (home_key, away_key, ngày),
    KHÔNG theo match_id. seed/team_alias.csv đã có sẵn source='odds' khớp tên đội của Odds API.
    Giới hạn: bản free chỉ trả kèo trận SẮP diễn ra, không backfill lịch sử."""
    def _empty(cols): return pd.DataFrame(columns=cols)
    sp = store.read_prefix("silver/betting/odds_spreads/")
    if sp.empty:
        print("  ! không có odds_spreads -> ah_* sẽ NULL")
        sp = _empty(["home_key", "away_key", "match_date", "side_key", "price", "point", "bookmaker"])
    else:
        sp = sp.copy()
        sp["home_key"] = map_team(sp["home_team"], "odds", alias, "odds_spreads.home_team")
        sp["away_key"] = map_team(sp["away_team"], "odds", alias, "odds_spreads.away_team")
        sp["side_key"] = map_team(sp["team"], "odds", alias, "odds_spreads.team")
        sp["match_date"] = pd.to_datetime(sp["commence_time"], errors="coerce").dt.date
    tt = store.read_prefix("silver/betting/odds_totals/")
    if tt.empty:
        print("  ! không có odds_totals -> ou_* sẽ NULL")
        tt = _empty(["home_key", "away_key", "match_date", "name", "price", "point", "bookmaker"])
    else:
        tt = tt.copy()
        tt["home_key"] = map_team(tt["home_team"], "odds", alias, "odds_totals.home_team")
        tt["away_key"] = map_team(tt["away_team"], "odds", alias, "odds_totals.away_team")
        tt["match_date"] = pd.to_datetime(tt["commence_time"], errors="coerce").dt.date
    return sp, tt


def build_market_ah(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_market_ah AS
    WITH sp AS (
      SELECT home_key, away_key, match_date,
             AVG(price) FILTER (WHERE side_key = home_key) AS ah_home_price_avg,
             AVG(price) FILTER (WHERE side_key = away_key) AS ah_away_price_avg,
             AVG(point) FILTER (WHERE side_key = home_key) AS ah_home_point_avg,
             COUNT(DISTINCT bookmaker) AS ah_n_books
      FROM ah_spreads GROUP BY 1, 2, 3),
    tt AS (
      SELECT home_key, away_key, match_date,
             AVG(price) FILTER (WHERE name = 'Over')  AS ou_over_price_avg,
             AVG(price) FILTER (WHERE name = 'Under') AS ou_under_price_avg,
             AVG(point) AS ou_line_avg,
             COUNT(DISTINCT bookmaker) AS ou_n_books
      FROM ah_totals GROUP BY 1, 2, 3)
    SELECT m.match_id,
           sp.ah_home_price_avg, sp.ah_away_price_avg, sp.ah_home_point_avg, sp.ah_n_books,
           tt.ou_over_price_avg, tt.ou_under_price_avg, tt.ou_line_avg, tt.ou_n_books
    FROM m
    LEFT JOIN sp ON sp.home_key = m.home_key AND sp.away_key = m.away_key AND sp.match_date = m.match_date
    LEFT JOIN tt ON tt.home_key = m.home_key AND tt.away_key = m.away_key AND tt.match_date = m.match_date
    """)


# ---------------- p20: Wikipedia (dim tĩnh) & Google News (độ ồn truyền thông)
def load_wiki_dim(store):
    """silver/text/wiki_articles/entity=team: bài full-text gần như KHÔNG đổi qua các lần cào
    -> không hợp làm feature rolling theo trận, chỉ ghi ra dim mô tả (gold/dim), không join vào ML."""
    w = store.read_prefix("silver/text/wiki_articles/entity=team/")
    if w.empty:
        print("  ! không có wiki_articles/entity=team -> bỏ qua dim_team_wiki_profile")
        return pd.DataFrame()
    dim = (w.sort_values("_key").drop_duplicates(["title", "lang"], keep="last")
             [["title", "lang", "page_id", "word_count", "fetched_date"]])
    print(f"  · dim_team_wiki_profile: {len(dim)} dòng (tĩnh, KHÔNG phải feature ML)")
    return dim


def load_news_buzz(store, alias):
    """silver/text/google_news_articles: chỉ 2/7 query gắn thẳng 1 CLB -> quét chuỗi 'text'
    (title+summary) tìm bất kỳ tên đội nào có trong seed/team_alias.csv thay vì dựa cột 'query'."""
    n = store.read_prefix("silver/text/google_news_articles/")
    if n.empty:
        print("  ! không có google_news_articles -> feature_team_news_buzz rỗng")
        return pd.DataFrame(columns=["team_key", "date", "n_mentions"])
    n = n.copy()
    n["date"] = pd.to_datetime(n["published_ts"], errors="coerce", utc=True).dt.tz_localize(None).dt.date
    n = n.dropna(subset=["date", "text"])
    _, any_src = alias
    aliases = sorted(any_src.items(), key=lambda kv: -len(kv[0]))  # alias dài trước, tránh khớp nhầm
    rows = []
    for r in n.itertuples():
        low = r.text.lower()
        for al, tk in aliases:
            if al in low: rows.append((tk, r.date))
    out = pd.DataFrame(rows, columns=["team_key", "date"])
    if out.empty:
        return pd.DataFrame(columns=["team_key", "date", "n_mentions"])
    out = out.groupby(["team_key", "date"]).size().reset_index(name="n_mentions")
    print(f"  · news_buzz: quét {len(n):,} bài -> {len(out):,} dòng team-ngày, "
          f"{out.team_key.nunique()} đội được nhắc tới")
    return out


def build_news_buzz(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_team_news_buzz AS
    SELECT tm.match_id, tm.team_key,
      SUM(nb.n_mentions) FILTER (WHERE nb.date >= tm.match_date - 7  AND nb.date < tm.match_date) AS news_n7,
      SUM(nb.n_mentions) FILTER (WHERE nb.date >= tm.match_date - 28 AND nb.date < tm.match_date) AS news_n28
    FROM tm LEFT JOIN nb ON nb.team_key = tm.team_key
    GROUP BY tm.match_id, tm.team_key
    """)


# ---------------- p16: injuries (as-of join, không rolling)
def load_injuries(store, alias):
    """silver/players/pr_player_injuries + dim/pr_club_injury_summary: snapshot theo ingest_date,
    KHÔNG có match_id -> ghép bằng ASOF JOIN (snapshot gần nhất TRƯỚC match_date) ở build_injuries()."""
    p = store.read_prefix("silver/players/pr_player_injuries/")
    s = store.read_prefix("silver/dim/pr_club_injury_summary/")
    cols = ["team_key", "ingest_date", "n_injured_players", "total_injuries"]
    if p.empty:
        print("  ! không có pr_player_injuries -> feature_team_injuries rỗng")
        return pd.DataFrame(columns=cols)
    p = p.copy()
    p["team_key"] = map_team(p["team"], "physioroom", alias, "pr_player_injuries.team")
    p["ingest_date"] = pd.to_datetime(p["ingest_date"], errors="coerce").dt.date
    agg = p.groupby(["team_key", "ingest_date"]).size().reset_index(name="n_injured_players")
    if not s.empty:
        s = s.copy()
        s["team_key"] = map_team(s["team"], "physioroom", alias, "pr_club_injury_summary.team")
        s["ingest_date"] = pd.to_datetime(s["ingest_date"], errors="coerce").dt.date
        agg = agg.merge(s[["team_key", "ingest_date", "total_injuries"]],
                         on=["team_key", "ingest_date"], how="outer")
    print(f"  · injuries: {agg['ingest_date'].nunique()} lần scrape, {agg['team_key'].nunique()} đội")
    return agg.sort_values(["team_key", "ingest_date"])


def build_injuries(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_team_injuries AS
    SELECT tm.match_id, tm.team_key, inj.ingest_date AS injury_asof_date,
           inj.n_injured_players, inj.total_injuries
    FROM tm
    ASOF LEFT JOIN inj
      ON tm.team_key = inj.team_key AND tm.match_date >= inj.ingest_date
    """)


# ---------------- p24: bắc cầu match_id (fd <-> api-football) + rolling stats (leakage-safe)
def load_af_fixtures(store, alias):
    af = store.read_prefix("silver/matches/af_fixtures/")
    cols = ["fixture_id", "home_key", "away_key", "match_date", "home_goals", "away_goals"]
    if af.empty:
        print("  ! không có af_fixtures -> không bắc cầu được"); return pd.DataFrame(columns=cols)
    af = af.sort_values("_key").drop_duplicates("fixture_id", keep="last").copy()
    af["home_key"] = map_team(af["home_team"], "apifootball", alias, "af_fixtures.home_team")
    af["away_key"] = map_team(af["away_team"], "apifootball", alias, "af_fixtures.away_team")
    dt = pd.to_datetime(af["date"], errors="coerce", utc=True)
    af["match_date"] = dt.dt.tz_localize(None).dt.date
    return af[cols]


def load_af_stats(store, alias):
    s = store.read_prefix("silver/matches/af_match_stats/")
    cols = ["fixture_id", "team_key", "possession_pct", "shots_on_goal", "total_shots", "corner_kicks", "fouls"]
    if s.empty:
        print("  ! không có af_match_stats -> feature_team_stats_af rỗng"); return pd.DataFrame(columns=cols)
    s = s.copy()
    s["team_key"] = map_team(s["team"], "apifootball", alias, "af_match_stats.team")
    poss = first_present(s, ["ball_possession", "possession"])
    out = pd.DataFrame({
        "fixture_id": s["fixture_id"].values, "team_key": s["team_key"].values,
        "possession_pct": num(s, poss).values, "shots_on_goal": num(s, "shots_on_goal").values,
        "total_shots": num(s, "total_shots").values, "corner_kicks": num(s, "corner_kicks").values,
        "fouls": num(s, "fouls").values})
    return out


def build_af_bridge(con):
    """Ánh xạ match_id (fd_matches) <-> fixture_id (api-football) qua (home_key, away_key, ngày lệch <=1).
    Đây là bảng nền cho feature_team_stats_af và cho các notebook khai thác af_match_events/af_lineups sau này."""
    con.execute("""
    CREATE OR REPLACE TABLE bridge_fd_af_match AS
    SELECT m.match_id, af.fixture_id, m.match_date AS fd_date, af.match_date AS af_date,
           (m.home_goals = af.home_goals AND m.away_goals = af.away_goals) AS score_match
    FROM m JOIN af_fx af
      ON af.home_key = m.home_key AND af.away_key = m.away_key
     AND abs(date_diff('day', af.match_date, m.match_date)) <= 1
    """)


def build_af_stats_rolling(con):
    """CHỐNG RÒ RỈ: af_match_stats là số liệu SAU trận -> chỉ dùng làm rolling l5 từ các trận
    TRƯỚC đó (giống hệt cách build_team_match/build_xg đang làm), không bao giờ dùng số liệu
    của chính trận đang dự đoán."""
    con.execute("""
    CREATE OR REPLACE TABLE afx AS
    SELECT b.match_id, b.fd_date AS match_date, s.team_key,
           s.possession_pct, s.shots_on_goal, s.total_shots, s.corner_kicks, s.fouls
    FROM af_stats_raw s JOIN bridge_fd_af_match b ON b.fixture_id = s.fixture_id
    """)
    con.execute("""
    CREATE OR REPLACE TABLE feature_team_stats_af AS
    SELECT match_id, team_key,
      AVG(possession_pct) OVER w5 AS poss_l5, AVG(shots_on_goal) OVER w5 AS sog_l5,
      AVG(total_shots) OVER w5 AS shots_total_l5, AVG(corner_kicks) OVER w5 AS corners_af_l5,
      AVG(fouls) OVER w5 AS fouls_l5
    FROM afx
    WINDOW w5 AS (PARTITION BY team_key ORDER BY match_date ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
    """)


# ---------------- p09: fdo_matches — CHỈ đối chiếu/QA với fd_matches, KHÔNG tạo feature
def load_fdo(store, alias):
    f = store.read_prefix("silver/matches/fdo_matches/")
    cols = ["match_id", "home_key", "away_key", "match_date", "home_goals", "away_goals", "status"]
    if f.empty:
        print("  ! không có fdo_matches -> bỏ qua đối chiếu p09"); return pd.DataFrame(columns=cols)
    f = f.copy()
    f["home_key"] = map_team(f["home_team"], "fdo", alias, "fdo_matches.home_team")
    f["away_key"] = map_team(f["away_team"], "fdo", alias, "fdo_matches.away_team")
    f["match_date"] = pd.to_datetime(f["utc_date"], errors="coerce").dt.date
    return f[cols]


def qa_fdo_crosscheck(con, store):
    n = con.execute("SELECT count(*) FROM fdo").fetchone()[0]
    if n == 0: return
    gap = con.execute("""
        SELECT fdo.match_id, fdo.home_key, fdo.away_key, fdo.match_date, fdo.home_goals, fdo.away_goals
        FROM fdo LEFT JOIN m ON m.home_key = fdo.home_key AND m.away_key = fdo.away_key
                             AND abs(date_diff('day', m.match_date, fdo.match_date)) <= 1
        WHERE m.match_id IS NULL AND fdo.status = 'FINISHED'
    """).df()
    mismatch = con.execute("""
        SELECT m.match_id, m.match_date, m.home_goals, m.away_goals,
               fdo.home_goals AS fdo_home_goals, fdo.away_goals AS fdo_away_goals
        FROM m JOIN fdo ON fdo.home_key = m.home_key AND fdo.away_key = m.away_key
                       AND abs(date_diff('day', m.match_date, fdo.match_date)) <= 1
        WHERE m.home_goals != fdo.home_goals OR m.away_goals != fdo.away_goals
    """).df()
    print(f"  · p09 QA: fdo_matches {n:,} trận. {len(gap)} trận fdo có mà fd_matches KHÔNG có "
          f"(ứng viên vá lỗ hổng); {len(mismatch)} trận tỉ số LỆCH giữa 2 nguồn.")
    report = pd.concat([gap.assign(issue="missing_in_fd"), mismatch.assign(issue="score_mismatch")],
                        ignore_index=True) if (len(gap) or len(mismatch)) else pd.DataFrame(
                        columns=["issue"])
    store.write_csv(f"{QA_OUT}/fdo_vs_fd_matches.csv", report)


# ---------------- SQL feature (nhóm gốc)
def win(n, order="match_date, match_id", by="team_key"):
    return f"(PARTITION BY {by} ORDER BY {order} ROWS BETWEEN {n} PRECEDING AND 1 PRECEDING)"


def build_team_match(con):
    con.execute("""
    CREATE OR REPLACE TABLE tm AS
    SELECT match_id, match_date, season, home_key AS team_key, away_key AS opp_key, 1 AS is_home,
           home_goals AS gf, away_goals AS ga,
           CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END AS pts,
           (home_goals > away_goals)::INT AS win, (away_goals = 0)::INT AS clean_sheet,
           h_shots AS shots, a_shots AS shots_against, h_sot AS sot, a_sot AS sot_against, h_corners AS corners,
           CASE WHEN h_yellow IS NULL AND h_red IS NULL THEN NULL ELSE COALESCE(h_yellow,0)+2*COALESCE(h_red,0) END AS cards
    FROM m
    UNION ALL
    SELECT match_id, match_date, season, away_key, home_key, 0, away_goals, home_goals,
           CASE WHEN away_goals > home_goals THEN 3 WHEN away_goals = home_goals THEN 1 ELSE 0 END,
           (away_goals > home_goals)::INT, (home_goals = 0)::INT,
           a_shots, h_shots, a_sot, h_sot, a_corners,
           CASE WHEN a_yellow IS NULL AND a_red IS NULL THEN NULL ELSE COALESCE(a_yellow,0)+2*COALESCE(a_red,0) END
    FROM m
    """)
    con.execute(f"""
    CREATE OR REPLACE TABLE feature_team_match AS
    SELECT match_id, match_date, season, team_key, opp_key, is_home,
      COUNT(*) OVER wall AS n_prior, COUNT(*) OVER wseason AS n_prior_season,
      LEAST(date_diff('day', LAG(match_date) OVER wt, match_date), 30) AS rest_days,
      AVG(pts) OVER w5 AS pts_l5, AVG(gf) OVER w5 AS gf_l5, AVG(ga) OVER w5 AS ga_l5,
      AVG(sot) OVER w5 AS sot_l5, AVG(sot_against) OVER w5 AS sot_against_l5,
      AVG(shots) OVER w5 AS shots_l5, AVG(corners) OVER w5 AS corners_l5,
      AVG(cards) OVER w5 AS cards_l5, AVG(win) OVER w5 AS win_l5, AVG(clean_sheet) OVER w5 AS cs_l5,
      AVG(pts) OVER w10 AS pts_l10, AVG(gf) OVER w10 AS gf_l10, AVG(ga) OVER w10 AS ga_l10,
      AVG(pts) OVER wv5 AS pts_l5_venue, AVG(gf) OVER wv5 AS gf_l5_venue, AVG(ga) OVER wv5 AS ga_l5_venue,
      AVG(pts) OVER wseason AS ppg_season, AVG(gf - ga) OVER wseason AS gd_pg_season
    FROM tm
    WINDOW
      wt AS (PARTITION BY team_key ORDER BY match_date, match_id),
      wall AS (PARTITION BY team_key ORDER BY match_date, match_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      wseason AS (PARTITION BY team_key, season ORDER BY match_date, match_id ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
      w5 AS {win(5)}, w10 AS {win(10)}, wv5 AS {win(5, by="team_key, is_home")}
    """)


def build_position(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_league_position AS
    WITH dts AS (SELECT DISTINCT season, match_date FROM tm),
         tms AS (SELECT DISTINCT season, team_key FROM tm),
    pos AS (
      SELECT d.season, d.match_date, t.team_key, COUNT(x.match_id) AS played_before,
             COALESCE(SUM(x.pts),0) AS points_before, COALESCE(SUM(x.gf-x.ga),0) AS gd_before,
             COALESCE(SUM(x.gf),0) AS gf_before
      FROM dts d JOIN tms t ON t.season = d.season
      LEFT JOIN tm x ON x.season = d.season AND x.team_key = t.team_key AND x.match_date < d.match_date
      GROUP BY d.season, d.match_date, t.team_key),
    ranked AS (SELECT *, RANK() OVER (PARTITION BY season, match_date
                 ORDER BY points_before DESC, gd_before DESC, gf_before DESC) AS rk FROM pos)
    SELECT tm.match_id, tm.team_key, r.played_before, r.points_before, r.gd_before,
           CASE WHEN r.played_before > 0 THEN r.rk END AS rank_before,
           CASE WHEN r.played_before > 0 THEN (r.rk <= 6)::INT END AS is_top6
    FROM tm JOIN ranked r ON r.season = tm.season AND r.match_date = tm.match_date AND r.team_key = tm.team_key
    """)


def compute_elo(df, k=20.0, hfa=65.0, keep=0.75, init=1500.0):
    ratings, last_season, out = {}, None, []
    for r in df.itertuples(index=False):
        if r.season != last_season:
            if last_season is not None:
                ratings = {t: keep * v + (1 - keep) * init for t, v in ratings.items()}
            last_season = r.season
        rh, ra = ratings.get(r.home_key, init), ratings.get(r.away_key, init)
        exp_h = 1.0 / (1.0 + 10 ** (-(rh + hfa - ra) / 400.0))
        out.append((r.match_id, rh, ra, rh - ra, exp_h))
        gd = r.home_goals - r.away_goals
        s = 1.0 if gd > 0 else 0.5 if gd == 0 else 0.0
        a = abs(gd); g = 1.0 if a <= 1 else 1.5 if a == 2 else (11 + a) / 8.0
        delta = k * g * (s - exp_h)
        ratings[r.home_key], ratings[r.away_key] = rh + delta, ra - delta
    return pd.DataFrame(out, columns=["match_id"] + ELO_COLS)


def build_elo(con):
    df = con.execute("SELECT match_id, season, home_key, away_key, home_goals, away_goals "
                     "FROM m ORDER BY match_date, match_id").df()
    con.register("elo_df", compute_elo(df))
    con.execute("CREATE OR REPLACE TABLE feature_elo AS SELECT * FROM elo_df")


def build_market(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_market AS
    WITH o AS (SELECT match_id, bookmaker, 1/odds_home AS ih, 1/odds_draw AS id_, 1/odds_away AS ia
               FROM odds WHERE odds_home > 1 AND odds_draw > 1 AND odds_away > 1),
    n AS (SELECT match_id, bookmaker, ih/(ih+id_+ia) AS p_h, id_/(ih+id_+ia) AS p_d,
                 ia/(ih+id_+ia) AS p_a, ih+id_+ia-1 AS margin FROM o)
    SELECT m.match_id,
      AVG(p_h) FILTER (WHERE bookmaker NOT IN ('Avg','Max')) AS mkt_p_home,
      AVG(p_d) FILTER (WHERE bookmaker NOT IN ('Avg','Max')) AS mkt_p_draw,
      AVG(p_a) FILTER (WHERE bookmaker NOT IN ('Avg','Max')) AS mkt_p_away,
      AVG(margin) FILTER (WHERE bookmaker NOT IN ('Avg','Max')) AS mkt_margin,
      CAST(COUNT(*) FILTER (WHERE bookmaker NOT IN ('Avg','Max')) AS DOUBLE) AS mkt_n_books,
      MAX(p_h) FILTER (WHERE bookmaker = 'PS') AS pin_p_home,
      MAX(p_d) FILTER (WHERE bookmaker = 'PS') AS pin_p_draw,
      MAX(p_a) FILTER (WHERE bookmaker = 'PS') AS pin_p_away
    FROM n JOIN m USING (match_id) GROUP BY m.match_id
    """)


def build_xg(con):
    wu = lambda n: win(n, order="match_date")
    con.execute(f"""
    CREATE OR REPLACE TABLE feature_team_xg AS
    WITH r AS (
      SELECT team_key, match_date,
        AVG(xg) OVER w5 AS xg_l5, AVG(xga) OVER w5 AS xga_l5, AVG(npxg) OVER w5 AS npxg_l5,
        AVG(xg - xga) OVER w5 AS xgd_l5, AVG(ppda) OVER w5 AS ppda_l5,
        AVG(xg) OVER w10 AS xg_l10, AVG(xga) OVER w10 AS xga_l10,
        AVG(scored - xg) OVER w10 AS xg_overperf_l10
      FROM ux WINDOW w5 AS {wu(5)}, w10 AS {wu(10)})
    SELECT tm.match_id, tm.team_key, {", ".join("r." + c for c in XG_FEATS)}
    FROM tm JOIN r ON r.team_key = tm.team_key AND r.match_date = tm.match_date
    """)


def build_attention(con):
    con.execute("""
    CREATE OR REPLACE TABLE feature_team_attention AS
    WITH a AS (
      SELECT tm.match_id, tm.team_key,
        COUNT(pv.views) FILTER (WHERE pv.date >= tm.match_date - 7) AS n7,
        AVG(pv.views) FILTER (WHERE pv.date >= tm.match_date - 7) AS avg7,
        COUNT(pv.views) AS n28, AVG(pv.views) AS avg28,
        MAX(pv.views) FILTER (WHERE pv.date = tm.match_date - 1) AS last1
      FROM tm LEFT JOIN pv ON pv.team_key = tm.team_key
                          AND pv.date < tm.match_date AND pv.date >= tm.match_date - 28
      GROUP BY tm.match_id, tm.team_key)
    SELECT match_id, team_key,
      CASE WHEN n7 >= 5 THEN avg7 END AS pv_7d, CASE WHEN n28 >= 20 THEN avg28 END AS pv_28d,
      last1 AS pv_last1,
      CASE WHEN n7 >= 5 AND n28 >= 20 AND avg28 > 0 THEN avg7 / avg28 END AS pv_ratio_7_28
    FROM a
    """)


def build_context(con, rain_mm):
    con.execute(f"""
    CREATE OR REPLACE TABLE feature_match_context AS
    SELECT m.match_id, CAST(m.kickoff_hour AS INTEGER) AS kickoff_hour,
           dayofweek(m.match_date) AS dow, month(m.match_date) AS month,
           (dayofweek(m.match_date) IN (0, 6))::INT AS is_weekend,
           w.temp_c, w.precip_mm, w.wind_kmh,
           CASE WHEN w.precip_mm IS NULL THEN NULL ELSE (w.precip_mm >= {rain_mm})::INT END AS is_raining
    FROM m LEFT JOIN wx w USING (match_id)
    """)


def build_match_ml(con):
    sel = lambda al, f, p: f"{al}.{f} AS {p}_{f}"
    form = [sel("h", f, "home") for f in ROLL_FEATS] + [sel("a", f, "away") for f in ROLL_FEATS]
    form += [f"h.{f} - a.{f} AS diff_{f}" for f in FORM_DIFF]
    pos = [sel("hp", f, "home") for f in POS_FEATS] + [sel("ap", f, "away") for f in POS_FEATS]
    pos += ["ap.rank_before - hp.rank_before AS rank_diff", "hp.points_before - ap.points_before AS points_diff"]
    xg = [sel("hx", f, "home") for f in XG_FEATS] + [sel("ax", f, "away") for f in XG_FEATS]
    xg += [f"hx.{f} - ax.{f} AS diff_{f}" for f in XG_DIFF]
    att = [sel("hv", f, "home") for f in ATT_FEATS] + [sel("av", f, "away") for f in ATT_FEATS]
    att += ["hv.pv_ratio_7_28 - av.pv_ratio_7_28 AS diff_pv_ratio_7_28"]
    news = [sel("hn", f, "home") for f in NEWS_FEATS] + [sel("an", f, "away") for f in NEWS_FEATS]
    news += [f"hn.{f} - an.{f} AS diff_{f}" for f in NEWS_DIFF]
    inj = [sel("hi", f, "home") for f in INJ_FEATS] + [sel("ai", f, "away") for f in INJ_FEATS]
    inj += [f"hi.{f} - ai.{f} AS diff_{f}" for f in INJ_DIFF]
    afst = [sel("haf", f, "home") for f in AF_FEATS] + [sel("aaf", f, "away") for f in AF_FEATS]
    afst += [f"haf.{f} - aaf.{f} AS diff_{f}" for f in AF_DIFF]
    con.execute(f"""
    CREATE OR REPLACE TABLE feature_match_ml AS
    SELECT m.match_id, m.division, m.season, m.match_date, m.home_key, m.away_key, m.home_team, m.away_team,
      CASE WHEN dense_rank() OVER (ORDER BY m.season DESC) = 1 THEN 'test'
           WHEN dense_rank() OVER (ORDER BY m.season DESC) = 2 THEN 'valid' ELSE 'train' END AS split,
      (h.n_prior >= {WARM_MIN_MATCHES} AND a.n_prior >= {WARM_MIN_MATCHES})::INT AS is_warm,
      {", ".join(form)}, {", ".join(pos)},
      e.elo_home, e.elo_away, e.elo_diff, e.elo_exp_home,
      {", ".join("mk." + c for c in MKT_COLS)},
      {", ".join(xg)}, {", ".join(att)},
      {", ".join("ah." + c for c in AH_COLS)},
      {", ".join(news)}, {", ".join(inj)}, {", ".join(afst)},
      {", ".join("cx." + c for c in CTX_COLS)},
      m.result AS target, m.home_goals AS target_home_goals, m.away_goals AS target_away_goals,
      m.home_goals + m.away_goals AS target_total_goals,
      (m.home_goals + m.away_goals > 2)::INT AS target_over25,
      (m.home_goals > 0 AND m.away_goals > 0)::INT AS target_btts
    FROM m
    JOIN feature_team_match h ON h.match_id = m.match_id AND h.is_home = 1
    JOIN feature_team_match a ON a.match_id = m.match_id AND a.is_home = 0
    JOIN feature_league_position hp ON hp.match_id = m.match_id AND hp.team_key = m.home_key
    JOIN feature_league_position ap ON ap.match_id = m.match_id AND ap.team_key = m.away_key
    JOIN feature_elo e ON e.match_id = m.match_id
    JOIN feature_match_context cx ON cx.match_id = m.match_id
    LEFT JOIN feature_market mk ON mk.match_id = m.match_id
    LEFT JOIN feature_team_xg hx ON hx.match_id = m.match_id AND hx.team_key = m.home_key
    LEFT JOIN feature_team_xg ax ON ax.match_id = m.match_id AND ax.team_key = m.away_key
    LEFT JOIN feature_team_attention hv ON hv.match_id = m.match_id AND hv.team_key = m.home_key
    LEFT JOIN feature_team_attention av ON av.match_id = m.match_id AND av.team_key = m.away_key
    LEFT JOIN feature_market_ah ah ON ah.match_id = m.match_id
    LEFT JOIN feature_team_news_buzz hn ON hn.match_id = m.match_id AND hn.team_key = m.home_key
    LEFT JOIN feature_team_news_buzz an ON an.match_id = m.match_id AND an.team_key = m.away_key
    LEFT JOIN feature_team_injuries hi ON hi.match_id = m.match_id AND hi.team_key = m.home_key
    LEFT JOIN feature_team_injuries ai ON ai.match_id = m.match_id AND ai.team_key = m.away_key
    LEFT JOIN feature_team_stats_af haf ON haf.match_id = m.match_id AND haf.team_key = m.home_key
    LEFT JOIN feature_team_stats_af aaf ON aaf.match_id = m.match_id AND aaf.team_key = m.away_key
    ORDER BY m.match_date, m.match_id
    """)


def catalog(con):
    cols = [r[0] for r in con.execute("DESCRIBE feature_match_ml").fetchall()]
    grp = {}
    for f in KEY_COLS: grp[f] = "key"
    for f in TARGET_COLS: grp[f] = "target"
    for f in ELO_COLS: grp[f] = "elo"
    for f in MKT_COLS: grp[f] = "market"
    for f in AH_COLS: grp[f] = "market_ah"
    for f in CTX_COLS: grp[f] = "context"
    for p in ("home", "away"):
        for f in ROLL_FEATS: grp[f"{p}_{f}"] = "form"
        for f in POS_FEATS: grp[f"{p}_{f}"] = "position"
        for f in XG_FEATS: grp[f"{p}_{f}"] = "xg"
        for f in ATT_FEATS: grp[f"{p}_{f}"] = "attention"
        for f in NEWS_FEATS: grp[f"{p}_{f}"] = "news_buzz"
        for f in INJ_FEATS: grp[f"{p}_{f}"] = "injuries"
        for f in AF_FEATS: grp[f"{p}_{f}"] = "stats_af"
    for f in FORM_DIFF: grp[f"diff_{f}"] = "form"
    for f in XG_DIFF: grp[f"diff_{f}"] = "xg"
    for f in NEWS_DIFF: grp[f"diff_{f}"] = "news_buzz"
    for f in INJ_DIFF: grp[f"diff_{f}"] = "injuries"
    for f in AF_DIFF: grp[f"diff_{f}"] = "stats_af"
    grp.update({"rank_diff": "position", "points_diff": "position", "diff_pv_ratio_7_28": "attention"})
    return pd.DataFrame({"column_name": cols, "feature_group": [grp.get(c, "other") for c in cols]})


def qa(con, xg_seasons):
    print("\n[QA]")
    n_m = con.execute("SELECT count(*) FROM m").fetchone()[0]
    for t in ("feature_elo", "feature_market", "feature_match_context", "feature_match_ml"):
        n, d = con.execute(f"SELECT count(*), count(DISTINCT match_id) FROM {t}").fetchone()
        assert n == d, f"{t}: match_id trùng"
        if t != "feature_market": assert n == n_m, f"{t} có {n} dòng, spine có {n_m}"
    for t in ("feature_team_match", "feature_league_position"):
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        assert n == 2 * n_m, f"{t}: {n} dòng, kỳ vọng {2*n_m}"
    leak = con.execute("""SELECT count(*) FROM feature_team_match WHERE n_prior = 0
        AND (pts_l5 IS NOT NULL OR gf_l5 IS NOT NULL OR ppg_season IS NOT NULL)""").fetchone()[0]
    assert leak == 0, "RÒ RỈ: trận đầu của đội vẫn có feature rolling"
    print(f"  ✓ {n_m:,} trận; khóa không trùng; không rò rỉ ở trận đầu của mỗi đội")
    print(con.execute("""SELECT split, season, count(*) AS n, sum(is_warm) AS warm,
        round(avg((target='H')::INT),3) AS p_H, round(avg((target='D')::INT),3) AS p_D,
        round(avg((target='A')::INT),3) AS p_A, round(avg(target_total_goals),2) AS goals
        FROM feature_match_ml GROUP BY ALL ORDER BY season""").df().to_string(index=False))
    cov = con.execute("""SELECT
        round(avg((home_pts_l5 IS NOT NULL)::INT),2) AS form, round(avg((home_rank_before IS NOT NULL)::INT),2) AS position,
        round(avg((elo_home IS NOT NULL)::INT),2) AS elo, round(avg((mkt_p_home IS NOT NULL)::INT),2) AS market,
        round(avg((home_xg_l5 IS NOT NULL)::INT),2) AS xg, round(avg((home_pv_7d IS NOT NULL)::INT),2) AS attention,
        round(avg((temp_c IS NOT NULL)::INT),2) AS weather, round(avg((home_sot_l5 IS NOT NULL)::INT),2) AS shots,
        round(avg((ah_n_books IS NOT NULL)::INT),2) AS market_ah,
        round(avg((home_news_n28 IS NOT NULL)::INT),2) AS news_buzz,
        round(avg((home_n_injured_players IS NOT NULL)::INT),2) AS injuries,
        round(avg((home_poss_l5 IS NOT NULL)::INT),2) AS stats_af
        FROM feature_match_ml WHERE is_warm = 1""").df()
    print("\n  Độ phủ feature (trận is_warm=1):"); print(cov.to_string(index=False))
    r = cov.iloc[0]
    hints = {"xg": f"Understat chỉ có {xg_seasons} — cần khớp mùa với fd_matches",
             "attention": "wm_pageviews chỉ từ 2023-01-01 hoặc label không khớp team_key",
             "weather": "fd_matches_weather chỉ có vài sân (p13 đang head(2))",
             "market": "fd_odds thiếu hoặc match_id không khớp",
             "shots": "fd_matches không có HS/HST",
             "market_ah": "p22 (Odds API) chỉ có kèo trận SẮP đấu, không backfill lịch sử — bình thường nếu thấp",
             "news_buzz": "p20 google_news_articles ít bài hoặc chưa nhắc tới đội này",
             "injuries": "p16 mới scrape ít lần (ingest_date) -> đa số trận cũ chưa có snapshot",
             "stats_af": "p24 chưa cào đủ af_match_stats hoặc bridge_fd_af_match khớp ít trận"}
    for k, why in hints.items():
        if pd.isna(r[k]) or r[k] < 0.5: print(f"  ⚠ nhóm '{k}' phủ {r[k]}: {why}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None); ap.add_argument("--division", default="E0")
    ap.add_argument("--seed", default=None)
    ap.add_argument("--rain-mm", type=float, default=0.1)
    import sys
    if 'ipykernel' in sys.modules or 'google.colab' in sys.modules:
        args = ap.parse_args([])
    else:
        args = ap.parse_args()
    a = args
    store = LocalStore(a.root) if a.root else MinioStore()
    print("[1/11] đọc silver (nhóm gốc: matches/odds/xg/pageviews/weather)")
    alias = load_alias(Path(a.seed) if a.seed else find_seed())
    m = load_matches(store, alias, a.division)
    odds, ux, pv, wx = load_odds(store), load_xg(store, alias), load_pageviews(store, alias), load_weather(store)
    xg_seasons = "?" if ux.empty else f"{pd.to_datetime(ux.match_date).min():%Y-%m}..{pd.to_datetime(ux.match_date).max():%Y-%m}"

    print("[2/11] đọc silver (nhóm mới: p22 odds handicap, p20 news, p16 injuries, p24 af, p09 fdo)")
    ah_spreads, ah_totals = load_odds_handicap(store, alias)
    dim_wiki = load_wiki_dim(store)
    nb_daily = load_news_buzz(store, alias)
    inj = load_injuries(store, alias)
    af_fx = load_af_fixtures(store, alias)
    af_stats_raw = load_af_stats(store, alias)
    fdo = load_fdo(store, alias)

    con = duckdb.connect()
    for name, df in (("m", m), ("odds", odds), ("ux", ux), ("pv", pv), ("wx", wx),
                      ("ah_spreads", ah_spreads), ("ah_totals", ah_totals), ("nb", nb_daily),
                      ("inj", inj), ("af_fx", af_fx), ("af_stats_raw", af_stats_raw), ("fdo", fdo)):
        con.register(name, df)

    for title, fn in [
        ("[3/11] form / rolling", lambda: build_team_match(con)),
        ("[4/11] vị trí bảng trước trận", lambda: build_position(con)),
        ("[5/11] Elo", lambda: build_elo(con)),
        ("[6/11] market (odds 1x2 + handicap/totals)", lambda: (build_market(con), build_market_ah(con))),
        ("[7/11] xG", lambda: build_xg(con)),
        ("[8/11] attention + context + news buzz + injuries", lambda: (
            build_attention(con), build_context(con, a.rain_mm), build_news_buzz(con), build_injuries(con))),
        ("[9/11] bắc cầu p24 (match_id <-> fixture_id) + rolling stats", lambda: (
            build_af_bridge(con), build_af_stats_rolling(con))),
        ("[10/11] đối chiếu p09 vs p03 (QA, không tạo feature)", lambda: qa_fdo_crosscheck(con, store)),
        ("[11/11] feature_match_ml", lambda: build_match_ml(con)),
    ]:
        print(title); fn()

    qa(con, xg_seasons)
    print("\n[ghi gold/features]")
    for t in ("feature_team_match", "feature_league_position", "feature_elo", "feature_market",
              "feature_team_xg", "feature_team_attention", "feature_match_context",
              "feature_market_ah", "feature_team_news_buzz", "feature_team_injuries",
              "feature_team_stats_af", "bridge_fd_af_match", "feature_match_ml"):
        store.write_parquet(f"{OUT}/{t}.parquet", con.execute(f"SELECT * FROM {t}").df())
    store.write_csv(f"{OUT}/_feature_catalog.csv", catalog(con))
    if not dim_wiki.empty:
        store.write_parquet(f"{DIM_OUT}/dim_team_wiki_profile.parquet", dim_wiki)


if __name__ == "__main__":
    main()