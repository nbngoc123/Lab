#!/usr/bin/env python3
"""
lake_utils.py — Tiện ích dùng chung cho toàn bộ pipeline Silver → Gold.

Import bất cứ đâu:
    from lake_utils import LocalStore, MinioStore, load_matches, load_odds, ...

Phân nhóm:
    1. Constants (tên cột, tham số cố định)
    2. Store classes (LocalStore, MinioStore)
    3. Alias / team_key helpers (find_seed, load_alias, map_team)
    4. Column helpers (first_present, num)
    5. Silver loaders — mỗi hàm đọc 1 nguồn Silver về DataFrame chuẩn hoá
    6. DuckDB bridge helpers (build_af_bridge)
"""
import io
import os
import sys
from pathlib import Path

import pandas as pd

# ═══════════════════════════════════════════════════════════════════════
# 1. CONSTANTS
# ═══════════════════════════════════════════════════════════════════════

SRC_TAG   = "gold-features"
OUT_FEAT  = "gold/features"
OUT_OBT   = "gold/obt"
OUT_DIM   = "gold/dim"
OUT_QA    = "gold/qa"

ROLL_FEATS = [
    "n_prior", "n_prior_season", "rest_days",
    "pts_l5", "gf_l5", "ga_l5", "sot_l5", "sot_against_l5", "shots_l5",
    "corners_l5", "cards_l5", "win_l5", "cs_l5",
    "pts_l10", "gf_l10", "ga_l10",
    "pts_l5_venue", "gf_l5_venue", "ga_l5_venue", "ppg_season", "gd_pg_season",
]
FORM_DIFF   = ["pts_l5", "gf_l5", "ga_l5", "sot_l5", "pts_l10", "ppg_season", "gd_pg_season", "pts_l5_venue"]
POS_FEATS   = ["played_before", "points_before", "gd_before", "rank_before", "is_top6"]
XG_FEATS    = ["xg_l5", "xga_l5", "npxg_l5", "xgd_l5", "ppda_l5", "xg_l10", "xga_l10", "xg_overperf_l10"]
XG_DIFF     = ["xg_l5", "xga_l5", "xgd_l5"]
ATT_FEATS   = ["pv_7d", "pv_28d", "pv_last1", "pv_ratio_7_28"]
MKT_COLS    = ["mkt_p_home", "mkt_p_draw", "mkt_p_away", "mkt_margin", "mkt_n_books",
               "pin_p_home", "pin_p_draw", "pin_p_away"]
ELO_COLS    = ["elo_home", "elo_away", "elo_diff", "elo_exp_home"]
CTX_COLS    = ["kickoff_hour", "dow", "month", "is_weekend",
               "temp_c", "precip_mm", "wind_kmh", "is_raining"]
AH_COLS     = ["ah_home_price_avg", "ah_away_price_avg", "ah_home_point_avg", "ah_n_books",
               "ou_over_price_avg", "ou_under_price_avg", "ou_line_avg", "ou_n_books"]
NEWS_FEATS  = ["news_n7", "news_n28"]
NEWS_DIFF   = ["news_n28"]
INJ_FEATS   = ["n_injured_players", "total_injuries"]
INJ_DIFF    = ["n_injured_players"]
AF_FEATS    = ["poss_l5", "sog_l5", "shots_total_l5", "corners_af_l5", "fouls_l5"]
AF_DIFF     = ["poss_l5", "sog_l5"]
TARGET_COLS = ["target", "target_home_goals", "target_away_goals",
               "target_total_goals", "target_over25", "target_btts"]
KEY_COLS    = ["match_id", "division", "season", "match_date",
               "home_key", "away_key", "home_team", "away_team", "split", "is_warm"]
WARM_MIN_MATCHES = 5


# ═══════════════════════════════════════════════════════════════════════
# 2. STORE CLASSES
# ═══════════════════════════════════════════════════════════════════════

class LocalStore:
    """Doc/ghi Parquet tu thu muc local (dung khi thu nghiem offline)."""

    def __init__(self, root):
        self.root = Path(root)

    def read_prefix(self, prefix):
        dfs = []
        for p in sorted((self.root / prefix).rglob("*.parquet")):
            d = pd.read_parquet(p)
            d["_key"] = p.relative_to(self.root).as_posix()
            dfs.append(d)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def read_file(self, key):
        p = self.root / key
        return pd.read_parquet(p) if p.exists() else pd.DataFrame()

    def write_parquet(self, key, df):
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False, compression="zstd")
        print(f"  v {key}  ({len(df):,} dong)")

    def write_csv(self, key, df):
        p = self.root / key
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(p, index=False)
        print(f"  v {key}  ({len(df):,} dong)")


class MinioStore:
    """Doc/ghi Parquet tu MinIO (S3-compatible) -- dung trong production."""

    def __init__(self):
        try:
            from lake import minio_io as mio
        except ImportError:
            base = Path(__file__).resolve() if "__file__" in globals() else Path.cwd()
            for cand in (base.parents[2] if len(base.parents) >= 2 else base,
                         Path("/opt/project"), base):
                sys.path.insert(0, str(cand))
            from lake import minio_io as mio
        self.mio = mio

    def read_prefix(self, prefix):
        dfs = []
        for key, _ in self.mio.list_keys(prefix):
            if key.endswith(".parquet"):
                d = pd.read_parquet(io.BytesIO(self.mio.read_bytes(key)))
                d["_key"] = key
                dfs.append(d)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def read_file(self, key):
        try:
            return pd.read_parquet(io.BytesIO(self.mio.read_bytes(key)))
        except Exception:
            return pd.DataFrame()

    def write_parquet(self, key, df):
        self.mio.put_parquet(key, df, SRC_TAG)
        print(f"  v {key}  ({len(df):,} dong)")

    def write_csv(self, key, df):
        self.mio.put_bytes(key, df.to_csv(index=False).encode("utf-8"),
                           SRC_TAG, content_type="text/csv")
        print(f"  v {key}  ({len(df):,} dong)")


# ═══════════════════════════════════════════════════════════════════════
# 3. ALIAS / TEAM_KEY HELPERS
# ═══════════════════════════════════════════════════════════════════════

def find_seed():
    """Tim file seed/team_alias.csv: uu tien bien moi truong SEED_PATH."""
    if os.environ.get("SEED_PATH"):
        return Path(os.environ["SEED_PATH"])
    here = Path(__file__).resolve() if "__file__" in globals() else Path.cwd()
    for parent in [here, *here.parents]:
        cand = parent / "seed" / "team_alias.csv"
        if cand.exists():
            return cand
    sys.exit("Khong tim thay seed/team_alias.csv (dat SEED_PATH hoac --seed)")


def load_alias(seed):
    """Doc team_alias.csv (+ team_alias_extra.csv neu co) -> (by_src_dict, any_src_dict)."""
    frames = []
    for p in (seed, seed.with_name("team_alias_extra.csv")):
        if p.exists():
            frames.append(pd.read_csv(p, comment="#", skipinitialspace=True))
            print(f"  . alias: {p.name} ({len(frames[-1])} dong)")
    df = pd.concat(frames, ignore_index=True)
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["source", "alias", "team_key"])
    for c in ("source", "alias", "team_key"):
        df[c] = df[c].astype(str).str.strip()
    df["a"] = df["alias"].str.lower()
    by_src = {(r.source, r.a): r.team_key
              for r in df.drop_duplicates(["source", "a"]).itertuples()}
    any_src = {}
    for r in df.itertuples():
        any_src.setdefault(r.a, r.team_key)
    return by_src, any_src


def map_team(names, source, alias, what):
    """Map Series ten doi -> team_key theo (source, alias). Log ten chua map duoc."""
    by_src, any_src = alias

    def f(n):
        if not isinstance(n, str):
            return None
        k = n.strip().lower()
        return by_src.get((source, k)) or any_src.get(k)

    keys = names.map(f)
    miss = sorted(set(names[keys.isna()].dropna()))
    if miss:
        print(f"  ! {what}: {len(miss)} ten chua co alias (source={source}): {miss}")
        print(f"    Them dong  {source},<ten>,<team_key>  vao seed/team_alias.csv")
    return keys.fillna(names)


# ═══════════════════════════════════════════════════════════════════════
# 4. COLUMN HELPERS
# ═══════════════════════════════════════════════════════════════════════

def first_present(df, cands):
    """Tra ve ten cot dau tien trong cands ton tai trong df, hoac None."""
    for c in cands:
        if c in df.columns:
            return c
    return None


def num(df, col):
    """Ep cot thanh float64, coerce loi -> NaN. An toan khi col=None."""
    if col and col in df.columns:
        return pd.to_numeric(df[col], errors="coerce").astype("float64")
    return pd.Series(float("nan"), index=df.index, dtype="float64")


# ═══════════════════════════════════════════════════════════════════════
# 5. SILVER LOADERS
# ═══════════════════════════════════════════════════════════════════════

def load_matches(store, alias, division="E0"):
    """silver/matches/fd_matches -> DataFrame chuan hoa (1 dong/tran da da)."""
    raw = store.read_prefix("silver/matches/fd_matches/")
    if raw.empty:
        sys.exit("silver/matches/fd_matches/ trong -- chay p03 truoc")
    print(f"  . fd_matches: {len(raw):,} dong")
    need = {
        "match_id":   ["match_id"],
        "home_team":  ["home_team"],
        "away_team":  ["away_team"],
        "home_goals": ["home_goals"],
        "away_goals": ["away_goals"],
        "date":       ["match_date", "date", "utc_date"],
        "season":     ["season"],
    }
    col = {}
    for k, cands in need.items():
        col[k] = first_present(raw, cands)
        if not col[k]:
            sys.exit(f"fd_matches thieu cot {cands}. Hien co: {list(raw.columns)}")

    raw = raw.sort_values("_key").drop_duplicates(col["match_id"], keep="last")
    dcol = first_present(raw, ["division", "Div"])
    if dcol:
        raw = raw[raw[dcol] == division]
    else:
        sel = raw[raw[col["match_id"]].astype(str).str.startswith(division + "_")]
        if not sel.empty:
            raw = sel

    dt   = pd.to_datetime(raw[col["date"]], errors="coerce")
    hour = pd.Series(float("nan"), index=raw.index)
    if "Time" in raw.columns:
        hour = pd.to_numeric(raw["Time"].astype(str).str.split(":").str[0], errors="coerce")
    elif dt.dt.hour.nunique() > 1:
        hour = dt.dt.hour.astype(float)

    def _keys(name_col, key_col):
        if key_col in raw.columns and raw[key_col].notna().all():
            return raw[key_col]
        return map_team(raw[name_col], "fd", alias, f"fd_matches.{name_col}")

    m = pd.DataFrame({
        "match_id":    raw[col["match_id"]].values,
        "division":    division,
        "season":      raw[col["season"]].astype(str).values,
        "match_date":  dt.dt.normalize().values,
        "kickoff_hour":hour.values,
        "home_team":   raw[col["home_team"]].values,
        "away_team":   raw[col["away_team"]].values,
        "home_key":    _keys(col["home_team"], "home_team_key").values,
        "away_key":    _keys(col["away_team"], "away_team_key").values,
        "home_goals":  num(raw, col["home_goals"]).values,
        "away_goals":  num(raw, col["away_goals"]).values,
    })
    for out_c, in_c in {
        "h_shots": "HS", "a_shots": "AS", "h_sot": "HST", "a_sot": "AST",
        "h_corners": "HC", "a_corners": "AC",
        "h_yellow": "HY", "a_yellow": "AY",
        "h_red": "HR",    "a_red": "AR",
    }.items():
        m[out_c] = num(raw, in_c).values

    m = m.dropna(subset=["match_date", "home_goals", "away_goals"]).copy()
    m["match_date"]  = pd.to_datetime(m["match_date"]).dt.date
    m["home_goals"]  = m["home_goals"].astype(int)
    m["away_goals"]  = m["away_goals"].astype(int)
    m["result"]      = "D"
    m.loc[m.home_goals > m.away_goals, "result"] = "H"
    m.loc[m.home_goals < m.away_goals, "result"] = "A"
    if m["match_id"].duplicated().any():
        sys.exit(f"match_id trung {int(m['match_id'].duplicated().sum())} dong")
    m = m.sort_values(["match_date", "match_id"]).reset_index(drop=True)
    print(f"  . {len(m):,} tran ({division}), {m.match_date.min()} -> {m.match_date.max()}, "
          f"mua: {sorted(m.season.unique())}")
    return m


def compute_split(m, test_frac=0.15, valid_frac=0.15, min_seasons=3):
    """Chia train/valid/test theo mua (uu tien) hoac % thoi gian (fallback)."""
    seasons_sorted = sorted(m["season"].unique())
    split = pd.Series("train", index=m.index)
    if len(seasons_sorted) >= min_seasons:
        test_s, valid_s = seasons_sorted[-1], seasons_sorted[-2]
        split[m["season"] == valid_s] = "valid"
        split[m["season"] == test_s]  = "test"
        print(f"  . split theo MUA: train={seasons_sorted[:-2]}, valid={valid_s}, test={test_s}")
    else:
        n = len(m)
        n_test  = max(1, round(n * test_frac))
        n_valid = max(1, round(n * valid_frac))
        split.iloc[-n_test:] = "test"
        split.iloc[-(n_test + n_valid):-n_test] = "valid"
        print(f"  ! chi {len(seasons_sorted)} mua -> fallback chia % THOI GIAN")
    return split


def load_odds(store):
    """silver/odds/fd_odds -> (match_id, bookmaker, odds_home, odds_draw, odds_away)."""
    o = store.read_prefix("silver/odds/fd_odds/")
    if o.empty:
        print("  ! khong co fd_odds -> feature_market rong")
        return pd.DataFrame(columns=["match_id", "bookmaker", "odds_home", "odds_draw", "odds_away"])
    o = o.sort_values("_key").drop_duplicates(["match_id", "bookmaker"], keep="last")
    for c in ("odds_home", "odds_draw", "odds_away"):
        o[c] = pd.to_numeric(o[c], errors="coerce")
    return o[["match_id", "bookmaker", "odds_home", "odds_draw", "odds_away"]]


def load_xg(store, alias):
    """silver/teams/understat_team_xg -> (team_key, match_date, xg, xga, npxg, ppda, scored)."""
    u = store.read_prefix("silver/teams/understat_team_xg/")
    cols = ["team_key", "match_date", "xg", "xga", "npxg", "ppda", "scored"]
    if u.empty:
        print("  ! khong co understat_team_xg -> rong")
        return pd.DataFrame(columns=cols)
    if "league" in u.columns:
        u = u[u["league"] == "EPL"]
    u = u.sort_values("_key")
    out = pd.DataFrame({
        "team_key":   map_team(u["team_name"], "understat", alias, "understat_team_xg").values,
        "match_date": pd.to_datetime(u["date"], errors="coerce").dt.date.values,
        "xg":   num(u, "xG").values,    "xga":  num(u, "xGA").values,
        "npxg": num(u, "npxG").values,  "ppda": num(u, "ppda").values,
        "scored": num(u, "scored").values,
    })
    out = out.dropna(subset=["match_date"]).drop_duplicates(["team_key", "match_date"], keep="last")
    print(f"  . understat_team_xg: {len(out):,} dong, {out.match_date.min()} -> {out.match_date.max()}")
    return out


def load_pageviews(store, alias):
    """silver/text/wm_pageviews/entity=team -> (team_key, date, views)."""
    p = store.read_prefix("silver/text/wm_pageviews/entity=team/")
    if p.empty:
        print("  ! khong co wm_pageviews(team) -> rong")
        return pd.DataFrame(columns=["team_key", "date", "views"])
    lab = "label" if "label" in p.columns else "article"
    out = pd.DataFrame({
        "team_key": map_team(p[lab], "wikidata", alias, "wm_pageviews.label").values,
        "date":     pd.to_datetime(p["date"], errors="coerce").dt.date.values,
        "views":    num(p, "views").values,
    })
    out = out.dropna(subset=["date", "views"]).drop_duplicates(["team_key", "date"], keep="last")
    print(f"  . wm_pageviews(team): {len(out):,} dong, {out.date.min()} -> {out.date.max()}")
    return out


def load_weather(store):
    """silver/matches/fd_matches_weather -> (match_id, temp_c, precip_mm, wind_kmh)."""
    w = store.read_prefix("silver/matches/fd_matches_weather/")
    out_cols = ["match_id", "temp_c", "precip_mm", "wind_kmh"]
    if w.empty:
        print("  ! khong co fd_matches_weather -> thoi tiet NULL")
        return pd.DataFrame(columns=out_cols)
    w = w.sort_values("_key").drop_duplicates("match_id", keep="last")
    t = first_present(w, ["temperature_c", "temp_max", "temp"])
    r = first_present(w, ["precipitation_mm", "precipitation_sum"])
    d = first_present(w, ["windspeed_kmh", "wind_speed_max"])
    return pd.DataFrame({
        "match_id":  w["match_id"].values,
        "temp_c":    num(w, t).values,
        "precip_mm": num(w, r).values,
        "wind_kmh":  num(w, d).values,
    })


def load_odds_handicap(store, alias):
    """
    silver/betting/odds_spreads + odds_totals (p22).
    Join key: (home_key, away_key, match_date).
    Returns: (ah_spreads_df, ah_totals_df).
    """
    def _empty(cols):
        return pd.DataFrame(columns=cols)

    sp = store.read_prefix("silver/betting/odds_spreads/")
    if sp.empty:
        print("  ! khong co odds_spreads -> ah_* se NULL")
        sp = _empty(["home_key", "away_key", "match_date", "side_key", "price", "point", "bookmaker"])
    else:
        sp = sp.copy()
        sp["home_key"]   = map_team(sp["home_team"], "odds", alias, "odds_spreads.home_team")
        sp["away_key"]   = map_team(sp["away_team"], "odds", alias, "odds_spreads.away_team")
        sp["side_key"]   = map_team(sp["team"],      "odds", alias, "odds_spreads.team")
        sp["match_date"] = pd.to_datetime(sp["commence_time"], errors="coerce").dt.date

    tt = store.read_prefix("silver/betting/odds_totals/")
    if tt.empty:
        print("  ! khong co odds_totals -> ou_* se NULL")
        tt = _empty(["home_key", "away_key", "match_date", "name", "price", "point", "bookmaker"])
    else:
        tt = tt.copy()
        tt["home_key"]   = map_team(tt["home_team"], "odds", alias, "odds_totals.home_team")
        tt["away_key"]   = map_team(tt["away_team"], "odds", alias, "odds_totals.away_team")
        tt["match_date"] = pd.to_datetime(tt["commence_time"], errors="coerce").dt.date

    return sp, tt


def load_wiki_dim(store):
    """silver/text/wiki_articles/entity=team -> DataFrame mo ta CLB (gold/dim)."""
    w = store.read_prefix("silver/text/wiki_articles/entity=team/")
    if w.empty:
        print("  ! khong co wiki_articles -> bo qua dim_team_wiki_profile")
        return pd.DataFrame()
    dim = (w.sort_values("_key")
            .drop_duplicates(["title", "lang"], keep="last")
            [["title", "lang", "page_id", "word_count", "fetched_date"]])
    print(f"  . dim_team_wiki_profile: {len(dim)} dong (tinh, KHONG phai feature ML)")
    return dim


def load_news_buzz(store, alias):
    """
    silver/text/google_news_articles -> (team_key, date, n_mentions).
    Phat hien CLB bang quet text (title+summary).
    """
    n = store.read_prefix("silver/text/google_news_articles/")
    if n.empty:
        print("  ! khong co google_news_articles -> rong")
        return pd.DataFrame(columns=["team_key", "date", "n_mentions"])
    n = n.copy()
    n["date"] = (pd.to_datetime(n["published_ts"], errors="coerce", utc=True)
                 .dt.tz_localize(None).dt.date)
    n = n.dropna(subset=["date", "text"])
    _, any_src = alias
    aliases = sorted(any_src.items(), key=lambda kv: -len(kv[0]))
    rows = []
    for r in n.itertuples():
        low = r.text.lower()
        for al, tk in aliases:
            if al in low:
                rows.append((tk, r.date))
    out = pd.DataFrame(rows, columns=["team_key", "date"])
    if out.empty:
        return pd.DataFrame(columns=["team_key", "date", "n_mentions"])
    out = out.groupby(["team_key", "date"]).size().reset_index(name="n_mentions")
    print(f"  . news_buzz: quet {len(n):,} bai -> {len(out):,} dong team-ngay, "
          f"{out.team_key.nunique()} doi duoc nhac toi")
    return out


def load_injuries(store, alias):
    """
    silver/players/pr_player_injuries + dim/pr_club_injury_summary
    -> (team_key, ingest_date, n_injured_players, total_injuries).
    Dung ASOF JOIN trong DuckDB de lay snapshot gan nhat truoc match_date.
    """
    p = store.read_prefix("silver/players/pr_player_injuries/")
    s = store.read_prefix("silver/dim/pr_club_injury_summary/")
    cols = ["team_key", "ingest_date", "n_injured_players", "total_injuries"]
    if p.empty:
        print("  ! khong co pr_player_injuries -> rong")
        return pd.DataFrame(columns=cols)
    p = p.copy()
    p["team_key"]    = map_team(p["team"], "physioroom", alias, "pr_player_injuries.team")
    p["ingest_date"] = pd.to_datetime(p["ingest_date"], errors="coerce").dt.date
    agg = p.groupby(["team_key", "ingest_date"]).size().reset_index(name="n_injured_players")
    if not s.empty:
        s = s.copy()
        s["team_key"]    = map_team(s["team"], "physioroom", alias, "pr_club_injury_summary.team")
        s["ingest_date"] = pd.to_datetime(s["ingest_date"], errors="coerce").dt.date
        agg = agg.merge(s[["team_key", "ingest_date", "total_injuries"]],
                        on=["team_key", "ingest_date"], how="outer")
    print(f"  . injuries: {agg['ingest_date'].nunique()} lan scrape, {agg['team_key'].nunique()} doi")
    return agg.sort_values(["team_key", "ingest_date"])


def load_af_fixtures(store, alias):
    """silver/matches/af_fixtures -> (fixture_id, home_key, away_key, match_date, home_goals, away_goals)."""
    af = store.read_prefix("silver/matches/af_fixtures/")
    cols = ["fixture_id", "home_key", "away_key", "match_date", "home_goals", "away_goals"]
    if af.empty:
        print("  ! khong co af_fixtures -> khong bac cau duoc")
        return pd.DataFrame(columns=cols)
    af = af.sort_values("_key").drop_duplicates("fixture_id", keep="last").copy()
    af["home_key"]   = map_team(af["home_team"], "apifootball", alias, "af_fixtures.home_team")
    af["away_key"]   = map_team(af["away_team"], "apifootball", alias, "af_fixtures.away_team")
    dt = pd.to_datetime(af["date"], errors="coerce", utc=True)
    af["match_date"] = dt.dt.tz_localize(None).dt.date
    return af[cols]


def load_af_stats(store, alias):
    """
    silver/matches/af_match_stats -> (fixture_id, team_key,
    possession_pct, shots_on_goal, total_shots, corner_kicks, fouls).
    """
    s = store.read_prefix("silver/matches/af_match_stats/")
    cols = ["fixture_id", "team_key", "possession_pct", "shots_on_goal",
            "total_shots", "corner_kicks", "fouls"]
    if s.empty:
        print("  ! khong co af_match_stats -> rong")
        return pd.DataFrame(columns=cols)
    s = s.copy()
    s["team_key"] = map_team(s["team"], "apifootball", alias, "af_match_stats.team")
    poss = first_present(s, ["ball_possession", "possession"])
    return pd.DataFrame({
        "fixture_id":     s["fixture_id"].values,
        "team_key":       s["team_key"].values,
        "possession_pct": num(s, poss).values,
        "shots_on_goal":  num(s, "shots_on_goal").values,
        "total_shots":    num(s, "total_shots").values,
        "corner_kicks":   num(s, "corner_kicks").values,
        "fouls":          num(s, "fouls").values,
    })


def load_fdo(store, alias):
    """silver/matches/fdo_matches -> DataFrame (p09, CHI dung QA, khong tao feature)."""
    f = store.read_prefix("silver/matches/fdo_matches/")
    cols = ["match_id", "home_key", "away_key", "match_date", "home_goals", "away_goals", "status"]
    if f.empty:
        print("  ! khong co fdo_matches -> bo qua doi chieu p09")
        return pd.DataFrame(columns=cols)
    f = f.copy()
    f["home_key"]   = map_team(f["home_team"], "fdo", alias, "fdo_matches.home_team")
    f["away_key"]   = map_team(f["away_team"], "fdo", alias, "fdo_matches.away_team")
    f["match_date"] = pd.to_datetime(f["utc_date"], errors="coerce").dt.date
    return f[cols]


# ═══════════════════════════════════════════════════════════════════════
# 6. DUCKDB BRIDGE HELPER
# ═══════════════════════════════════════════════════════════════════════

def build_af_bridge(con):
    """
    Anh xa match_id (fd_matches) <-> fixture_id (api-football) qua
    (home_key, away_key, ngay lech <= 1 ngay).

    Tao bang DuckDB `bridge_fd_af_match` trong connection da cho.
    Yeu cau: con da register 'm' (fd_matches) va 'af_fx' (af_fixtures).
    """
    con.execute("""
    CREATE OR REPLACE TABLE bridge_fd_af_match AS
    SELECT m.match_id,
           af.fixture_id,
           m.match_date  AS fd_date,
           af.match_date AS af_date,
           (m.home_goals = af.home_goals AND m.away_goals = af.away_goals) AS score_match
    FROM m
    JOIN af_fx af
      ON  af.home_key = m.home_key
      AND af.away_key = m.away_key
      AND abs(date_diff('day', af.match_date, m.match_date)) <= 1
    """)
