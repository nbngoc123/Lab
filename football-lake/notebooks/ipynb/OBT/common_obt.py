"""
ml/obt/common_obt.py
---------------------
Tiện ích DÙNG CHUNG cho 2 bảng OBT (obt_match_360.py, obt_player_360.py).

Import lại toàn bộ hạ tầng đọc Silver / xử lý team_key / MinIO đã có sẵn trong
notebooks/python/build_features.py (silver -> feature) để KHÔNG viết lại logic
tìm seed/team_alias.csv, map tên đội -> team_key, đọc/ghi MinIO...
"""
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

# --- tái sử dụng hạ tầng đọc Silver / alias / MinIO từ build_features.py ---------------
_ROOT = Path(__file__).resolve().parents[2]  # .../football-lake
sys.path.insert(0, str(_ROOT / "notebooks" / "python"))
import build_features as bf  # noqa: E402

LocalStore = bf.LocalStore
MinioStore = bf.MinioStore
find_seed = bf.find_seed
load_alias = bf.load_alias
map_team = bf.map_team
first_present = bf.first_present
num = bf.num
load_matches = bf.load_matches
load_odds = bf.load_odds
load_xg = bf.load_xg
load_pageviews = bf.load_pageviews
load_weather = bf.load_weather
load_odds_handicap = bf.load_odds_handicap
load_injuries = bf.load_injuries
load_af_fixtures = bf.load_af_fixtures
load_af_stats = bf.load_af_stats
load_news_buzz = bf.load_news_buzz
load_wiki_dim = bf.load_wiki_dim
load_fdo = bf.load_fdo


# ---------------- chuẩn hoá tên -> khoá thay thế khi nguồn không share ID -------------
def normalize_name(s):
    """'Bruno Fernandes ' / 'BRUNO FERNANDES' / 'Bruno Fernándes' -> 'bruno fernandes'.
    Dùng làm player_key vì af_players / understat_player_xg / tsdb_players / fdo_players
    đều tự đánh player_id riêng, không có ID chung.

    CẢNH BÁO: 2 cầu thủ trùng tên (hiếm, thường ở đội trẻ) sẽ bị gộp nhầm. Nếu gặp,
    tạo seed/player_alias.csv (source, alias, player_key) giống hệt seed/team_alias.csv
    rồi sửa nơi gọi normalize_name() để tra bảng đó trước."""
    if not isinstance(s, str) or not s.strip():
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z\s]", " ", s).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


# ---------------- dimension mô tả đội (KHÔNG phải feature ML — chỉ mô tả tĩnh) --------
def load_team_dim_tsdb(store, alias):
    """silver/dim/tsdb_teams (p08): nguồn sạch nhất cho mô tả đội — sân, sức chứa,
    năm thành lập, quốc gia. Cột gốc: team_id, team_name, formed_year, league, country,
    stadium, stadium_capacity, stadium_location, website, description_en."""
    t = store.read_prefix("silver/dim/tsdb_teams/")
    cols = ["team_key", "stadium", "stadium_capacity", "formed_year", "country", "website"]
    if t.empty:
        print("  ! không có tsdb_teams -> bỏ qua dim đội (thesportsdb)")
        return pd.DataFrame(columns=cols)
    t = t.sort_values("_key").drop_duplicates("team_id", keep="last").copy()
    t["team_key"] = map_team(t["team_name"], "thesportsdb", alias, "tsdb_teams.team_name")
    out = t.reindex(columns=["team_key", "stadium", "stadium_capacity", "formed_year",
                              "country", "website"])
    out = out.dropna(subset=["team_key"]).drop_duplicates("team_key", keep="last")
    print(f"  · tsdb_teams: {len(out)} đội có dimension mô tả")
    return out


def load_team_dim_wikidata(store, alias):
    """silver/dim/wd_clubs (p06): fallback — lấp khoảng trống capacity/năm thành lập nếu
    tsdb thiếu. Tên cột SPARQL có thể lệch tuỳ query -> tra động bằng first_present()."""
    w = store.read_prefix("silver/dim/wd_clubs/")
    cols = ["team_key", "wd_capacity", "wd_inception_year"]
    if w.empty:
        print("  ! không có wd_clubs -> bỏ qua dim đội (wikidata)")
        return pd.DataFrame(columns=cols)
    name_col = first_present(w, ["clubLabel", "club_label"])
    if not name_col:
        print(f"  ! wd_clubs không có cột tên CLB nhận diện được (cột hiện có: "
              f"{[c for c in w.columns if c != '_key']}) -> bỏ qua")
        return pd.DataFrame(columns=cols)
    w = w.sort_values("_key").drop_duplicates(name_col, keep="last").copy()
    w["team_key"] = map_team(w[name_col], "wikidata", alias, "wd_clubs.clubLabel")
    inception_year = (pd.to_datetime(w["inception"], errors="coerce", utc=True).dt.year
                       if "inception" in w.columns else pd.Series(float("nan"), index=w.index))
    out = pd.DataFrame({"team_key": w["team_key"], "wd_capacity": num(w, "capacity"),
                         "wd_inception_year": inception_year})
    out = out.dropna(subset=["team_key"]).drop_duplicates("team_key", keep="last")
    print(f"  · wd_clubs: {len(out)} đội có dimension bổ sung (wikidata)")
    return out


# ---------------- xG THỰC TẾ của từng trận (khác rolling trong feature layer) ---------
def load_match_xg_understat(store, alias, m):
    """silver/matches/understat_match_xg (p19): xG THỰC của từng trận, KHÁC với
    feature_team_xg (trung bình rolling 5/10 trận dùng cho ML). Understat không có
    match_id chung với fd_matches -> ghép gần đúng theo (home_key, away_key, ngày lệch
    <=1), cùng cách tiếp cận với bridge_fd_af_match trong build_features.py."""
    u = store.read_prefix("silver/matches/understat_match_xg/")
    cols = ["match_id", "home_xg_actual", "away_xg_actual"]
    if u.empty:
        print("  ! không có understat_match_xg -> obt sẽ không có xG thực trận")
        return pd.DataFrame(columns=cols)
    u = u.copy()
    home_col = first_present(u, ["home_team", "h_title", "home"])
    away_col = first_present(u, ["away_team", "a_title", "away"])
    date_col = first_present(u, ["datetime", "date"])
    xh_col = first_present(u, ["xG_home", "xg_home"])
    xa_col = first_present(u, ["xG_away", "xg_away"])
    if not all([home_col, away_col, date_col, xh_col, xa_col]):
        print(f"  ! understat_match_xg thiếu cột cần thiết (home/away/date/xG_home/xG_away). "
              f"Cột hiện có: {[c for c in u.columns if c != '_key']} -> bỏ qua "
              f"(kiểm tra lại tên cột thật của thư viện understatapi trước khi bật lại)")
        return pd.DataFrame(columns=cols)
    u["home_key"] = map_team(u[home_col], "understat", alias, "understat_match_xg.home")
    u["away_key"] = map_team(u[away_col], "understat", alias, "understat_match_xg.away")
    u["u_date"] = pd.to_datetime(u[date_col], errors="coerce").dt.date
    u = u.dropna(subset=["u_date"])

    mm = m[["match_id", "home_key", "away_key", "match_date"]]
    merged = mm.merge(u[["home_key", "away_key", "u_date", xh_col, xa_col]],
                       on=["home_key", "away_key"], how="inner")
    merged["day_diff"] = (pd.to_datetime(merged["match_date"]) -
                           pd.to_datetime(merged["u_date"])).abs().dt.days
    merged = (merged[merged["day_diff"] <= 1]
              .sort_values("day_diff")
              .drop_duplicates("match_id", keep="first"))
    out = merged[["match_id", xh_col, xa_col]].rename(
        columns={xh_col: "home_xg_actual", xa_col: "away_xg_actual"})
    print(f"  · understat_match_xg: khớp được {len(out):,}/{len(m):,} trận")
    return out


# ---------------- thống kê trận đấu THỰC TẾ (không rolling) từ p24 --------------------
def load_af_match_stats_actual(store, alias, m):
    """silver/matches/af_fixtures + af_match_stats (p24): thống kê THỰC của trận (sút,
    kiểm soát bóng, phạt góc, phạm lỗi) — không rolling, khác feature_team_stats_af.
    Ghép qua bridge (home_key, away_key, ngày lệch <=1), cùng logic build_af_bridge
    trong build_features.py nhưng viết lại bằng pandas (script này không dùng duckdb)."""
    af_fx = load_af_fixtures(store, alias)
    af_stats = load_af_stats(store, alias)
    stat_cols = ["possession_pct", "shots_on_goal", "total_shots", "corner_kicks", "fouls"]
    if af_fx.empty or af_stats.empty:
        print("  ! thiếu af_fixtures hoặc af_match_stats -> bỏ qua thống kê trận thực (p24)")
        return pd.DataFrame(columns=["match_id"])

    mm = m[["match_id", "home_key", "away_key", "match_date"]]
    bridge = mm.merge(af_fx[["fixture_id", "home_key", "away_key", "match_date"]],
                       on=["home_key", "away_key"], suffixes=("", "_af"))
    bridge["day_diff"] = (pd.to_datetime(bridge["match_date"]) -
                           pd.to_datetime(bridge["match_date_af"])).abs().dt.days
    bridge = (bridge[bridge["day_diff"] <= 1]
              .sort_values("day_diff")
              .drop_duplicates("match_id", keep="first")[["match_id", "fixture_id"]])

    s = af_stats.merge(bridge, on="fixture_id")
    home = (s.merge(m[["match_id", "home_key"]], on="match_id")
              .query("team_key == home_key")[["match_id"] + stat_cols]
              .rename(columns={c: f"home_{c}" for c in stat_cols}))
    away = (s.merge(m[["match_id", "away_key"]], on="match_id")
              .query("team_key == away_key")[["match_id"] + stat_cols]
              .rename(columns={c: f"away_{c}" for c in stat_cols}))
    out = home.merge(away, on="match_id", how="outer")
    print(f"  · af_match_stats (thực, p24): khớp được {len(out):,}/{len(m):,} trận")
    return out
