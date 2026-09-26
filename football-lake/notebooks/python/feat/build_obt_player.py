#!/usr/bin/env python3
"""
build_obt_player.py — silver -> gold/obt/obt_player_360.parquet

ĐÂY LÀ OBT (One Big Table), KHÔNG PHẢI Feature cho Machine Learning.
Grain: 1 dòng = 1 player / 1 season (theo "Hướng dẫn triển khai OBT" mục 5.1).

Nguồn Player Statistics THỰC SỰ có trong Silver (chỉ dùng đúng những gì tồn tại -- mục 3):
    players/af_players           (API-Football, p24) -- ĐÃ ở grain player+season-to-date snapshot,
                                   dùng làm bảng XƯƠNG SỐNG (nhiều trường nhất: appearances, minutes,
                                   goals, assists, cards, shots, passes, dribbles, rating).
    players/understat_player_xg  (Understat, p19)     -- ĐÃ ở grain player+season, bổ sung xG/xA.
    players/fpl_player_gw + fpl_player_dim (FPL, p01) -- ở grain player+gameweek, tự gộp về season
                                   để lấy các trường ĐỘC QUYỀN không nguồn nào khác có: price_m,
                                   selected_by_percent, fpl_total_points, ict/bonus.

VẤN ĐỀ THẬT CẦN BIẾT TRƯỚC: 3 nguồn trên dùng 3 hệ player_id khác nhau (af: số nguyên riêng của
API-Football; understat: số nguyên riêng của Understat; fpl: số nguyên riêng của FPL) và KHÔNG có
seed/player_alias.csv nào để ánh xạ chuẩn (khác team, đã có seed/team_alias.csv). Script này ghép
bằng TÊN đã chuẩn hóa (bỏ dấu, hạ chữ thường, bỏ ký tự đặc biệt) + season -- best-effort, sẽ MISS
với các trường hợp tên viết khác nhau nhiều giữa các nguồn (vd "Bukayo Saka" vs "Saka"). Coverage
match được in ra cuối script; nếu cần chính xác hơn, nên tạo seed/player_alias.csv giống team.

Cũng phải tự chuẩn hóa SEASON vì 3 nguồn ghi season khác định dạng nhau:
    fpl:        "2025-26"   (đã đúng định dạng)
    understat:  "2024"      (quy ước: năm bắt đầu mùa -> "2024-25")
    af:         2024 (int)  (quy ước: năm bắt đầu mùa -> "2024-25")

Đặt tại: football-lake/ml/obt/build_obt_player.py
Chạy:
    python ml/obt/build_obt_player.py                # MinIO
    python ml/obt/build_obt_player.py --root ./lake --seed ./seed/team_alias.csv   # local

GHI: gold/obt/obt_player_360.parquet, gold/qa/obt_player_360_unmatched.csv (tên chưa ghép được nguồn nào)
"""
import re
import sys
import argparse
import unicodedata
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve()
sys.path.insert(0, str(_HERE.parents[2] / "notebooks" / "python"))
from build_features import LocalStore, MinioStore, find_seed, load_alias, map_team  # noqa: E402

OUT = "gold/obt"
QA_OUT = "gold/qa"


_EXTRA_CHAR_MAP = str.maketrans({
    # Các ký tự không tự tách dấu qua NFKD (bị unicodedata bỏ luôn nếu không map tay trước) --
    # gặp khá thường xuyên trong tên cầu thủ Bắc Âu (Ødegaard), Iceland (ð), Croatia/Việt Nam (đ)...
    "ø": "o", "Ø": "O", "đ": "d", "Đ": "D", "ð": "d", "Ð": "D", "ł": "l", "Ł": "L",
})


def normalize_name(s):
    """Chuẩn hóa tên cầu thủ để ghép giữa 3 nguồn: bỏ dấu, hạ chữ thường, bỏ ký tự đặc biệt.
    Map tay vài ký tự KHÔNG tự tách dấu qua NFKD trước (vd 'Ø' bị unicodedata bỏ trắng thay vì
    ra 'O' nếu không map tay -> "Ødegaard" vs "Odegaard" sẽ KHÔNG khớp nếu thiếu bước này)."""
    if not isinstance(s, str) or not s.strip():
        return None
    s = s.translate(_EXTRA_CHAR_MAP)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9\s]", " ", s).lower().strip()
    return re.sub(r"\s+", " ", s)


def normalize_season(s):
    """Quy 3 định dạng season khác nhau (fpl '2025-26', understat '2024', af 2024) về 1 chuẩn 'YYYY-YY'."""
    if pd.isna(s):
        return None
    s = str(s).strip()
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}", s):
        y = int(s)
        return f"{y}-{str(y + 1)[-2:]}"
    return s


def season_from_key(key, pattern=re.compile(r"season=([^/]+)/")):
    m = pattern.search(key or "")
    return m.group(1) if m else None


def load_af_players(store, alias):
    """silver/players/af_players: snapshot cộng dồn theo ingest_date -> chỉ giữ ingest_date GẦN NHẤT
    của mỗi (player_id, season) để lấy tổng cuối mùa, tránh double-count nhiều lần scrape."""
    p = store.read_prefix("silver/players/af_players/")
    if p.empty:
        print("  ! không có af_players -> obt_player_360 sẽ rất thiếu (đây là nguồn chính)")
        return pd.DataFrame()
    p = p.copy()
    p["season"] = p["_key"].map(season_from_key).map(normalize_season)
    p = p.sort_values("_key").drop_duplicates(["player_id", "season"], keep="last")
    p["team_key"] = map_team(p["team"], "apifootball", alias, "af_players.team")
    p["name_norm"] = p["name"].map(normalize_name)
    print(f"  · af_players: {len(p):,} dòng (player x season), {p['season'].nunique()} mùa")
    return p


def load_understat_players(store):
    """silver/players/understat_player_xg: đã ở grain player+season (Understat tự tổng hợp),
    chỉ cần dedupe nếu bị scrape lại nhiều ingest_date trong cùng 1 mùa."""
    u = store.read_prefix("silver/players/understat_player_xg/")
    if u.empty:
        print("  ! không có understat_player_xg -> bỏ qua enrich xG/xA cầu thủ")
        return pd.DataFrame()
    u = u.copy()
    if "league" in u.columns:
        u = u[u["league"] == "EPL"]
    namec = "player_name" if "player_name" in u.columns else "name"
    idc = "id" if "id" in u.columns else None
    u["season"] = u["season"].map(normalize_season) if "season" in u.columns else None
    dedupe_keys = [c for c in (idc, "season") if c]
    if dedupe_keys:
        u = u.sort_values("_key").drop_duplicates(dedupe_keys, keep="last")
    u["name_norm"] = u[namec].map(normalize_name)
    keep = {c: n for c, n in {
        "games": "us_games", "time": "us_minutes", "goals": "us_goals", "assists": "us_assists",
        "xG": "xg", "xA": "xa", "shots": "us_shots", "key_passes": "us_key_passes",
        "npg": "us_npg", "npxG": "npxg", "xGChain": "xg_chain", "xGBuildup": "xg_buildup",
        "xg_per90": "xg_per90", "xA_per90": "xa_per90", "goals_minus_xg": "goals_minus_xg",
    }.items() if c in u.columns}
    out = u[["name_norm", "season"] + list(keep.keys())].rename(columns=keep)
    print(f"  · understat_player_xg: {len(out):,} dòng (player x season)")
    return out


def load_fpl_players(store, alias):
    """silver/players/fpl_player_gw (1 dòng/player/gameweek) -> tự gộp SUM về season, join với
    fpl_player_dim để lấy tên/đội/vị trí + các trường độc quyền FPL (price_m, selected_by_percent)."""
    dim = store.read_prefix("silver/players/fpl_player_dim/")
    gw = store.read_prefix("silver/players/fpl_player_gw/")
    if dim.empty or gw.empty:
        print("  ! thiếu fpl_player_dim hoặc fpl_player_gw -> bỏ qua enrich FPL")
        return pd.DataFrame()
    dim = dim.sort_values("_key").drop_duplicates("id", keep="last").copy()
    gw = gw.copy()
    gw["season"] = gw["_key"].map(season_from_key).map(normalize_season)
    agg_spec = {c: ("sum" if c != "gameweek" else "nunique") for c in
                ["minutes", "goals_scored", "assists", "total_points", "bonus",
                 "expected_goals", "expected_assists", "gameweek"] if c in gw.columns}
    agg = gw.groupby(["player_id", "season"]).agg(agg_spec).reset_index()
    agg = agg.rename(columns={
        "player_id": "player_id_fpl", "minutes": "fpl_minutes", "goals_scored": "fpl_goals",
        "assists": "fpl_assists", "total_points": "fpl_total_points", "bonus": "fpl_bonus",
        "expected_goals": "fpl_xg", "expected_assists": "fpl_xa", "gameweek": "fpl_gw_played"})
    dim_cols = [c for c in ["id", "full_name", "team_name", "position", "price_m",
                             "selected_by_percent"] if c in dim.columns]
    agg = agg.merge(dim[dim_cols], left_on="player_id_fpl", right_on="id", how="left")
    agg["name_norm"] = agg["full_name"].map(normalize_name) if "full_name" in agg.columns else None
    agg["team_key_fpl"] = map_team(agg["team_name"], "fpl", alias, "fpl_player_dim.team_name")
    agg = agg.drop(columns=["id", "team_name"])
    print(f"  · fpl: {len(agg):,} dòng (player x season) sau khi gộp {len(gw):,} dòng gameweek")
    return agg


def build_obt_player(af, us, fpl):
    if af.empty:
        return pd.DataFrame()
    base = af.rename(columns={
        "player_id": "player_id_af", "name": "player_name", "team": "team_name",
    })[["player_id_af", "player_name", "name_norm", "nationality", "age", "team_id", "team_name",
        "team_key", "position", "season", "appearances", "minutes", "goals", "assists",
        "yellow_cards", "red_cards", "shots_total", "shots_on", "passes_total", "passes_key",
        "dribbles_att", "dribbles_ok", "rating"]].copy()

    if not us.empty:
        base = base.merge(us, on=["name_norm", "season"], how="left")
    if not fpl.empty:
        base = base.merge(
            fpl.drop(columns=["full_name", "position"], errors="ignore"),
            on=["name_norm", "season"], how="left")

    base["league"] = "EPL"
    return base


def qa(df, store):
    n = len(df)
    dup = df.duplicated(["player_id_af", "season"]).sum()
    print(f"\n[QA] obt_player_360: {n:,} dòng (player x season).")
    if dup:
        print(f"  ⚠ {dup} dòng trùng (player_id_af, season) -- kiểm tra lại dedupe nguồn af_players")
    else:
        print("  ✓ Grain đúng: không trùng (player_id_af, season)")
    cov = {
        "xg (understat)": df["xg"].notna().mean() if "xg" in df.columns else 0.0,
        "fpl_total_points": df["fpl_total_points"].notna().mean() if "fpl_total_points" in df.columns else 0.0,
    }
    print("  Độ phủ ghép nối (theo tên chuẩn hóa, best-effort -- xem docstring):")
    for k, v in cov.items():
        print(f"    {k}: {v:.0%}")
    unmatched_us = pd.DataFrame()
    if "xg" in df.columns:
        unmatched_us = df[df["xg"].isna()][["player_id_af", "player_name", "season"]].copy()
        unmatched_us["missing_from"] = "understat_player_xg"
    if not unmatched_us.empty:
        store.write_csv(f"{QA_OUT}/obt_player_360_unmatched.csv", unmatched_us)
        print(f"  · ghi {len(unmatched_us)} tên chưa ghép được Understat vào "
              f"{QA_OUT}/obt_player_360_unmatched.csv để soát/bổ sung seed/player_alias.csv sau này")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None)
    ap.add_argument("--seed", default=None)
    if "ipykernel" in sys.modules or "google.colab" in sys.modules:
        a = ap.parse_args([])
    else:
        a = ap.parse_args()

    store = LocalStore(a.root) if a.root else MinioStore()
    alias = load_alias(Path(a.seed) if a.seed else find_seed())

    print("[1/3] đọc silver: af_players (xương sống), understat_player_xg, fpl (gộp về season)")
    af = load_af_players(store, alias)
    us = load_understat_players(store)
    fpl = load_fpl_players(store, alias)

    print("[2/3] ghép theo (tên đã chuẩn hóa, season)")
    df = build_obt_player(af, us, fpl)
    if df.empty:
        sys.exit("Không có af_players -> không xây được obt_player_360 (chạy p24 trước)")

    print("[3/3] ghi obt_player_360")
    qa(df, store)
    store.write_parquet(f"{OUT}/obt_player_360.parquet", df)


if __name__ == "__main__":
    main()
