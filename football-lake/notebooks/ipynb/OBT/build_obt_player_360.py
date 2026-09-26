#!/usr/bin/env python3
"""
build_obt_player_360.py — silver -> gold/obt/obt_player_360.parquet

KHÔNG PHẢI feature ML. Đây là bảng OBT mô tả "1 cầu thủ có những thông tin/thành tích
gì trong 1 mùa giải?" theo tài liệu OBT (mục 5) — dùng cho Analytics/BI.

Đặt tại: football-lake/ml/obt/build_obt_player_360.py
Chạy:
    python ml/obt/build_obt_player_360.py                 # MinIO
    python ml/obt/build_obt_player_360.py --root ./lake    # thư mục local (thử nghiệm)

Grain (mục tiêu): 1 dòng = 1 cầu thủ / mùa giải (mục 5.1 tài liệu OBT).
Grain THỰC TẾ: 1 dòng = 1 cầu thủ / team_key / mùa giải — vì af_players (p24, nguồn
thống kê chính) chỉ lấy thống kê của ĐỘI ĐẦU TIÊN mà API trả về cho mỗi cầu thủ/mùa,
nên trường hợp hiếm cầu thủ đổi đội giữa mùa sẽ ra 2 dòng thay vì 1. Script assert theo
(player_key, team_key, season) để BÁO LỖI THẬT nếu có, thay vì âm thầm che giấu vấn đề.

CẢNH BÁO QUAN TRỌNG — đọc trước khi dùng:
  Không có player_id CHUNG giữa các nguồn (af_players, understat_player_xg, tsdb_players,
  fdo_players đều tự đánh id riêng theo hệ thống của họ). Script này ghép các nguồn theo
  player_key = normalize(tên cầu thủ) (xem common_obt.normalize_name).
    -> 2 cầu thủ trùng tên NHƯNG khác đội vẫn tách được (nhờ ghép thêm team_key/season).
    -> 2 cầu thủ trùng tên VÀ cùng đội (hiếm, ví dụ đội trẻ) sẽ bị gộp NHẦM.
  Nếu gặp trường hợp đó: tạo seed/player_alias.csv (source, alias, player_key) giống hệt
  seed/team_alias.csv, rồi sửa các hàm load_* trong file này để tra bảng đó trước khi
  fallback sang normalize_name().

ĐỌC silver (tái sử dụng loader/alias qua common_obt.py):
    players/af_players            (p24, BẮT BUỘC — nguồn thống kê chính: trận, phút, bàn,
                                    kiến tạo, thẻ, sút, chuyền, rating)
    players/understat_player_xg   (p19, tùy chọn — enrich xG/xA nâng cao)
    dim/tsdb_players               (p08, tùy chọn — bio: quốc tịch, ngày sinh, cao/nặng, lương)
    dim/fdo_players                 (p09, tùy chọn — bio fallback #2: vị trí, ngày sinh, quốc tịch)
    players/pr_player_injuries    (p16, tùy chọn — trạng thái chấn thương HIỆN TẠI, xem giới hạn
                                    trong load_injury_status())
GHI gold/obt/obt_player_360.parquet

KHÔNG đưa vào OBT:
    - fpl_player_gw (p01): pipeline này KHÔNG nằm trong danh sách DAG đang chạy -> Silver
      chưa có dữ liệu. Nếu sau này bật lại p01, có thể thêm loader tương tự af_players.
    - wd_players (p06): cột "clubs"/"positions" ở dạng list-string gộp nhiều giá trị lịch
      sử, khó ghép an toàn theo (player_key, team_key, season) -> để dành cho việc mở rộng
      sau, chưa đưa vào bản này.
"""
import argparse
import sys
from pathlib import Path

import pandas as pd

from common_obt import (
    LocalStore, MinioStore, find_seed, load_alias, map_team, first_present, num,
    normalize_name,
)

OUT = "gold/obt/obt_player_360.parquet"


def load_af_players(store, alias):
    """silver/players/af_players (p24): nguồn CHÍNH, đã có sẵn thống kê cộng dồn theo mùa.
    Nhiều snapshot ingest_date/mùa -> giữ snapshot GẦN NHẤT (số liệu mới nhất) mỗi
    player_key + team_key + season."""
    p = store.read_prefix("silver/players/af_players/")
    if p.empty:
        print("  ! không có af_players trong Silver -> obt_player_360 sẽ RỖNG "
              "(đây là nguồn thống kê chính, cần chạy p24 trước)")
        return pd.DataFrame()
    p = p.copy()
    p["team_key"] = map_team(p["team"], "apifootball", alias, "af_players.team")
    p["player_key"] = p["name"].map(normalize_name)
    season_col = first_present(p, ["season"])
    p["season"] = (p[season_col].astype(str) if season_col
                   else p["_key"].str.extract(r"season=([^/]+)")[0])
    p = (p.dropna(subset=["player_key"])
           .sort_values("_key")
           .drop_duplicates(["player_key", "team_key", "season"], keep="last"))
    keep = ["player_key", "name", "team_key", "team", "season", "position", "nationality",
            "age", "height", "weight", "appearances", "minutes", "goals", "assists",
            "yellow_cards", "red_cards", "shots_total", "shots_on", "passes_total",
            "passes_key", "dribbles_att", "dribbles_ok", "rating"]
    out = p[[c for c in keep if c in p.columns]].rename(columns={"name": "player_name"})
    print(f"  · af_players: {len(out):,} dòng player/team/season (nguồn thống kê chính)")
    return out


def load_understat_player_xg_season(store, alias):
    """silver/players/understat_player_xg (p19): xG/xA nâng cao — enrich theo
    (player_key, season). Cột gốc: player_name (hoặc 'player'), team_title, games, time,
    goals, xG, assists, xA, shots, npg, npxG, xg_per90, xA_per90, goals_minus_xg,
    xGChain_per90 (đã tính sẵn ở p19)."""
    u = store.read_prefix("silver/players/understat_player_xg/")
    cols = ["player_key", "season", "xg", "xa", "npxg", "xg_per90", "xa_per90",
            "goals_minus_xg", "xgchain_per90"]
    if u.empty:
        print("  ! không có understat_player_xg -> bỏ qua enrich xG cầu thủ")
        return pd.DataFrame(columns=cols)
    u = u.copy()
    name_col = first_present(u, ["player_name", "player"])
    if not name_col:
        print(f"  ! understat_player_xg thiếu cột tên cầu thủ (hiện có: "
              f"{[c for c in u.columns if c != '_key']}) -> bỏ qua")
        return pd.DataFrame(columns=cols)
    u["player_key"] = u[name_col].map(normalize_name)
    u["season"] = u["season"].astype(str)
    u = u.dropna(subset=["player_key"]).sort_values("_key")
    dedup_keys = ["player_key", "season"] + (["league"] if "league" in u.columns else [])
    u = u.drop_duplicates(dedup_keys, keep="last")
    out = pd.DataFrame({
        "player_key": u["player_key"], "season": u["season"],
        "xg": num(u, "xG"), "xa": num(u, "xA"), "npxg": num(u, "npxG"),
        "xg_per90": num(u, "xg_per90"), "xa_per90": num(u, "xA_per90"),
        "goals_minus_xg": num(u, "goals_minus_xg"),
        "xgchain_per90": num(u, "xGChain_per90"),
    }).drop_duplicates(["player_key", "season"], keep="last")
    print(f"  · understat_player_xg: {len(out):,} dòng player/season sẽ enrich xG/xA")
    return out


def load_bio_tsdb(store):
    """silver/dim/tsdb_players (p08): bio bổ sung — quốc tịch, ngày sinh, cao/nặng, lương,
    transfermarkt_id. Ghép theo player_key (KHÔNG có team_key chung đáng tin cậy giữa
    tsdb và af/understat nên chỉ ghép theo tên, xem CẢNH BÁO đầu file)."""
    t = store.read_prefix("silver/dim/tsdb_players/")
    cols = ["player_key", "tsdb_nationality", "tsdb_birth_date", "tsdb_height",
            "tsdb_weight", "tsdb_wage", "transfermarkt_id"]
    if t.empty:
        print("  ! không có tsdb_players -> bỏ qua bio bổ sung (thesportsdb)")
        return pd.DataFrame(columns=cols)
    t = t.copy()
    t["player_key"] = t["player_name"].map(normalize_name)
    t = t.dropna(subset=["player_key"]).sort_values("_key").drop_duplicates("player_id", keep="last")
    out = pd.DataFrame({
        "player_key": t["player_key"],
        "tsdb_nationality": t.get("nationality"),
        "tsdb_birth_date": pd.to_datetime(t.get("birth_date"), errors="coerce"),
        "tsdb_height": t.get("height"), "tsdb_weight": t.get("weight"),
        "tsdb_wage": t.get("wage"), "transfermarkt_id": t.get("transfermarkt_id"),
    }).drop_duplicates("player_key", keep="last")
    print(f"  · tsdb_players: {len(out):,} cầu thủ có bio bổ sung")
    return out


def load_bio_fdo(store, alias):
    """silver/dim/fdo_players (p09, football-data.org): bio fallback #2 — vị trí, ngày
    sinh, quốc tịch. Cột gốc: player_id, name, position, date_of_birth, nationality,
    team_id, team_name."""
    f = store.read_prefix("silver/dim/fdo_players/")
    cols = ["player_key", "fdo_position", "fdo_birth_date", "fdo_nationality"]
    if f.empty:
        print("  ! không có fdo_players -> bỏ qua bio bổ sung (football-data.org)")
        return pd.DataFrame(columns=cols)
    f = f.copy()
    f["player_key"] = f["name"].map(normalize_name)
    f = f.dropna(subset=["player_key"]).sort_values("_key").drop_duplicates("player_id", keep="last")
    out = pd.DataFrame({
        "player_key": f["player_key"], "fdo_position": f.get("position"),
        "fdo_birth_date": pd.to_datetime(f.get("date_of_birth"), errors="coerce"),
        "fdo_nationality": f.get("nationality"),
    }).drop_duplicates("player_key", keep="last")
    print(f"  · fdo_players: {len(out):,} cầu thủ có bio bổ sung")
    return out


def load_injury_status(store):
    """silver/players/pr_player_injuries (p16): CHỈ LÀ SNAPSHOT chấn thương HIỆN TẠI
    (ingest_date gần nhất khi scrape), KHÔNG PHẢI trạng thái đúng theo từng mùa lịch sử.
    Vì vậy ở main() chỉ gắn cột này vào dòng của MÙA GIẢI MỚI NHẤT, các mùa cũ luôn NULL
    — tránh hiểu nhầm là 'cầu thủ X từng chấn thương ở mùa 2019/20'."""
    p = store.read_prefix("silver/players/pr_player_injuries/")
    cols = ["player_key", "is_injured_now", "injury_type_now", "injury_asof_date"]
    if p.empty:
        print("  ! không có pr_player_injuries -> bỏ qua trạng thái chấn thương")
        return pd.DataFrame(columns=cols)
    p = p.copy()
    p["player_key"] = p["player_name"].map(normalize_name)
    p["ingest_date"] = pd.to_datetime(p["ingest_date"], errors="coerce")
    p = p.dropna(subset=["player_key", "ingest_date"])
    latest = p.sort_values("ingest_date").drop_duplicates("player_key", keep="last")
    out = pd.DataFrame({
        "player_key": latest["player_key"], "is_injured_now": True,
        "injury_type_now": latest["injury_type"], "injury_asof_date": latest["ingest_date"].dt.date,
    })
    print(f"  · pr_player_injuries: {len(out):,} cầu thủ đang có chấn thương ghi nhận "
          f"(snapshot mới nhất: {out['injury_asof_date'].max() if len(out) else 'n/a'})")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", default=None, help="thư mục local thay vì MinIO (thử nghiệm)")
    ap.add_argument("--seed", default=None)
    args = ap.parse_args([] if ("ipykernel" in sys.modules or "google.colab" in sys.modules) else None)

    store = LocalStore(args.root) if args.root else MinioStore()
    alias = load_alias(Path(args.seed) if args.seed else find_seed())

    print("[1/5] spine + thống kê chính: af_players (p24)")
    spine = load_af_players(store, alias)
    if spine.empty:
        sys.exit("Dừng: không có af_players trong Silver — chạy p24 trước khi build obt_player_360")

    print("[2/5] enrich xG/xA: understat_player_xg (p19)")
    xg = load_understat_player_xg_season(store, alias)

    print("[3/5] bio bổ sung: tsdb_players (p08) + fdo_players (p09)")
    bio_tsdb = load_bio_tsdb(store)
    bio_fdo = load_bio_fdo(store, alias)

    print("[4/5] trạng thái chấn thương hiện tại (p16, chỉ áp cho mùa mới nhất)")
    injury = load_injury_status(store)

    print("[5/5] JOIN theo Grain 1 dòng / player_key / team_key / season")
    obt = spine.merge(xg, on=["player_key", "season"], how="left")
    obt = obt.merge(bio_tsdb, on="player_key", how="left")
    obt = obt.merge(bio_fdo, on="player_key", how="left")

    latest_season = sorted(obt["season"].dropna().unique())[-1] if obt["season"].notna().any() else None
    obt = obt.merge(injury, on="player_key", how="left")
    if latest_season is not None:
        old = obt["season"] != latest_season
        obt.loc[old, ["is_injured_now", "injury_type_now", "injury_asof_date"]] = None
        obt["is_injured_now"] = obt["is_injured_now"].fillna(False)

    dup = int(obt.duplicated(["player_key", "team_key", "season"]).sum())
    assert dup == 0, (f"VỠ GRAIN: {dup} dòng trùng (player_key, team_key, season) — "
                       f"kiểm tra lại drop_duplicates trong load_af_players()")

    cov = obt.notna().mean().round(2)
    print("\n[QA] Grain OK. Độ phủ theo nhóm cột:")
    for grp, cols in {"xg": ["xg"], "bio_tsdb": ["tsdb_nationality"],
                       "bio_fdo": ["fdo_position"], "injury": ["is_injured_now"]}.items():
        cols = [c for c in cols if c in cov.index]
        if cols:
            print(f"  · {grp:10s}: {cov[cols].mean():.0%}")

    print(f"\n✓ obt_player_360: {len(obt):,} dòng, {obt['player_key'].nunique():,} cầu thủ "
          f"duy nhất, {obt.shape[1]} cột, mùa: {sorted(obt['season'].dropna().unique())}")
    store.write_parquet(OUT, obt)


if __name__ == "__main__":
    main()
