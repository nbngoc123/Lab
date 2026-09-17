"""
Ingest Understat qua thư viện cộng đồng 'jeke-understat-scrapper'.
Import name: understatapi.UnderstatClient

LƯU Ý: understat.com có robots.txt chặn bot. Thư viện này được cộng đồng
phân tích bóng đá dùng rộng rãi cho mục đích học thuật. Giữ tần suất thấp
(hàng tuần) và không dùng thương mại.

Cần thêm vào requirements.txt:
  jeke-understat-scrapper
"""
import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, today, summary

SRC = "understat"
D   = today()

# Tất cả 6 giải Understat hỗ trợ
LEAGUES = ["EPL", "La_liga", "Bundesliga", "Serie_A", "Ligue_1", "RFPL"]
SEASON  = "2025"   # năm bắt đầu mùa 2025/26

# TEST_MODE: True = chỉ lấy EPL (nhanh)
# False = lấy cả 6 giải
TEST_MODE = True

# Số trận cào shot data (1 request/trận, tốn nhiều lời gọi hơn)
# Đặt 0 để tắt hoàn toàn, tăng dần theo tuần
MAX_SHOT_MATCHES = 0


def get_client():
    try:
        from understatapi import UnderstatClient
        return UnderstatClient()
    except ImportError:
        raise ImportError(
            "Thiếu thư viện understatapi. "
            "Chạy: pip install jeke-understat-scrapper"
        )


# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
def ingest_players(client, league: str) -> list:
    data = client.league(league=league).get_player_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/players/league={league}/season={SEASON}"
        f"/ingest_date={D}/players.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_teams(client, league: str) -> dict:
    data = client.league(league=league).get_team_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/teams/league={league}/season={SEASON}"
        f"/ingest_date={D}/teams.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_matches(client, league: str) -> list:
    data = client.league(league=league).get_match_data(season=SEASON)
    put_json_gz(
        f"bronze/understat/matches/league={league}/season={SEASON}"
        f"/ingest_date={D}/matches.json.gz",
        data, SRC, meta={"count": len(data)})
    return data


def ingest_shots_for_matches(client, match_ids: list, league: str,
                              max_matches: int = 20, sleep: float = 1.5):
    shots_all = []
    for mid in match_ids[:max_matches]:
        try:
            shots = client.match(match=str(mid)).get_shot_data()
        except Exception as e:
            print(f"  ! match {mid}: {e}")
            continue
        put_json_gz(
            f"bronze/understat/shots/league={league}/season={SEASON}"
            f"/match_id={mid}/shots.json.gz",
            shots, SRC, meta={"match_id": mid})
        shots_all.append((mid, shots))
        time.sleep(sleep)
    print(f"  ✓ shot data: {len(shots_all)}/{min(max_matches, len(match_ids))} trận")
    return shots_all


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
def build_player_xg(players: list, league: str) -> pd.DataFrame:
    df = pd.DataFrame(players)
    numeric_cols = ["games", "time", "goals", "xG", "assists", "xA", "shots",
                    "key_passes", "yellow_cards", "red_cards",
                    "npg", "npxG", "xGChain", "xGBuildup"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["xg_per90"]       = df["xG"] / (df["time"] / 90).replace(0, pd.NA)
    df["goals_minus_xg"] = df["goals"] - df["xG"]
    df["xA_per90"]       = df["xA"] / (df["time"] / 90).replace(0, pd.NA)
    df["xGChain_per90"]  = df["xGChain"] / (df["time"] / 90).replace(0, pd.NA)
    df["league"] = league
    df["season"] = SEASON
    df["ingest_date"] = D
    put_parquet(
        f"silver/players/understat_player_xg/league={league}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    print(f"  ✓ {len(df)} cầu thủ -> understat_player_xg [{league}]")
    return df


def build_team_xg(teams: dict, league: str) -> pd.DataFrame:
    """teams = dict {team_id: {title, history: [match1, ...]}}"""
    rows = []
    for team_id, info in teams.items():
        title = info.get("title")
        for match in info.get("history", []):
            ppda = match.get("ppda")
            ppda_val = None
            if isinstance(ppda, dict):
                att = ppda.get("att", 0)
                def_ = ppda.get("def", 1)
                ppda_val = att / def_ if def_ else None
            elif isinstance(ppda, (int, float)):
                ppda_val = ppda
            rows.append({
                "team_id":    team_id,
                "team_name":  title,
                "date":       match.get("date"),
                "h_a":        match.get("h_a"),
                "xG":         match.get("xG"),
                "xGA":        match.get("xGA"),
                "npxG":       match.get("npxG"),
                "npxGA":      match.get("npxGA"),
                "result":     match.get("result"),
                "ppda":       ppda_val,
                "scored":     match.get("scored"),
                "missed":     match.get("missed"),
                "wins":       match.get("wins"),
                "draws":      match.get("draws"),
                "loses":      match.get("loses"),
                "pts":        match.get("pts"),
            })
    df = pd.DataFrame(rows)
    df["date"]   = pd.to_datetime(df["date"], errors="coerce")
    df["league"] = league
    df["season"] = SEASON
    put_parquet(
        f"silver/teams/understat_team_xg/league={league}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    print(f"  ✓ {df.team_name.nunique()} đội -> understat_team_xg [{league}]")
    return df


def build_match_xg(matches: list, league: str) -> pd.DataFrame:
    df = pd.DataFrame(matches)
    if "datetime" in df.columns:
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["league"] = league
    df["season"] = SEASON
    # ép kiểu các cột xG
    for c in ["xG_home", "xG_away", "goals_h", "goals_a"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    put_parquet(
        f"silver/matches/understat_match_xg/league={league}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    print(f"  ✓ {len(df)} trận -> understat_match_xg [{league}]")
    return df


def build_shots(shots_all: list, league: str) -> pd.DataFrame:
    """shotsData = {'h': [...], 'a': [...]}"""
    rows = []
    for mid, shots in shots_all:
        for side in ("h", "a"):
            for s in shots.get(side, []):
                row = dict(s)
                row["match_id"] = mid
                row["side"]     = side
                rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for c in ["X", "Y", "xG", "minute"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["league"] = league
    df["season"] = SEASON
    put_parquet(
        f"silver/events/understat_shots/league={league}"
        f"/season={SEASON}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    print(f"  ✓ {len(df)} cú sút -> understat_shots [{league}]")
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_pipeline():
    print(f"[init] Khởi tạo UnderstatClient (TEST_MODE={TEST_MODE})")
    client = get_client()

    leagues = ["EPL"] if TEST_MODE else LEAGUES

    for league in leagues:
        print(f"\n--- {league} ---")
        try:
            print(f"[1/4] players")
            players = ingest_players(client, league)
            build_player_xg(players, league)

            print(f"[2/4] teams")
            teams = ingest_teams(client, league)
            build_team_xg(teams, league)

            print(f"[3/4] matches")
            matches = ingest_matches(client, league)
            build_match_xg(matches, league)

            if MAX_SHOT_MATCHES > 0:
                print(f"[4/4] shot data (top {MAX_SHOT_MATCHES} trận đã đá)")
                finished = [m["id"] for m in matches if m.get("isResult")]
                shots_all = ingest_shots_for_matches(
                    client, finished, league, max_matches=MAX_SHOT_MATCHES)
                build_shots(shots_all, league)
            else:
                print("[4/4] shot data: bỏ qua (MAX_SHOT_MATCHES=0)")

        except Exception as e:
            print(f"  ! Lỗi cho {league}: {e}")
            continue

    print("\n[summary]")
    summary("bronze/understat/")
    summary("silver/players/understat_player_xg/")
    summary("silver/teams/understat_team_xg/")


if __name__ == "__main__":
    run_pipeline()
