"""
Ingest Understat qua thư viện cộng đồng 'jeke-understat-scrapper'.
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.

Import name: understatapi.UnderstatClient

LƯU Ý: understat.com có robots.txt chặn bot. Thư viện này được cộng đồng
phân tích bóng đá dùng rộng rãi cho mục đích học thuật. Giữ tần suất thấp
(hàng tuần) và không dùng thương mại.
"""
import time
import os
from lake.minio_io import put_json_gz, exists, today

SRC = "understat"
D   = today()

# Tất cả 6 giải Understat hỗ trợ
LEAGUES = ["EPL", "La_Liga", "Bundesliga", "Serie_A", "Ligue_1", "RFPL"]
SEASONS = ["2024", "2025"]

TEST_MODE = os.getenv("TEST_MODE") == "1"
MAX_SHOT_MATCHES = 3 if TEST_MODE else 1000

def get_client():
    try:
        from understatapi import UnderstatClient
        return UnderstatClient()
    except ImportError:
        raise ImportError(
            "Thiếu thư viện understatapi. "
            "Chạy: pip install jeke-understat-scrapper"
        )

def get_partitions() -> list[dict]:
    leagues = ["EPL"] if TEST_MODE else LEAGUES
    return [{"league": l, "season": s} for l in leagues for s in SEASONS]

def ingest(partition: dict) -> str:
    league = partition["league"]
    season = partition["season"]
    client = get_client()

    print(f"\n--- Ingesting {league} / {season} ---")

    # 1. Players
    p_key = f"raw/understat/players/league={league}/season={season}/ingest_date={D}/players.json.gz"
    if not exists(p_key):
        players = client.league(league=league).get_player_data(season=season)
        put_json_gz(p_key, players, SRC, meta={"count": len(players)})
        print(f"  ✓ players -> {p_key}")
    else:
        print(f"  · players đã có")

    # 2. Teams
    t_key = f"raw/understat/teams/league={league}/season={season}/ingest_date={D}/teams.json.gz"
    if not exists(t_key):
        teams = client.league(league=league).get_team_data(season=season)
        put_json_gz(t_key, teams, SRC, meta={"count": len(teams)})
        print(f"  ✓ teams -> {t_key}")
    else:
        print(f"  · teams đã có")

    # 3. Matches
    m_key = f"raw/understat/matches/league={league}/season={season}/ingest_date={D}/matches.json.gz"
    matches = None
    if not exists(m_key):
        matches = client.league(league=league).get_match_data(season=season)
        put_json_gz(m_key, matches, SRC, meta={"count": len(matches)})
        print(f"  ✓ matches -> {m_key}")
    else:
        print(f"  · matches đã có")

    # 4. Shots
    if MAX_SHOT_MATCHES > 0:
        if not matches:
            # Re-fetch matches if we skipped them but need them for shots
            matches = client.league(league=league).get_match_data(season=season)
            
        finished = [m["id"] for m in matches if m.get("isResult")]
        shots_all = []
        for mid in finished[:MAX_SHOT_MATCHES]:
            s_key = f"raw/understat/shots/league={league}/season={season}/match_id={mid}/shots.json.gz"
            if exists(s_key):
                continue
            try:
                shots = client.match(match=str(mid)).get_shot_data()
            except Exception as e:
                print(f"  ! match {mid}: {e}")
                continue
            put_json_gz(s_key, shots, SRC, meta={"match_id": mid})
            shots_all.append(mid)
            time.sleep(1.5)
            
        if shots_all:
            print(f"  ✓ shot data: lấy mới {len(shots_all)} trận")
        else:
            print(f"  · shot data: không có trận mới")

    return m_key
