"""
Ingest Wikimedia Pageviews (p10) - phiên bản tối đa.

Chiến lược thu thập:
  1. Hardcoded seed list: đội bóng, giải đấu, cầu thủ nổi tiếng nhất (chạy đầu tiên)
  2. Auto-discover từ Silver fdo_players (p09): tự tạo Wikipedia title từ tên
     thật → thử gọi API → nếu trả về dữ liệu thì lưu, nếu 404 bỏ qua.
     Điều này cho phép theo dõi TOÀN BỘ ~1500-2000 cầu thủ của 6 giải tự động
     mà không cần hardcode.
  3. Multi-language: en, es, pt, de, fr - đo độ hot theo vùng địa lý
  4. Top viral hàng ngày (top 1000 trang)
  5. Spike detection nâng cao (Z-score 28 ngày)
"""
import time
import os
import re
import unicodedata
from datetime import date, timedelta
import pandas as pd
import numpy as np
from lake.minio_io import put_json_gz, put_parquet, today, summary
from lake.http import SESSION

SRC = "wikimedia-pageviews"
D = today()
BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews"
UA = "football-lake/0.1 (educational project; contact: you@example.com)"
HEADERS = {"User-Agent": UA}

START = "20230101"   # yyyymmdd
END   = date.today().strftime("%Y%m%d")

# True = chỉ lấy 3 đội + 3 cầu thủ để test nhanh
# False = chạy full (có thể mất 10-20 phút)
TEST_MODE = True

# ---------------------------------------------------------------------------
# Seed lists (hardcoded - đảm bảo chính xác)
# ---------------------------------------------------------------------------
LANGUAGES = ["en", "es", "pt", "de", "fr"]

TEAMS = {
    # Premier League (tất cả 20 đội)
    "Arsenal_F.C.": "Arsenal", "Chelsea_F.C.": "Chelsea",
    "Liverpool_F.C.": "Liverpool", "Manchester_City_F.C.": "Manchester City",
    "Manchester_United_F.C.": "Manchester United",
    "Tottenham_Hotspur_F.C.": "Tottenham Hotspur",
    "Newcastle_United_F.C.": "Newcastle United",
    "Aston_Villa_F.C.": "Aston Villa", "Brighton_%26_Hove_Albion_F.C.": "Brighton",
    "West_Ham_United_F.C.": "West Ham United", "Fulham_F.C.": "Fulham",
    "Brentford_F.C.": "Brentford", "Crystal_Palace_F.C.": "Crystal Palace",
    "Wolverhampton_Wanderers_F.C.": "Wolverhampton", "Everton_F.C.": "Everton",
    "Nottingham_Forest_F.C.": "Nottingham Forest",
    "AFC_Bournemouth": "Bournemouth", "Leicester_City_F.C.": "Leicester City",
    "Ipswich_Town_F.C.": "Ipswich Town", "Southampton_F.C.": "Southampton",
    # La Liga
    "Real_Madrid_CF": "Real Madrid", "FC_Barcelona": "FC Barcelona",
    "Atlético_de_Madrid": "Atlético Madrid", "Sevilla_FC": "Sevilla",
    "Real_Betis": "Real Betis", "Athletic_Club": "Athletic Bilbao",
    "Villarreal_CF": "Villarreal", "Valencia_CF": "Valencia",
    # Bundesliga
    "FC_Bayern_Munich": "Bayern Munich", "Borussia_Dortmund": "Borussia Dortmund",
    "RB_Leipzig": "RB Leipzig", "Bayer_04_Leverkusen": "Bayer Leverkusen",
    "Eintracht_Frankfurt": "Eintracht Frankfurt",
    # Serie A
    "Inter_Milan": "Inter Milan", "A.C._Milan": "AC Milan",
    "Juventus_F.C.": "Juventus", "SSC_Napoli": "Napoli", "AS_Roma": "AS Roma",
    # Ligue 1
    "Paris_Saint-Germain_FC": "PSG", "Olympique_de_Marseille": "Marseille",
    "Olympique_Lyonnais": "Lyon", "AS_Monaco_FC": "Monaco",
    # Giải đấu
    "Premier_League": "Premier League", "La_Liga": "La Liga",
    "Serie_A": "Serie A", "Bundesliga": "Bundesliga", "Ligue_1": "Ligue 1",
    "UEFA_Champions_League": "Champions League", "FIFA_World_Cup": "FIFA World Cup",
    "UEFA_European_Championship": "European Championship",
}

SEED_PLAYERS = {
    # === PL Stars ===
    "Erling_Haaland": "Erling Haaland", "Mohamed_Salah": "Mohamed Salah",
    "Bukayo_Saka": "Bukayo Saka", "Son_Heung-min": "Heung-min Son",
    "Alexander_Isak": "Alexander Isak", "Ollie_Watkins": "Ollie Watkins",
    "Kevin_De_Bruyne": "Kevin De Bruyne", "Declan_Rice": "Declan Rice",
    "Martin_Ødegaard": "Martin Ødegaard", "Bernardo_Silva": "Bernardo Silva",
    "Bruno_Fernandes": "Bruno Fernandes", "Rodri_(footballer)": "Rodri",
    "Phil_Foden": "Phil Foden", "James_Maddison": "James Maddison",
    "Trent_Alexander-Arnold": "Trent Alexander-Arnold",
    "Virgil_van_Dijk": "Virgil van Dijk", "Rúben_Dias": "Rúben Dias",
    "Alisson_Becker": "Alisson", "Ederson_(footballer)": "Ederson",
    "Cole_Palmer": "Cole Palmer", "Kobbie_Mainoo": "Kobbie Mainoo",
    "Lamine_Yamal": "Lamine Yamal",
    # === World Stars ===
    "Lionel_Messi": "Lionel Messi", "Cristiano_Ronaldo": "Cristiano Ronaldo",
    "Kylian_Mbappé": "Kylian Mbappé", "Jude_Bellingham": "Jude Bellingham",
    "Vinícius_Júnior": "Vinícius Jr.", "Pedri": "Pedri",
    "Gavi_(footballer)": "Gavi", "Robert_Lewandowski": "Robert Lewandowski",
    "Harry_Kane": "Harry Kane", "Toni_Kroos": "Toni Kroos",
    "Luka_Modrić": "Luka Modrić", "Neymar": "Neymar",
    # === Managers ===
    "Pep_Guardiola": "Pep Guardiola", "Mikel_Arteta": "Mikel Arteta",
    "Thomas_Tuchel": "Thomas Tuchel", "Ange_Postecoglou": "Ange Postecoglou",
    "Unai_Emery": "Unai Emery", "Carlo_Ancelotti": "Carlo Ancelotti",
    "José_Mourinho": "José Mourinho", "Zinedine_Zidane": "Zinedine Zidane",
}


# ---------------------------------------------------------------------------
# Auto-discover: tự tạo Wikipedia title từ tên cầu thủ trong Silver fdo_players
# ---------------------------------------------------------------------------
def name_to_wiki_title(name: str) -> str:
    """
    Cố gắng chuyển tên cầu thủ thành Wikipedia article title.
    VD: "Erling Haaland" → "Erling_Haaland"
         "Virgil van Dijk" → "Virgil_van_Dijk"
    Không bỏ dấu vì Wikipedia giữ nguyên unicode.
    """
    return name.strip().replace(" ", "_")


def load_players_from_silver() -> dict:
    """
    Đọc bảng fdo_players từ MinIO (do p09 tạo ra).
    Trả về dict {wiki_title: player_name} cho TẤT CẢ cầu thủ.
    Nếu Silver chưa có thì trả về dict rỗng (dùng seed list).
    """
    import boto3
    from botocore.client import Config
    import io

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    BUCKET         = os.getenv("MINIO_BUCKET", "football-lake")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )

    players = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        prefix = "silver/dim/fdo_players/"
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                try:
                    resp = s3.get_object(Bucket=BUCKET, Key=obj["Key"])
                    df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
                    for _, row in df.iterrows():
                        pname = row.get("name", "")
                        if pname and isinstance(pname, str) and len(pname) > 2:
                            wiki_title = name_to_wiki_title(pname)
                            players[wiki_title] = pname
                except Exception:
                    continue
    except Exception as e:
        print(f"  ! Không đọc được fdo_players từ Silver: {e}")
    print(f"  · Loaded {len(players)} players from fdo_players Silver")
    return players


# ---------------------------------------------------------------------------
# API Helpers
# ---------------------------------------------------------------------------
def fetch_daily(article: str, lang: str = "en") -> dict | None:
    """
    Lấy time-series lượt xem 1 bài theo ngày.
    Trả về None nếu 404 (article không tồn tại).
    """
    url = (f"{BASE}/per-article/{lang}.wikipedia/all-access/user/"
           f"{article}/daily/{START}/{END}")
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()


def fetch_top(day: date, lang: str = "en") -> dict:
    url = (f"{BASE}/top/{lang}.wikipedia/all-access/"
           f"{day.year}/{day.month:02d}/{day.day:02d}")
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()


# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
def ingest_entities_multilang(entities: dict, entity_type: str,
                               lang_list: list = None) -> list:
    """
    Lấy lượt xem cho từng entity trên tất cả ngôn ngữ.
    Tự động bỏ qua nếu article không tồn tại (404).
    """
    if lang_list is None:
        lang_list = LANGUAGES
    results = []
    total = len(entities) * len(lang_list)
    done = 0
    for article, label in entities.items():
        found_any = False
        for lang in lang_list:
            body = fetch_daily(article, lang)
            done += 1
            if body is None:
                continue
            n = len(body.get("items", []))
            if n > 0:
                put_json_gz(
                    f"bronze/wikimedia_pageviews/per_article/entity={entity_type}"
                    f"/lang={lang}/article={article}/ingest_date={D}/daily.json.gz",
                    body, SRC, meta={"article": article, "lang": lang, "days": n})
                results.append((article, label, lang, body))
                found_any = True
        if found_any:
            print(f"  · {label}: ✓")
    return results


def ingest_top_daily_multilang(n_days: int = 7):
    """Lấy top viral n_days ngày gần nhất, tất cả ngôn ngữ."""
    for i in range(n_days):
        target = date.today() - timedelta(days=i+1)
        for lang in LANGUAGES:
            try:
                body = fetch_top(target, lang)
                put_json_gz(
                    f"bronze/wikimedia_pageviews/top_daily/lang={lang}"
                    f"/date={target.isoformat()}/top.json.gz",
                    body, SRC, meta={"date": target.isoformat(), "lang": lang})
            except Exception as e:
                print(f"    ! top {lang}/{target}: {e}")


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
def build_silver_timeseries(results: list, entity_type: str) -> pd.DataFrame:
    """Dựng time-series đầy đủ với nhiều feature phân tích."""
    rows = []
    for article, label, lang, body in results:
        for item in body.get("items", []):
            rows.append({
                "entity_type": entity_type,
                "article":     article,
                "label":       label,
                "lang":        lang,
                "date":        item["timestamp"][:8],
                "views":       item["views"],
            })
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
    df["year"]        = df["date"].dt.year
    df["month"]       = df["date"].dt.month
    df["week_of_year"]= df["date"].dt.isocalendar().week.astype(int)
    df["day_of_week"] = df["date"].dt.dayofweek  # 0=Mon, 6=Sun
    df["is_weekend"]  = df["day_of_week"].isin([5, 6]).astype(int)
    df["football_season"] = df["date"].apply(
        lambda d: f"{d.year}/{d.year+1}" if d.month >= 8 else f"{d.year-1}/{d.year}")

    # Rolling stats per (article, lang)
    df = df.sort_values(["article", "lang", "date"])
    grp = df.groupby(["article", "lang"])["views"]
    df["views_7d_avg"]  = grp.transform(lambda s: s.rolling(7,  min_periods=1).mean())
    df["views_28d_avg"] = grp.transform(lambda s: s.rolling(28, min_periods=1).mean())
    df["views_28d_std"] = grp.transform(lambda s: s.rolling(28, min_periods=5).std())
    df["zscore"]        = ((df["views"] - df["views_28d_avg"])
                           / df["views_28d_std"].replace(0, np.nan))

    # Spike: z-score >= 2 HOẶC cao hơn 3x baseline 7 ngày
    baseline_7 = grp.transform(lambda s: s.shift(1).rolling(7, min_periods=3).mean())
    df["spike_ratio_7d"] = df["views"] / baseline_7.replace(0, np.nan)
    df["is_spike"]       = (
        (df["zscore"] >= 2.0) | (df["spike_ratio_7d"] >= 3.0)
    ).astype(int)

    # Silver English-only (bảng chính)
    df_en = df[df["lang"] == "en"].drop(columns=["lang"])
    if not df_en.empty:
        put_parquet(
            f"silver/text/wm_pageviews/entity={entity_type}/part-0.parquet",
            df_en, SRC, meta={"entities": df_en.article.nunique(), "rows": len(df_en)})

    # Silver multi-lang
    put_parquet(
        f"silver/text/wm_pageviews_multilang/entity={entity_type}/part-0.parquet",
        df, SRC, meta={"entities": df.article.nunique(), "rows": len(df),
                       "langs": LANGUAGES})

    print(f"  ✓ {entity_type}: {df.article.nunique()} entities | "
          f"{len(df_en)} rows (en) | {len(df)} rows (all langs)")
    return df


def build_silver_spikes(team_df: pd.DataFrame, player_df: pd.DataFrame):
    dfs = [x for x in [team_df, player_df] if not x.empty]
    if not dfs:
        return
    spikes = pd.concat(dfs, ignore_index=True)
    spikes = spikes[spikes["is_spike"] == 1].sort_values("zscore", ascending=False)
    if not spikes.empty:
        put_parquet("silver/text/wm_pageview_spikes/part-0.parquet", spikes, SRC,
                    meta={"spike_rows": len(spikes)})
        print(f"  ✓ {len(spikes)} spike-events → silver/text/wm_pageview_spikes/")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def _slice(d: dict, n: int) -> dict:
    return dict(list(d.items())[:n])


def run_pipeline():
    print(f"[0/5] Auto-discover cầu thủ từ Silver fdo_players (p09)... (TEST_MODE={TEST_MODE})")
    auto_players = load_players_from_silver()

    # Gộp seed + auto-discovered, seed có thể ghi đè (title chính xác hơn)
    all_players = {**auto_players, **SEED_PLAYERS}
    print(f"  · Tổng players: {len(all_players)} "
          f"(seed: {len(SEED_PLAYERS)}, auto: {len(auto_players)})")

    # Test mode: giới hạn số lượng để chạy nhanh
    teams_run   = _slice(TEAMS,       3) if TEST_MODE else TEAMS
    players_run = _slice(all_players, 3) if TEST_MODE else all_players
    top_days    = 1                      if TEST_MODE else 7

    print(f"\n[1/5] pageviews đội bóng ({len(teams_run)} đội, multi-lang)")
    team_results = ingest_entities_multilang(teams_run, "team")

    print(f"\n[2/5] pageviews {len(players_run)} cầu thủ (multi-lang)")
    player_results = ingest_entities_multilang(players_run, "player")

    print(f"\n[3/5] top viral {top_days} ngày gần nhất (all langs)")
    ingest_top_daily_multilang(n_days=top_days)

    print("\n[4/5] silver: time-series + spike detection")
    team_df   = build_silver_timeseries(team_results,   "team")
    player_df = build_silver_timeseries(player_results, "player")

    print("\n[5/5] spike events")
    build_silver_spikes(team_df, player_df)

    print("\n[summary]")
    summary("bronze/wikimedia_pageviews/")
    summary("silver/text/wm_pageviews/")
    summary("silver/text/wm_pageviews_multilang/")


if __name__ == "__main__":
    run_pipeline()
