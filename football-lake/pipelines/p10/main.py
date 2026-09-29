"""
Ingest Wikimedia Pageviews (p10) - phiên bản tối đa.
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.
"""
import time
import os
import re
import unicodedata
from datetime import date, timedelta
import pandas as pd
from lake.minio_io import put_json_gz, exists, today, S3, BUCKET
from lake.http import SESSION

SRC = "wikimedia-pageviews"
D = today()
BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews"
UA = "football-lake/0.1 (educational project; contact: you@example.com)"
HEADERS = {"User-Agent": UA}

START = "20230101"   # yyyymmdd
END   = date.today().strftime("%Y%m%d")

TEST_MODE = os.getenv("TEST_MODE") == "1"

# ---------------------------------------------------------------------------
# Seed lists (hardcoded - đảm bảo chính xác)
# ---------------------------------------------------------------------------
LANGUAGES = ["en", "es", "pt", "de", "fr"]

TEAMS = {
    # Premier League
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

def name_to_wiki_title(name: str) -> str:
    return name.strip().replace(" ", "_")

def load_players_from_silver() -> dict:
    import boto3
    import io

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    MINIO_BUCKET   = os.getenv("MINIO_BUCKET", "football-lake")

    s3 = boto3.client(
        "s3", endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS, aws_secret_access_key=MINIO_SECRET,
    )

    players = {}
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix="silver/dim/fdo_players/"):
            for obj in page.get("Contents", []):
                try:
                    resp = s3.get_object(Bucket=MINIO_BUCKET, Key=obj["Key"])
                    df = pd.read_parquet(io.BytesIO(resp["Body"].read()))
                    for _, row in df.iterrows():
                        pname = row.get("name", "")
                        if pname and isinstance(pname, str) and len(pname) > 2:
                            wiki_title = name_to_wiki_title(pname)
                            players[wiki_title] = pname
                except Exception:
                    continue
    except Exception as e:
        print(f"  ! Lỗi đọc fdo_players từ Silver: {e}")
    return players

def _slice(d: dict, n: int) -> dict:
    return dict(list(d.items())[:n])

def get_partitions() -> list[dict]:
    auto_players = load_players_from_silver()
    all_players = {**auto_players, **SEED_PLAYERS}
    
    teams_run   = _slice(TEAMS, 3) if TEST_MODE else TEAMS
    players_run = _slice(all_players, 3) if TEST_MODE else all_players
    top_days    = 1 if TEST_MODE else 7

    partitions = []
    
    # 1. Pageviews cho Team
    for article, label in teams_run.items():
        for lang in LANGUAGES:
            partitions.append({"type": "team", "article": article, "label": label, "lang": lang})
            
    # 2. Pageviews cho Player
    for article, label in players_run.items():
        for lang in LANGUAGES:
            partitions.append({"type": "player", "article": article, "label": label, "lang": lang})
            
    # 3. Top daily viral
    for i in range(top_days):
        target = date.today() - timedelta(days=i+1)
        for lang in LANGUAGES:
            partitions.append({"type": "top_daily", "date": target.isoformat(), "lang": lang})
            
    return partitions

def fetch_daily(article: str, lang: str = "en") -> dict | None:
    url = f"{BASE}/per-article/{lang}.wikipedia/all-access/user/{article}/daily/{START}/{END}"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()

def fetch_top(target_date: str, lang: str = "en") -> dict:
    day = date.fromisoformat(target_date)
    url = f"{BASE}/top/{lang}.wikipedia/all-access/{day.year}/{day.month:02d}/{day.day:02d}"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    time.sleep(0.3)
    return r.json()

def ingest(partition: dict) -> str:
    ptype = partition["type"]
    lang = partition["lang"]
    
    if ptype in ("team", "player"):
        article = partition["article"]
        key = f"bronze/wikimedia_pageviews/per_article/entity={ptype}/lang={lang}/article={article}/ingest_date={D}/daily.json.gz"
        if exists(key):
            return key
            
        body = fetch_daily(article, lang)
        if body:
            n = len(body.get("items", []))
            put_json_gz(key, body, SRC, meta={"article": article, "lang": lang, "days": n})
        return key
        
    elif ptype == "top_daily":
        target_date = partition["date"]
        key = f"bronze/wikimedia_pageviews/top_daily/lang={lang}/date={target_date}/top.json.gz"
        if exists(key):
            return key
            
        body = fetch_top(target_date, lang)
        put_json_gz(key, body, SRC, meta={"date": target_date, "lang": lang})
        return key
