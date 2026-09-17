"""Ingest TheSportsDB: metadata JSON + file ảnh nhị phân + manifest."""
import hashlib
import os
import time
from urllib.parse import urlparse

import pandas as pd
from lake.minio_io import (put_json_gz, put_bytes, put_parquet,
                           exists, today, summary, S3, BUCKET)
from lake.http import SESSION

SRC = "thesportsdb"
D = today()
KEY = os.getenv("THESPORTSDB_KEY", "3")
BASE = f"https://www.thesportsdb.com/api/v1/json/{KEY}"
LEAGUE = "English Premier League"

# field ảnh -> tên file lưu trong lake
TEAM_IMAGES = {
    "strBadge": "badge",
    "strLogo": "logo",
    "strBanner": "banner",
    "strEquipment": "equipment",
    "strStadiumThumb": "stadium_thumb",
    "strFanart1": "fanart1",
}
PLAYER_IMAGES = {
    "strThumb": "thumb",
    "strCutout": "cutout",
    "strRender": "render",
}

MIME = {".png": "image/png", ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg", ".webp": "image/webp",
        ".gif": "image/gif"}


# ---------- tải 1 file ảnh ----------
def fetch_image(url: str, s3_key: str, entity: str, entity_id: str,
                image_role: str) -> dict | None:
    if not url or not url.startswith("http"):
        return None
        
    # Idempotent: check if exists
    if exists(s3_key):
        # We need the sha256 to reconstruct manifest if it exists?
        # If it exists but we don't have manifest, we'd have to read the object.
        # But for simplicity, we just fetch it again if we want to ensure manifest is correct,
        # or we read the existing object metadata.
        # However, to avoid high API calls, we return None and let a separate script handle full sync if needed.
        # Actually, let's fetch object metadata to construct manifest if it already exists.
        try:
            head = S3.head_object(Bucket=BUCKET, Key=s3_key)
            return {
                "s3_key": s3_key,
                "entity": entity,
                "entity_id": str(entity_id),
                "image_role": image_role,
                "content_type": head.get("ContentType", "image/jpeg"),
                "size_bytes": head.get("ContentLength", 0),
                "sha256": "existing_file", # Dummy hash to save time since we didn't download
                "source_url": url,
                "ingest_date": D,
            }
        except Exception:
            return None

    try:
        r = SESSION.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"    ! không tải được {image_role} của {entity_id}: {e}")
        return None

    data = r.content
    ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
    ctype = MIME.get(ext, r.headers.get("Content-Type", "image/jpeg"))

    put_bytes(s3_key, data, SRC, content_type=ctype,
              meta={"entity": entity, "entity_id": entity_id,
                    "role": image_role, "source_url": url})
                    
    time.sleep(0.3)
    
    return {
        "s3_key": s3_key,
        "entity": entity,
        "entity_id": str(entity_id),
        "image_role": image_role,
        "content_type": ctype,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "source_url": url,
        "ingest_date": D,
    }


# ---------- BRONZE: metadata ----------
def ingest_teams() -> list:
    r = SESSION.get(f"{BASE}/search_all_teams.php", params={"l": LEAGUE})
    body = r.json()
    teams = body.get("teams") or []
    put_json_gz(
        f"bronze/thesportsdb/metadata/entity=teams/league=EPL"
        f"/ingest_date={D}/teams.json.gz",
        body, SRC, meta={"teams": len(teams)})
    print(f"  ✓ {len(teams)} đội")
    return teams


def ingest_players(team_id: str) -> list:
    r = SESSION.get(f"{BASE}/lookup_all_players.php", params={"id": team_id})
    body = r.json()
    players = body.get("player") or []
    if players:
        put_json_gz(
            f"bronze/thesportsdb/metadata/entity=players/team_id={team_id}"
            f"/ingest_date={D}/players.json.gz",
            body, SRC, meta={"team_id": team_id, "players": len(players)})
    return players


# ---------- BRONZE: binary ----------
def ingest_team_media(teams: list) -> list:
    manifest = []
    n = 0
    for t in teams:
        tid = t["idTeam"]
        for field, role in TEAM_IMAGES.items():
            url = t.get(field)
            if not url:
                continue
            ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
            key = (f"bronze/thesportsdb/media/entity=team/team_id={tid}"
                   f"/{role}{ext}")
            mf = fetch_image(url, key, "team", tid, role)
            if mf:
                manifest.append(mf)
                n += 1
        print(f"  · {t.get('strTeam')}: xong đội")
    print(f"  ✓ tải {n} ảnh đội")
    return manifest


def ingest_players_and_media(teams: list) -> list:
    manifest = []
    for t in teams:
        players = ingest_players(t["idTeam"])
        if not players:
            continue
            
        n = 0
        for p in players:
            pid = p.get("idPlayer")
            if not pid:
                continue
            for field, role in PLAYER_IMAGES.items():
                url = p.get(field)
                if not url:
                    continue
                ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
                key = (f"bronze/thesportsdb/media/entity=player/player_id={pid}"
                       f"/{role}{ext}")
                mf = fetch_image(url, key, "player", pid, role)
                if mf:
                    manifest.append(mf)
                    n += 1
        print(f"    · {t.get('strTeam')}: {len(players)} cầu thủ, {n} ảnh")
        time.sleep(0.5)
    return manifest


# ---------- SILVER ----------
def build_teams_dim(teams: list):
    rows = []
    for t in teams:
        rows.append({
            "team_id": t.get("idTeam"),
            "team_name": t.get("strTeam"),
            "team_short": t.get("strTeamShort"),
            "alternate_names": t.get("strTeamAlternate"),
            "formed_year": pd.to_numeric(t.get("intFormedYear"),
                                         errors="coerce"),
            "league": t.get("strLeague"),
            "stadium": t.get("strStadium"),
            "stadium_capacity": pd.to_numeric(t.get("intStadiumCapacity"),
                                              errors="coerce"),
            "stadium_location": t.get("strLocation"),
            "country": t.get("strCountry"),
            "website": t.get("strWebsite"),
            "facebook": t.get("strFacebook"),
            "twitter": t.get("strTwitter"),
            "instagram": t.get("strInstagram"),
            "description_en": (t.get("strDescriptionEN") or "")[:2000],
            "gender": t.get("strGender"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        put_parquet(f"silver/dim/tsdb_teams/ingest_date={D}/part-0.parquet", df, SRC)
        print(f"  ✓ dim đội: {len(df)} dòng")
    return df


def write_manifest(manifest_list: list):
    """Bảng tra cứu mọi file nhị phân — làm cho ảnh trở nên query-được."""
    if not manifest_list:
        print("  ! manifest rỗng")
        return pd.DataFrame()
    df = pd.DataFrame(manifest_list)
    # Deduplicate in case of multiple runs appending same keys
    df = df.drop_duplicates(subset=["s3_key"])
    
    put_parquet(f"_meta/thesportsdb/media_manifest/ingest_date={D}/manifest.parquet",
                df, SRC)
    put_parquet(f"silver/dim/tsdb_media/ingest_date={D}/part-0.parquet",
                df, SRC)
    print(f"  ✓ manifest: {len(df)} file, "
          f"tổng {df.size_bytes.sum()/1024/1024:.1f} MB")
    print(df.groupby("image_role").size().to_string())
    return df
