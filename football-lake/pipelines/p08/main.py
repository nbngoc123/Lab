"""
Ingest TheSportsDB (p08): metadata JSON + ảnh nhị phân + manifest.
Mở rộng: cào tất cả giải bóng đá lớn châu Âu + Nam Mỹ.
TheSportsDB free (key=3): không giới hạn request nhưng nên sleep nhẹ.
"""
import hashlib
import os
import time
from urllib.parse import urlparse

import pandas as pd
from lake.minio_io import (put_json_gz, put_bytes, put_parquet,
                           exists, today, summary, S3, BUCKET)
from lake.http import SESSION

SRC  = "thesportsdb"
D    = today()
KEY  = os.getenv("THESPORTSDB_KEY", "3")
BASE = f"https://www.thesportsdb.com/api/v1/json/{KEY}"

# ── Danh sách giải đấu cần lấy (tên chính xác theo TheSportsDB) ──────────────
LEAGUES = [
    # Châu Âu
    {"name": "English Premier League",       "slug": "EPL"},
    {"name": "English League Championship",  "slug": "Championship"},
    {"name": "Spanish La Liga",              "slug": "LaLiga"},
    {"name": "German Bundesliga",            "slug": "Bundesliga"},
    {"name": "Italian Serie A",              "slug": "SerieA"},
    {"name": "French Ligue 1",               "slug": "Ligue1"},
    {"name": "Dutch Eredivisie",             "slug": "Eredivisie"},
    {"name": "Portuguese Primeira Liga",     "slug": "PrimeiraLiga"},
    {"name": "Belgian Pro League",           "slug": "BelgianPro"},
    {"name": "Scottish Premiership",         "slug": "ScottishPrem"},
    # Liên lục địa / Cup
    {"name": "UEFA Champions League",        "slug": "UCL"},
    {"name": "UEFA Europa League",           "slug": "UEL"},
    # Nam Mỹ
    {"name": "Brazilian Serie A",            "slug": "SerieA_BR"},
    {"name": "Argentine Primera Division",   "slug": "PrimeraDivision"},
    # Quốc tế
    {"name": "FIFA World Cup",               "slug": "WorldCup"},
]

# field ảnh đội -> tên file lưu lake
TEAM_IMAGES = {
    "strBadge":        "badge",
    "strLogo":         "logo",
    "strBanner":       "banner",
    "strEquipment":    "equipment",
    "strStadiumThumb": "stadium_thumb",
    "strFanart1":      "fanart1",
}
PLAYER_IMAGES = {
    "strThumb":  "thumb",
    "strCutout": "cutout",
    "strRender": "render",
}

MIME = {".png": "image/png", ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg", ".webp": "image/webp",
        ".gif": "image/gif"}


# ── Tải 1 file ảnh (idempotent) ───────────────────────────────────────────────
def fetch_image(url: str, s3_key: str, entity: str,
                entity_id: str, image_role: str) -> dict | None:
    if not url or not url.startswith("http"):
        return None

    if exists(s3_key):
        try:
            head = S3.head_object(Bucket=BUCKET, Key=s3_key)
            return {
                "s3_key": s3_key, "entity": entity,
                "entity_id": str(entity_id), "image_role": image_role,
                "content_type": head.get("ContentType", "image/jpeg"),
                "size_bytes": head.get("ContentLength", 0),
                "sha256": "cached", "source_url": url, "ingest_date": D,
            }
        except Exception:
            return None

    try:
        r = SESSION.get(url, timeout=60)
        r.raise_for_status()
    except Exception as e:
        print(f"    ! không tải {image_role} {entity_id}: {e}")
        return None

    data  = r.content
    ext   = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
    ctype = MIME.get(ext, r.headers.get("Content-Type", "image/jpeg"))
    put_bytes(s3_key, data, SRC, content_type=ctype,
              meta={"entity": entity, "entity_id": entity_id,
                    "role": image_role, "source_url": url})
    time.sleep(0.3)
    return {
        "s3_key": s3_key, "entity": entity,
        "entity_id": str(entity_id), "image_role": image_role,
        "content_type": ctype, "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "source_url": url, "ingest_date": D,
    }


# ── BRONZE: metadata JSON ─────────────────────────────────────────────────────
def ingest_teams_for_league(league_name: str, league_slug: str) -> list:
    """Lấy tất cả đội của 1 giải."""
    key = (f"bronze/thesportsdb/metadata/entity=teams"
           f"/league={league_slug}/ingest_date={D}/teams.json.gz")
    if exists(key):
        print(f"    · {league_slug}: teams đã cache")
        from lake.minio_io import read_json_gz
        body = read_json_gz(key)
        return body.get("teams") or []

    r    = SESSION.get(f"{BASE}/search_all_teams.php", params={"l": league_name})
    body = r.json()
    teams = body.get("teams") or []
    if teams:
        put_json_gz(key, body, SRC, meta={"league": league_slug, "teams": len(teams)})
    print(f"    · {league_slug}: {len(teams)} đội")
    time.sleep(0.5)
    return teams


def ingest_players_for_team(team_id: str) -> list:
    """Lấy cầu thủ của 1 đội."""
    r    = SESSION.get(f"{BASE}/lookup_all_players.php", params={"id": team_id})
    body = r.json()
    players = body.get("player") or []
    if players:
        put_json_gz(
            f"bronze/thesportsdb/metadata/entity=players"
            f"/team_id={team_id}/ingest_date={D}/players.json.gz",
            body, SRC, meta={"team_id": team_id, "players": len(players)})
    time.sleep(0.4)
    return players


# ── BRONZE: binary ────────────────────────────────────────────────────────────
def ingest_team_media(teams: list, league_slug: str) -> list:
    manifest = []
    n = 0
    for t in teams:
        tid = t["idTeam"]
        for field, role in TEAM_IMAGES.items():
            url = t.get(field)
            if not url:
                continue
            ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
            key = (f"bronze/thesportsdb/media/entity=team"
                   f"/team_id={tid}/{role}{ext}")
            mf = fetch_image(url, key, "team", tid, role)
            if mf:
                manifest.append(mf)
                n += 1
    print(f"    · {league_slug}: {len(teams)} đội, {n} ảnh đội")
    return manifest


def ingest_players_and_media(teams: list) -> list:
    manifest = []
    for t in teams:
        players = ingest_players_for_team(t["idTeam"])
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
                key = (f"bronze/thesportsdb/media/entity=player"
                       f"/player_id={pid}/{role}{ext}")
                mf = fetch_image(url, key, "player", pid, role)
                if mf:
                    manifest.append(mf)
                    n += 1
        print(f"      · {t.get('strTeam')}: {len(players)} cầu thủ, {n} ảnh")
    return manifest


# ── SILVER ────────────────────────────────────────────────────────────────────
def build_teams_dim(all_teams: list):
    """Gộp tất cả đội tất cả giải -> 1 bảng dim."""
    rows = []
    for t in all_teams:
        rows.append({
            "team_id":           t.get("idTeam"),
            "team_name":         t.get("strTeam"),
            "team_short":        t.get("strTeamShort"),
            "alternate_names":   t.get("strTeamAlternate"),
            "formed_year":       pd.to_numeric(t.get("intFormedYear"), errors="coerce"),
            "league":            t.get("strLeague"),
            "country":           t.get("strCountry"),
            "stadium":           t.get("strStadium"),
            "stadium_capacity":  pd.to_numeric(t.get("intStadiumCapacity"), errors="coerce"),
            "stadium_location":  t.get("strLocation"),
            "website":           t.get("strWebsite"),
            "facebook":          t.get("strFacebook"),
            "twitter":           t.get("strTwitter"),
            "instagram":         t.get("strInstagram"),
            "description_en":    (t.get("strDescriptionEN") or "")[:2000],
            "gender":            t.get("strGender"),
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["team_id"])
    if not df.empty:
        put_parquet(f"silver/dim/tsdb_teams/ingest_date={D}/part-0.parquet",
                    df, SRC, meta={"rows": len(df),
                                   "leagues": df["league"].nunique()})
        print(f"  ✓ dim teams: {len(df)} đội từ {df['league'].nunique()} giải")
    return df


def write_manifest(manifest_list: list):
    if not manifest_list:
        print("  ! manifest rỗng")
        return pd.DataFrame()
    df = pd.DataFrame(manifest_list).drop_duplicates(subset=["s3_key"])
    put_parquet(f"_meta/thesportsdb/media_manifest/ingest_date={D}/manifest.parquet",
                df, SRC)
    put_parquet(f"silver/dim/tsdb_media/ingest_date={D}/part-0.parquet", df, SRC)
    total_mb = df["size_bytes"].sum() / 1024 / 1024
    print(f"  ✓ manifest: {len(df)} file, tổng {total_mb:.1f} MB")
    print(df.groupby(["entity", "image_role"]).size().to_string())
    return df


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run_pipeline():
    print(f"=== p08: TheSportsDB Multi-League ({len(LEAGUES)} giải) ===\n")

    all_teams    = []
    all_manifest = []

    for lg in LEAGUES:
        name = lg["name"]
        slug = lg["slug"]
        print(f"\n[League] {slug} — {name}")

        # 1. Lấy teams
        teams = ingest_teams_for_league(name, slug)
        if not teams:
            print(f"    ! Không có đội nào (giải có thể cần API key trả phí)")
            continue
        all_teams.extend(teams)

        # 2. Media đội
        manifest = ingest_team_media(teams, slug)
        all_manifest.extend(manifest)

        # 3. Cầu thủ + ảnh cầu thủ
        print(f"    · Cào cầu thủ {slug}...")
        player_manifest = ingest_players_and_media(teams)
        all_manifest.extend(player_manifest)

    # Silver
    print("\n[Silver] Build dim tables...")
    build_teams_dim(all_teams)
    write_manifest(all_manifest)

    print("\n[Summary]")
    summary("bronze/thesportsdb/")
    summary("silver/dim/tsdb_")


if __name__ == "__main__":
    run_pipeline()
