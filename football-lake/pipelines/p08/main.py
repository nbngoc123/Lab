"""
Ingest TheSportsDB (p08): metadata JSON + ảnh nhị phân + manifest.
Mở rộng: cào tất cả giải bóng đá lớn châu Âu + Nam Mỹ + Honors + Former Teams + Equipment.
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

LEAGUES = [
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
    {"name": "UEFA Champions League",        "slug": "UCL"},
    {"name": "UEFA Europa League",           "slug": "UEL"},
    {"name": "Brazilian Serie A",            "slug": "SerieA_BR"},
    {"name": "Argentine Primera Division",   "slug": "PrimeraDivision"},
    {"name": "FIFA World Cup",               "slug": "WorldCup"},
]

TEAM_IMAGES = {
    "strBadge": "badge", "strLogo": "logo", "strBanner": "banner",
    "strEquipment": "equipment", "strStadiumThumb": "stadium_thumb",
    "strFanart1": "fanart1",
}
PLAYER_IMAGES = {
    "strThumb": "thumb", "strCutout": "cutout", "strRender": "render",
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


# ── BRONZE ────────────────────────────────────────────────────────────────────
def ingest_teams_for_league(league_name: str, league_slug: str) -> list:
    key = (f"bronze/thesportsdb/metadata/entity=teams"
           f"/league={league_slug}/ingest_date={D}/teams.json.gz")
    if exists(key):
        from lake.minio_io import read_json_gz
        return read_json_gz(key).get("teams") or []
    try:
        r = SESSION.get(f"{BASE}/search_all_teams.php", params={"l": league_name})
        r.raise_for_status()
        body = r.json()
    except Exception as e:
        print(f"    ! Lỗi API tải teams cho giải {league_slug}: {e}")
        return []
    teams = body.get("teams") or []
    if teams:
        put_json_gz(key, body, SRC, meta={"league": league_slug, "teams": len(teams)})
    time.sleep(0.5)
    return teams


def ingest_players_for_team(team_id: str) -> list:
    key = f"bronze/thesportsdb/metadata/entity=players/team_id={team_id}/ingest_date={D}/players.json.gz"
    if exists(key):
        from lake.minio_io import read_json_gz
        return read_json_gz(key).get("player") or []
    try:
        r = SESSION.get(f"{BASE}/lookup_all_players.php", params={"id": team_id})
        r.raise_for_status()
        body = r.json()
    except Exception as e:
        print(f"    ! Lỗi API tải players cho team {team_id}: {e}")
        return []
    players = body.get("player") or []
    if players:
        put_json_gz(key, body, SRC, meta={"team_id": team_id, "players": len(players)})
    time.sleep(0.3)
    return players


def ingest_player_extras(player_id: str):
    """Cào Honors và Former Teams của 1 cầu thủ"""
    # 1. Honors
    hk = f"bronze/thesportsdb/metadata/entity=honors/player_id={player_id}/ingest_date={D}/honors.json.gz"
    if not exists(hk):
        try:
            r = SESSION.get(f"{BASE}/lookuphonors.php", params={"id": player_id})
            body = r.json()
            if body.get("honors"):
                put_json_gz(hk, body, SRC)
            time.sleep(0.3)
        except Exception: pass

    # 2. Former Teams
    fk = f"bronze/thesportsdb/metadata/entity=former_teams/player_id={player_id}/ingest_date={D}/former_teams.json.gz"
    if not exists(fk):
        try:
            r = SESSION.get(f"{BASE}/lookupformerteams.php", params={"id": player_id})
            body = r.json()
            if body.get("formerteams"):
                put_json_gz(fk, body, SRC)
            time.sleep(0.3)
        except Exception: pass


def ingest_team_equipment(team_id: str) -> list:
    """Cào danh sách áo đấu và tải ảnh nhị phân"""
    manifest = []
    ek = f"bronze/thesportsdb/metadata/entity=equipment/team_id={team_id}/ingest_date={D}/equipment.json.gz"
    
    if exists(ek):
        from lake.minio_io import read_json_gz
        body = read_json_gz(ek)
    else:
        try:
            r = SESSION.get(f"{BASE}/lookupequipment.php", params={"id": team_id})
            body = r.json()
            if body.get("equipment"):
                put_json_gz(ek, body, SRC)
            time.sleep(0.3)
        except Exception:
            return []

    equip = body.get("equipment") or []
    for eq in equip:
        url = eq.get("strEquipment")
        if not url: continue
        season = eq.get("strSeason", "unknown")
        type_  = eq.get("strType", "home").lower().replace(" ", "_")
        ext    = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
        key = f"bronze/thesportsdb/media/entity=equipment/team_id={team_id}/season={season}_{type_}{ext}"
        mf = fetch_image(url, key, "equipment", team_id, f"{season}_{type_}")
        if mf: manifest.append(mf)
    return manifest


def ingest_team_media(teams: list) -> list:
    manifest = []
    for t in teams:
        tid = t["idTeam"]
        for field, role in TEAM_IMAGES.items():
            url = t.get(field)
            if not url: continue
            ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
            key = f"bronze/thesportsdb/media/entity=team/team_id={tid}/{role}{ext}"
            mf = fetch_image(url, key, "team", tid, role)
            if mf: manifest.append(mf)
            
        # Áo đấu các mùa
        eq_mf = ingest_team_equipment(tid)
        manifest.extend(eq_mf)
    return manifest


def ingest_players_and_media(teams: list) -> list:
    manifest = []
    for t in teams:
        players = ingest_players_for_team(t["idTeam"])
        if not players: continue
        n = 0
        for p in players:
            pid = p.get("idPlayer")
            if not pid: continue
            
            # Metadata phụ
            ingest_player_extras(pid)
            
            # Ảnh
            for field, role in PLAYER_IMAGES.items():
                url = p.get(field)
                if not url: continue
                ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
                key = f"bronze/thesportsdb/media/entity=player/player_id={pid}/{role}{ext}"
                mf = fetch_image(url, key, "player", pid, role)
                if mf:
                    manifest.append(mf)
                    n += 1
        print(f"      · {t.get('strTeam')}: {len(players)} cầu thủ, {n} ảnh")
    return manifest


# ── SILVER ────────────────────────────────────────────────────────────────────
def build_teams_dim(all_teams: list):
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
            "description_en":    (t.get("strDescriptionEN") or "")[:2000],
        })
    df = pd.DataFrame(rows).drop_duplicates(subset=["team_id"])
    if not df.empty:
        put_parquet(f"silver/dim/tsdb_teams/ingest_date={D}/part-0.parquet", df, SRC)
    return df


def build_players_dim():
    """Đọc từ Bronze build metadata cầu thủ chi tiết (Lương, Hợp đồng, ID chéo...)"""
    from lake.minio_io import list_objects, read_json_gz
    keys = list_objects(f"bronze/thesportsdb/metadata/entity=players/")
    rows = []
    for k in keys:
        if k.endswith(".json.gz"):
            try:
                data = read_json_gz(k)
                for p in data.get("player", []):
                    rows.append({
                        "player_id": p.get("idPlayer"),
                        "team_id": p.get("idTeam"),
                        "player_name": p.get("strPlayer"),
                        "nationality": p.get("strNationality"),
                        "birth_date": p.get("dateBorn"),
                        "wage": p.get("strWage"),
                        "date_signed": p.get("dateSigned"),
                        "signing_fee": p.get("strSigning"),
                        "outfitter": p.get("strOutfitter"),
                        "agent": p.get("strAgent"),
                        "position": p.get("strPosition"),
                        "height": p.get("strHeight"),
                        "weight": p.get("strWeight"),
                        "wikidata_id": p.get("idWikidata"),
                        "transfermarkt_id": p.get("idTransferMkt"),
                        "espn_id": p.get("idESPN"),
                    })
            except Exception: pass
            
    df = pd.DataFrame(rows).drop_duplicates(subset=["player_id"])
    if not df.empty:
        put_parquet(f"silver/dim/tsdb_players/ingest_date={D}/part-0.parquet", df, SRC)
        print(f"  ✓ dim players: {len(df)} cầu thủ")
    return df


def build_player_honors_former_teams():
    from lake.minio_io import list_objects, read_json_gz
    
    # Honors
    keys = list_objects(f"bronze/thesportsdb/metadata/entity=honors/")
    rows = []
    for k in keys:
        if k.endswith(".json.gz"):
            try:
                for h in read_json_gz(k).get("honors", []):
                    rows.append({
                        "player_id": h.get("idPlayer"),
                        "honor": h.get("strHonour"),
                        "season": h.get("strSeason")
                    })
            except Exception: pass
    df_h = pd.DataFrame(rows).drop_duplicates()
    if not df_h.empty:
        put_parquet(f"silver/dim/tsdb_player_honors/ingest_date={D}/part-0.parquet", df_h, SRC)
        print(f"  ✓ dim honors: {len(df_h)} danh hiệu")

    # Former Teams
    keys = list_objects(f"bronze/thesportsdb/metadata/entity=former_teams/")
    rows = []
    for k in keys:
        if k.endswith(".json.gz"):
            try:
                for f in read_json_gz(k).get("formerteams", []):
                    rows.append({
                        "player_id": f.get("idPlayer"),
                        "former_team": f.get("strFormerTeam"),
                        "start_year": f.get("strJoined"),
                        "end_year": f.get("strDeparted")
                    })
            except Exception: pass
    df_f = pd.DataFrame(rows).drop_duplicates()
    if not df_f.empty:
        put_parquet(f"silver/dim/tsdb_player_former_teams/ingest_date={D}/part-0.parquet", df_f, SRC)
        print(f"  ✓ dim former teams: {len(df_f)} lịch sử CLB")


def write_manifest(manifest_list: list):
    if not manifest_list: return
    df = pd.DataFrame(manifest_list).drop_duplicates(subset=["s3_key"])
    put_parquet(f"_meta/thesportsdb/media_manifest/ingest_date={D}/manifest.parquet", df, SRC)
    put_parquet(f"silver/dim/tsdb_media/ingest_date={D}/part-0.parquet", df, SRC)
    total_mb = df["size_bytes"].sum() / 1024 / 1024
    print(f"  ✓ manifest: {len(df)} file ảnh, tổng {total_mb:.1f} MB")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def run_pipeline():
    print(f"=== p08: TheSportsDB Multi-League Full Data ({len(LEAGUES)} giải) ===\n")
    all_teams    = []
    all_manifest = []

    for lg in LEAGUES:
        name = lg["name"]
        slug = lg["slug"]
        print(f"\n[League] {slug} — {name}")

        teams = ingest_teams_for_league(name, slug)
        if not teams: continue
        all_teams.extend(teams)

        manifest = ingest_team_media(teams, slug)
        all_manifest.extend(manifest)

        print(f"    · Cào cầu thủ, hợp đồng, cúp, CLB cũ {slug}...")
        player_manifest = ingest_players_and_media(teams)
        all_manifest.extend(player_manifest)

    print("\n[Silver] Build dim tables...")
    build_teams_dim(all_teams)
    build_players_dim()
    build_player_honors_former_teams()
    write_manifest(all_manifest)

    print("\n[Summary]")
    summary("bronze/thesportsdb/")
    summary("silver/dim/tsdb_")


if __name__ == "__main__":
    run_pipeline()
