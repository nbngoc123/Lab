# 08 — TheSportsDB: Binary (ảnh) + metadata → MinIO

**Kiểu ingest:** REST API (metadata JSON) + tải file nhị phân (ảnh PNG/JPG)
**Tần suất:** 1 lần/mùa, refresh khi có đội/cầu thủ mới
**Độ khó:** ★★☆☆☆ — dạy đúng thế mạnh của object storage so với warehouse

---

## 1. Vì sao chọn nguồn này

Bảy nguồn kia đều là text hoặc số. Nguồn này là **binary**. Đây là lý do tồn tại của object storage: warehouse không lưu được ảnh, lake thì có.

Nội dung: logo CLB, áo đấu, ảnh sân vận động, ảnh chân dung cầu thủ, banner — kèm metadata mô tả (năm thành lập, sân nhà, mô tả CLB nhiều ngôn ngữ, link mạng xã hội).

Ứng dụng thật: dashboard BI có logo đội, model computer vision nhận diện áo đấu, hoặc đơn giản là chứng minh lake của bạn xử lý được **mọi loại dữ liệu** chứ không chỉ bảng.

## 2. API

Base (free/test key): `https://www.thesportsdb.com/api/v1/json/3/`
Key `3` là test key công khai, đủ dùng cho học tập. Đăng ký Patreon để có key riêng nếu cần quota cao.

| Endpoint | Nội dung |
|---|---|
| `search_all_teams.php?l=English%20Premier%20League` | 20 đội PL + toàn bộ URL ảnh |
| `lookup_all_players.php?id={team_id}` | Cầu thủ của 1 đội (có thể trống với key test) |
| `lookupteam.php?id={team_id}` | Chi tiết 1 đội |
| `searchplayers.php?p={name}` | Tìm cầu thủ theo tên |
| `all_leagues.php` | Toàn bộ giải đấu |

Các field ảnh trong record đội: `strBadge` (logo), `strLogo`, `strBanner`, `strFanart1..4`, `strEquipment` (áo đấu), `strStadiumThumb`.
Với cầu thủ: `strThumb`, `strCutout`, `strRender`, `strFanart1..4`.

## 3. Layout trong lake

Đây là điểm quan trọng: **binary và metadata tách riêng, nối nhau bằng key**.

```
bronze/thesportsdb/metadata/entity=teams/league=EPL/ingest_date=2026-09-16/teams.json.gz
bronze/thesportsdb/metadata/entity=players/team_id=133604/ingest_date=2026-09-16/players.json.gz

bronze/thesportsdb/media/entity=team/team_id=133604/badge.png
bronze/thesportsdb/media/entity=team/team_id=133604/logo.png
bronze/thesportsdb/media/entity=team/team_id=133604/stadium_thumb.jpg
bronze/thesportsdb/media/entity=team/team_id=133604/equipment.png
bronze/thesportsdb/media/entity=player/player_id=34145937/thumb.jpg

_meta/thesportsdb/media_manifest/ingest_date=2026-09-16/manifest.parquet

silver/dim/tsdb_teams/ingest_date=2026-09-16/part-0.parquet
silver/dim/tsdb_media/ingest_date=2026-09-16/part-0.parquet
```

**Manifest** là mảnh ghép then chốt: một bảng Parquet liệt kê mọi file nhị phân với kích thước, kiểu MIME, checksum và khóa S3. Không có nó, ảnh trong lake chỉ là đống file không truy vấn được.

## 4. Script ingest — `pipelines/p08_thesportsdb.py`

```python
"""Ingest TheSportsDB: metadata JSON + file ảnh nhị phân + manifest."""
import hashlib
import os
import time
from urllib.parse import urlparse

import pandas as pd
from lake.minio_io import (put_json_gz, put_bytes, put_parquet,
                           exists, today, summary, S3, BUCKET)
from lake.http import get

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

_manifest = []      # gom lại để ghi 1 lần cuối


# ---------- tải 1 file ảnh ----------
def fetch_image(url: str, s3_key: str, entity: str, entity_id: str,
                image_role: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    if exists(s3_key):
        return False                      # idempotent: đã có thì bỏ qua

    try:
        r = get(url, timeout=60)
    except Exception as e:
        print(f"    ! không tải được {image_role} của {entity_id}: {e}")
        return False

    data = r.content
    ext = os.path.splitext(urlparse(url).path)[1].lower() or ".jpg"
    ctype = MIME.get(ext, r.headers.get("Content-Type", "image/jpeg"))

    put_bytes(s3_key, data, SRC, content_type=ctype,
              meta={"entity": entity, "entity_id": entity_id,
                    "role": image_role, "source_url": url})

    _manifest.append({
        "s3_key": s3_key,
        "entity": entity,
        "entity_id": str(entity_id),
        "image_role": image_role,
        "content_type": ctype,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "source_url": url,
        "ingest_date": D,
    })
    time.sleep(0.3)
    return True


# ---------- BRONZE: metadata ----------
def ingest_teams() -> list:
    body = get(f"{BASE}/search_all_teams.php",
               params={"l": LEAGUE}).json()
    teams = body.get("teams") or []
    put_json_gz(
        f"bronze/thesportsdb/metadata/entity=teams/league=EPL"
        f"/ingest_date={D}/teams.json.gz",
        body, SRC, meta={"teams": len(teams)})
    print(f"  ✓ {len(teams)} đội")
    return teams


def ingest_players(team_id: str) -> list:
    body = get(f"{BASE}/lookup_all_players.php",
               params={"id": team_id}).json()
    players = body.get("player") or []
    if players:
        put_json_gz(
            f"bronze/thesportsdb/metadata/entity=players/team_id={team_id}"
            f"/ingest_date={D}/players.json.gz",
            body, SRC, meta={"team_id": team_id, "players": len(players)})
    return players


# ---------- BRONZE: binary ----------
def ingest_team_media(teams: list):
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
            if fetch_image(url, key, "team", tid, role):
                n += 1
        print(f"  · {t['strTeam']}: xong")
    print(f"  ✓ tải {n} ảnh đội")


def ingest_player_media(players: list, team_name: str):
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
            if fetch_image(url, key, "player", pid, role):
                n += 1
    print(f"    · {team_name}: {len(players)} cầu thủ, {n} ảnh")


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
    put_parquet(f"silver/dim/tsdb_teams/ingest_date={D}/part-0.parquet",
                df, SRC)
    print(f"  ✓ dim đội: {len(df)} dòng")
    return df


def write_manifest():
    """Bảng tra cứu mọi file nhị phân — làm cho ảnh trở nên query-được."""
    if not _manifest:
        print("  ! manifest rỗng (có thể ảnh đã tồn tại từ lần chạy trước)")
        return pd.DataFrame()
    df = pd.DataFrame(_manifest)
    put_parquet(f"_meta/thesportsdb/media_manifest/ingest_date={D}/manifest.parquet",
                df, SRC)
    put_parquet(f"silver/dim/tsdb_media/ingest_date={D}/part-0.parquet",
                df, SRC)
    print(f"  ✓ manifest: {len(df)} file, "
          f"tổng {df.size_bytes.sum()/1024/1024:.1f} MB")
    print(df.groupby("image_role").size().to_string())
    return df


def presigned_url(s3_key: str, expires=3600) -> str:
    """Tạo link tạm để nhúng ảnh vào dashboard/notebook."""
    return S3.generate_presigned_url(
        "get_object", Params={"Bucket": BUCKET, "Key": s3_key},
        ExpiresIn=expires)


if __name__ == "__main__":
    print("[1/5] metadata đội")
    teams = ingest_teams()

    print("[2/5] ảnh đội")
    ingest_team_media(teams)

    print("[3/5] metadata + ảnh cầu thủ")
    for t in teams:
        players = ingest_players(t["idTeam"])
        if players:
            ingest_player_media(players, t["strTeam"])
        time.sleep(0.5)

    print("[4/5] silver: dim đội")
    build_teams_dim(teams)

    print("[5/5] manifest")
    mf = write_manifest()

    if not mf.empty:
        sample = mf.iloc[0]["s3_key"]
        print(f"\nLink tạm để xem thử ảnh (hết hạn sau 1h):\n{presigned_url(sample)}")

    summary("bronze/thesportsdb/")
    summary("silver/dim/tsdb_teams/")
```

## 5. Kết quả mong đợi

```
[1/5] metadata đội
  ✓ s3://football-lake/bronze/thesportsdb/metadata/entity=teams/league=EPL/ingest_date=2026-09-16/teams.json.gz  (62,118 B, ...)
  ✓ 20 đội
[2/5] ảnh đội
  ✓ s3://football-lake/bronze/thesportsdb/media/entity=team/team_id=133604/badge.png  (32,104 B, sha256:a91f...)
  ✓ .../team_id=133604/stadium_thumb.jpg  (188,442 B, ...)
  · Arsenal: xong
  · Aston Villa: xong
  ...
  ✓ tải 104 ảnh đội
[3/5] metadata + ảnh cầu thủ
    · Arsenal: 24 cầu thủ, 41 ảnh
    · Chelsea: 26 cầu thủ, 44 ảnh
  ...
[4/5] silver: dim đội
  ✓ dim đội: 20 dòng
[5/5] manifest
  ✓ manifest: 483 file, tổng 214.7 MB
image_role
badge            20
banner           18
cutout          142
equipment        19
fanart1          16
logo             20
stadium_thumb    17
thumb           231

Link tạm để xem thử ảnh (hết hạn sau 1h):
http://localhost:9000/football-lake/bronze/thesportsdb/media/...?X-Amz-Algorithm=...

[summary] bronze/thesportsdb/: 505 objects, 216.83 MB
```

Bảng phân bố `image_role` ở cuối là bằng chứng tốt: nó cho thấy dữ liệu **không đồng đều** (231 ảnh thumb nhưng chỉ 16 fanart) — thực tế của dữ liệu mở.

## 6. Truy vấn kiểm chứng

Điểm hay của manifest: bạn **query được dữ liệu nhị phân bằng SQL**.

```sql
-- Có bao nhiêu ảnh mỗi loại, dung lượng ra sao?
SELECT entity, image_role,
       COUNT(*)                              AS so_file,
       ROUND(SUM(size_bytes)/1024.0/1024, 2) AS mb,
       ROUND(AVG(size_bytes)/1024.0, 1)      AS kb_tb
FROM read_parquet('s3://football-lake/silver/dim/tsdb_media/**/*.parquet')
GROUP BY 1, 2 ORDER BY mb DESC;

-- Đội nào thiếu ảnh? (bài toán data completeness)
SELECT t.team_name,
       COUNT(m.s3_key)                                        AS so_anh,
       STRING_AGG(m.image_role, ', ' ORDER BY m.image_role)   AS co_gi
FROM read_parquet('s3://football-lake/silver/dim/tsdb_teams/**/*.parquet') t
LEFT JOIN read_parquet('s3://football-lake/silver/dim/tsdb_media/**/*.parquet') m
  ON m.entity_id = t.team_id AND m.entity = 'team'
GROUP BY t.team_name ORDER BY so_anh ASC;

-- Phát hiện ảnh trùng lặp bằng checksum
SELECT sha256, COUNT(*) AS n,
       STRING_AGG(s3_key, ' | ') AS cac_file
FROM read_parquet('s3://football-lake/silver/dim/tsdb_media/**/*.parquet')
GROUP BY sha256 HAVING COUNT(*) > 1;

-- Sân vận động: đối chiếu với Wikidata (file 06)
SELECT t.team_name, t.stadium, t.stadium_capacity AS tsdb_capacity,
       w.capacity AS wikidata_capacity,
       t.stadium_capacity - w.capacity AS chenh_lech
FROM read_parquet('s3://football-lake/silver/dim/tsdb_teams/**/*.parquet') t
JOIN read_parquet('s3://football-lake/silver/dim/wd_stadiums/**/*.parquet') w
  ON LOWER(w.venueLabel) = LOWER(t.stadium)
ORDER BY ABS(chenh_lech) DESC;
```

Truy vấn cuối là một **data quality check cross-source** đẹp: hai nguồn độc lập cùng nói về sức chứa sân, chênh lệch lớn nghĩa là một bên đã lỗi thời.

## 7. Hiển thị ảnh trong notebook

```python
from IPython.display import Image, display
from lake.minio_io import read_bytes

display(Image(read_bytes(
    "bronze/thesportsdb/media/entity=team/team_id=133604/badge.png")))
```

Hoặc dùng presigned URL để nhúng vào dashboard (Streamlit, Metabase) mà không cần mở public bucket.

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `lookup_all_players` trả `null` | Test key `3` giới hạn một số endpoint | Bình thường; ảnh đội vẫn lấy được. Nâng key nếu cần cầu thủ |
| Ảnh tải về 0 byte | URL chết (link ảnh cũ) | Kiểm tra `len(data) > 0` trước khi ghi |
| Tải rất chậm | Ảnh fanart có thể >2 MB | Bỏ `strFanart*` khỏi `TEAM_IMAGES` nếu chỉ cần logo |
| `Content-Type` sai trong MinIO | Server trả header lạ | Script đã suy ra từ phần mở rộng file trước |
| Chạy lại tạo bản sao | Thiếu kiểm tra tồn tại | `fetch_image` đã có `if exists(key): return False` |
| Manifest rỗng khi chạy lần 2 | Mọi ảnh đã có, không tải gì | Đúng ý đồ; muốn manifest đầy đủ thì quét lại bằng `list_keys` |

## 9. Mở rộng

- **Trích màu chủ đạo từ logo** bằng Pillow → cột `primary_color` trong dim đội, dùng để tô màu biểu đồ tự động:
```python
from PIL import Image as PILImage
import io
img = PILImage.open(io.BytesIO(read_bytes(key))).convert("RGB")
dominant = max(img.getcolors(img.size[0]*img.size[1]), key=lambda x: x[0])[1]
```
- Tạo **thumbnail 64×64** cho mọi ảnh, lưu ở `silver/media/thumbnails/` → dashboard load nhanh hơn nhiều.
- Dùng ảnh cutout cầu thủ làm dataset cho bài toán CV đơn giản (phân loại đội qua màu áo).
- Đặt **lifecycle policy** trên MinIO để chuyển ảnh fanart ít dùng sang tier lưu trữ rẻ hơn — mô phỏng tiering trong lake thật.
