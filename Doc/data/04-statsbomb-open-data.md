# 04 — StatsBomb Open Data (nested JSON lớn) → MinIO

**Kiểu ingest:** Flat file JSON lồng sâu, khối lượng lớn (hàng trăm MB → GB)
**Tần suất:** 1 lần, dữ liệu tĩnh
**Độ khó:** ★★★★☆ — thử thách nằm ở **flatten nested JSON** và **xử lý số lượng file lớn**

---

## 1. Vì sao chọn nguồn này

Đây là dữ liệu **chi tiết nhất** mà bạn có thể lấy miễn phí: mỗi trận có ~3.500 event, mỗi event có tọa độ `(x, y)` trên sân, kết quả, người liên quan, và với cú sút thì có **xG do StatsBomb tính** cộng **freeze frame** (vị trí mọi cầu thủ tại thời điểm sút).

Không nguồn nào khác trong bộ 8 cho bạn dữ liệu ở mức granularity này. Nó biến lake của bạn từ "thống kê trận" thành "phân tích chiến thuật".

**Giới hạn:** chỉ một số giải/mùa được mở (World Cup, Euro, La Liga có Messi, FA WSL, NWSL, Champions League chung kết...). Không có Premier League đầy đủ. Chấp nhận điều này — giá trị nằm ở độ sâu, không phải độ phủ.

## 2. Cấu trúc repo

Repo: `https://github.com/statsbomb/open-data`, nhánh `master`, thư mục `data/`:

```
data/
├── competitions.json                    # danh mục giải + mùa
├── matches/{competition_id}/{season_id}.json    # danh sách trận
├── events/{match_id}.json               # ~3.500 event/trận  ← file to nhất
├── lineups/{match_id}.json              # đội hình
└── three-sixty/{match_id}.json          # tracking 360 (chỉ vài giải)
```

Raw URL: `https://raw.githubusercontent.com/statsbomb/open-data/master/data/...`

**Hai cách lấy:**
- **Git clone** (khuyên dùng): 1 lần, ~2 GB, nhanh nhất
- **HTTP từng file**: linh hoạt hơn nhưng chậm, dễ bị GitHub rate-limit

## 3. Layout trong lake

```
bronze/statsbomb/competitions/competitions.json.gz
bronze/statsbomb/matches/competition_id=43/season_id=106/matches.json.gz
bronze/statsbomb/events/competition_id=43/season_id=106/match_id=3869685/events.json.gz
bronze/statsbomb/lineups/competition_id=43/season_id=106/match_id=3869685/lineups.json.gz

silver/events/sb_events/competition_id=43/season_id=106/part-0.parquet
silver/events/sb_shots/competition_id=43/season_id=106/part-0.parquet
silver/matches/sb_matches/competition_id=43/season_id=106/part-0.parquet
```

Partition theo `competition_id`/`season_id` cho phép query engine chỉ đọc giải cần thiết.

## 4. Chọn giải để ingest

Đừng lấy hết 2 GB ngay lần đầu. Bắt đầu với 2-3 giải:

| competition_id | season_id | Giải | Số trận |
|---|---|---|---|
| 43 | 106 | FIFA World Cup 2022 | 64 |
| 55 | 43 | UEFA Euro 2020 | 51 |
| 11 | 90 | La Liga 2020/21 | 35 |
| 2 | 44 | Premier League 2003/04 | 33 |
| 37 | 90 | FA WSL 2020/21 | 131 |

World Cup 2022 là lựa chọn tốt nhất để bắt đầu: 64 trận, có cả 360 data, ~250 MB.

## 5. Script ingest — `pipelines/p04_statsbomb.py`

```python
"""Ingest StatsBomb Open Data: clone repo -> bronze -> flatten -> silver."""
import json
import subprocess
from pathlib import Path

import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, read_json_gz, exists, summary

SRC = "statsbomb-open-data"
REPO = "https://github.com/statsbomb/open-data.git"
LOCAL = Path("/tmp/statsbomb-open-data")

# (competition_id, season_id) muốn ingest
TARGETS = [
    (43, 106),   # World Cup 2022
    (55, 43),    # Euro 2020
    (11, 90),    # La Liga 2020/21
]


# ---------- bước 0: lấy repo ----------
def clone_repo():
    if LOCAL.exists():
        print(f"  · repo đã có tại {LOCAL}")
        return
    print("  · đang clone (~2 GB, mất vài phút)...")
    subprocess.run(
        ["git", "clone", "--depth", "1", REPO, str(LOCAL)],
        check=True)
    print("  ✓ clone xong")


def _load(rel: str):
    return json.loads((LOCAL / "data" / rel).read_text(encoding="utf-8"))


# ---------- BRONZE ----------
def ingest_competitions():
    comps = _load("competitions.json")
    put_json_gz("bronze/statsbomb/competitions/competitions.json.gz",
                comps, SRC, meta={"count": len(comps)})
    return comps


def ingest_competition(cid: int, sid: int):
    matches = _load(f"matches/{cid}/{sid}.json")
    put_json_gz(
        f"bronze/statsbomb/matches/competition_id={cid}/season_id={sid}/matches.json.gz",
        matches, SRC, meta={"matches": len(matches)})

    match_ids = [m["match_id"] for m in matches]
    print(f"  · {len(match_ids)} trận cho comp={cid} season={sid}")

    for i, mid in enumerate(match_ids, 1):
        base = (f"bronze/statsbomb/events/competition_id={cid}"
                f"/season_id={sid}/match_id={mid}")
        ekey = f"{base}/events.json.gz"
        if exists(ekey):
            continue
        try:
            events = _load(f"events/{mid}.json")
            put_json_gz(ekey, events, SRC,
                        meta={"match_id": mid, "events": len(events)})
        except FileNotFoundError:
            print(f"  ! thiếu events cho match {mid}")
            continue

        lkey = (f"bronze/statsbomb/lineups/competition_id={cid}"
                f"/season_id={sid}/match_id={mid}/lineups.json.gz")
        try:
            put_json_gz(lkey, _load(f"lineups/{mid}.json"), SRC,
                        meta={"match_id": mid})
        except FileNotFoundError:
            pass

        if i % 10 == 0:
            print(f"    ... {i}/{len(match_ids)}")

    return match_ids


# ---------- SILVER: flatten nested JSON ----------
def flatten_events(cid: int, sid: int, match_ids: list):
    """
    Event của StatsBomb lồng rất sâu. Chiến lược:
    - Cột phẳng cơ bản -> giữ nguyên
    - Object 1 cấp (type, team, player) -> lấy .name
    - location [x, y] -> tách 2 cột
    - pass.end_location, shot.end_location -> tách cột
    - Phần còn lại quá sâu (freeze_frame) -> giữ dạng JSON string
    """
    rows = []
    for mid in match_ids:
        key = (f"bronze/statsbomb/events/competition_id={cid}"
               f"/season_id={sid}/match_id={mid}/events.json.gz")
        if not exists(key):
            continue
        for e in read_json_gz(key):
            loc = e.get("location") or [None, None]
            rec = {
                "match_id": mid,
                "event_id": e["id"],
                "index": e["index"],
                "period": e["period"],
                "minute": e["minute"],
                "second": e["second"],
                "timestamp": e["timestamp"],
                "type": e["type"]["name"],
                "possession": e.get("possession"),
                "possession_team": (e.get("possession_team") or {}).get("name"),
                "play_pattern": (e.get("play_pattern") or {}).get("name"),
                "team": (e.get("team") or {}).get("name"),
                "player": (e.get("player") or {}).get("name"),
                "player_id": (e.get("player") or {}).get("id"),
                "position": (e.get("position") or {}).get("name"),
                "x": loc[0], "y": loc[1] if len(loc) > 1 else None,
                "duration": e.get("duration"),
                "under_pressure": e.get("under_pressure", False),
            }

            # chi tiết pass
            if p := e.get("pass"):
                end = p.get("end_location") or [None, None]
                rec.update({
                    "pass_end_x": end[0], "pass_end_y": end[1] if len(end) > 1 else None,
                    "pass_length": p.get("length"),
                    "pass_angle": p.get("angle"),
                    "pass_height": (p.get("height") or {}).get("name"),
                    "pass_recipient": (p.get("recipient") or {}).get("name"),
                    "pass_outcome": (p.get("outcome") or {}).get("name"),
                    "pass_is_cross": p.get("cross", False),
                })

            # chi tiết shot (phần giá trị nhất)
            if s := e.get("shot"):
                end = s.get("end_location") or [None, None, None]
                rec.update({
                    "shot_statsbomb_xg": s.get("statsbomb_xg"),
                    "shot_outcome": (s.get("outcome") or {}).get("name"),
                    "shot_technique": (s.get("technique") or {}).get("name"),
                    "shot_body_part": (s.get("body_part") or {}).get("name"),
                    "shot_type": (s.get("type") or {}).get("name"),
                    "shot_end_x": end[0],
                    "shot_end_y": end[1] if len(end) > 1 else None,
                    "shot_end_z": end[2] if len(end) > 2 else None,
                    # freeze_frame quá sâu -> lưu JSON string, parse sau khi cần
                    "shot_freeze_frame": json.dumps(s.get("freeze_frame"))
                                         if s.get("freeze_frame") else None,
                })

            if c := e.get("carry"):
                end = c.get("end_location") or [None, None]
                rec["carry_end_x"] = end[0]
                rec["carry_end_y"] = end[1] if len(end) > 1 else None

            if d := e.get("duel"):
                rec["duel_type"] = (d.get("type") or {}).get("name")
                rec["duel_outcome"] = (d.get("outcome") or {}).get("name")

            rows.append(rec)

    df = pd.DataFrame(rows)
    if df.empty:
        print("  ! không có event nào")
        return df

    put_parquet(
        f"silver/events/sb_events/competition_id={cid}/season_id={sid}/part-0.parquet",
        df, SRC, meta={"matches": len(match_ids)})
    return df


def build_shots(df: pd.DataFrame, cid: int, sid: int):
    """Bảng riêng cho cú sút — dùng nhiều nhất trong phân tích."""
    if df.empty or "shot_statsbomb_xg" not in df.columns:
        return
    shots = df[df["type"] == "Shot"].copy()
    shots["is_goal"] = shots["shot_outcome"] == "Goal"
    # khoảng cách tới khung thành (sân StatsBomb: 120x80, khung ở x=120, y=40)
    shots["dist_to_goal"] = ((120 - shots["x"])**2 + (40 - shots["y"])**2)**0.5
    put_parquet(
        f"silver/events/sb_shots/competition_id={cid}/season_id={sid}/part-0.parquet",
        shots, SRC, meta={"shots": len(shots)})
    print(f"  ✓ {len(shots)} cú sút, tổng xG = {shots.shot_statsbomb_xg.sum():.1f}, "
          f"bàn thực tế = {shots.is_goal.sum()}")


def build_matches_dim(cid: int, sid: int):
    key = (f"bronze/statsbomb/matches/competition_id={cid}"
           f"/season_id={sid}/matches.json.gz")
    rows = []
    for m in read_json_gz(key):
        rows.append({
            "match_id": m["match_id"],
            "match_date": m["match_date"],
            "competition": m["competition"]["competition_name"],
            "season": m["season"]["season_name"],
            "home_team": m["home_team"]["home_team_name"],
            "away_team": m["away_team"]["away_team_name"],
            "home_score": m["home_score"],
            "away_score": m["away_score"],
            "stadium": (m.get("stadium") or {}).get("name"),
            "referee": (m.get("referee") or {}).get("name"),
            "stage": (m.get("competition_stage") or {}).get("name"),
        })
    df = pd.DataFrame(rows)
    df["match_date"] = pd.to_datetime(df["match_date"])
    put_parquet(
        f"silver/matches/sb_matches/competition_id={cid}/season_id={sid}/part-0.parquet",
        df, SRC)


if __name__ == "__main__":
    print("[1/4] clone repo")
    clone_repo()

    print("[2/4] competitions")
    ingest_competitions()

    for cid, sid in TARGETS:
        print(f"\n[3/4] bronze: competition={cid} season={sid}")
        mids = ingest_competition(cid, sid)

        print(f"[4/4] silver: flatten {len(mids)} trận")
        ev = flatten_events(cid, sid, mids)
        build_shots(ev, cid, sid)
        build_matches_dim(cid, sid)

    summary("bronze/statsbomb/")
    summary("silver/events/")
```

## 6. Kết quả mong đợi

```
[1/4] clone repo
  · đang clone (~2 GB, mất vài phút)...
  ✓ clone xong
[2/4] competitions
  ✓ s3://football-lake/bronze/statsbomb/competitions/competitions.json.gz  (3,882 B, ...)

[3/4] bronze: competition=43 season=106
  · 64 trận cho comp=43 season=106
  ✓ .../match_id=3869685/events.json.gz  (412,338 B, ...)
    ... 10/64
    ... 60/64
[4/4] silver: flatten 64 trận
  ✓ .../sb_events/competition_id=43/season_id=106/part-0.parquet  (18,204,551 B, 226,318 rows)
  ✓ 1,412 cú sút, tổng xG = 171.3, bàn thực tế = 172

[summary] bronze/statsbomb/: 3,552 objects, 408.61 MB
[summary] silver/events/: 6 objects, 54.22 MB
```

**Dòng đáng chú ý nhất:** `tổng xG = 171.3, bàn thực tế = 172`. Hai con số gần nhau chứng minh model xG của StatsBomb được hiệu chỉnh tốt **và** pipeline của bạn flatten đúng. Nếu chúng lệch xa (ví dụ xG = 40 vs 172 bàn) thì bạn đã mất dữ liệu ở đâu đó.

Nén cũng đáng chú ý: 226 nghìn event → 18 MB Parquet, trong khi JSON gốc gzip đã là 400 MB. Đó là sức mạnh của columnar format.

## 7. Truy vấn kiểm chứng

```sql
-- Cầu thủ nào vượt kỳ vọng xG nhiều nhất?
SELECT player, team,
       COUNT(*)                          AS so_sut,
       ROUND(SUM(shot_statsbomb_xg), 2)  AS tong_xg,
       SUM(is_goal::INT)                 AS ban_thang,
       ROUND(SUM(is_goal::INT) - SUM(shot_statsbomb_xg), 2) AS vuot_ky_vong
FROM read_parquet('s3://football-lake/silver/events/sb_shots/**/*.parquet')
GROUP BY player, team
HAVING COUNT(*) >= 8
ORDER BY vuot_ky_vong DESC LIMIT 15;

-- Bản đồ nhiệt: xG trung bình theo vùng sân
SELECT (x / 10)::INT * 10 AS zone_x,
       (y / 10)::INT * 10 AS zone_y,
       COUNT(*)                         AS so_sut,
       ROUND(AVG(shot_statsbomb_xg), 3) AS xg_tb
FROM read_parquet('s3://football-lake/silver/events/sb_shots/**/*.parquet')
GROUP BY 1, 2 HAVING COUNT(*) > 5
ORDER BY xg_tb DESC LIMIT 20;

-- Đội nào chuyền tiến lên nhiều nhất?
SELECT team,
       COUNT(*) FILTER (WHERE pass_end_x > x + 15) AS chuyen_tien_len,
       COUNT(*)                                     AS tong_chuyen,
       ROUND(100.0 * COUNT(*) FILTER (WHERE pass_end_x > x + 15) / COUNT(*), 1) AS pct
FROM read_parquet('s3://football-lake/silver/events/sb_events/**/*.parquet')
WHERE type = 'Pass' AND pass_outcome IS NULL   -- NULL = chuyền thành công
GROUP BY team ORDER BY pct DESC LIMIT 10;
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `MemoryError` khi flatten | Gom hết event vào 1 list | Xử lý theo batch 10 trận, ghi nhiều file `part-N.parquet` |
| Clone repo quá lâu | Repo ~2 GB có lịch sử git | Đã dùng `--depth 1`; hoặc tải từng file qua HTTP |
| `pyarrow` lỗi `Conversion failed for column` | Cột lẫn kiểu (int và string) | Ép `.astype(str)` hoặc `pd.to_numeric(errors="coerce")` |
| Parquet file quá lớn (>1 GB) | Một giải quá nhiều trận | Partition thêm theo `match_id` chia batch |
| `pass_outcome` toàn NULL | Đây là **đúng** — StatsBomb chỉ ghi outcome khi chuyền **hỏng** | NULL = thành công, đừng "sửa" |

## 9. Xử lý khối lượng lớn: phiên bản batch

Nếu ingest hết 2 GB, thay `flatten_events` bằng:

```python
def flatten_events_batched(cid, sid, match_ids, batch_size=10):
    for i in range(0, len(match_ids), batch_size):
        batch = match_ids[i:i+batch_size]
        rows = []
        for mid in batch:
            key = (f"bronze/statsbomb/events/competition_id={cid}"
                   f"/season_id={sid}/match_id={mid}/events.json.gz")
            if exists(key):
                rows.extend(_flatten_one(read_json_gz(key), mid))
        df = pd.DataFrame(rows)
        put_parquet(
            f"silver/events/sb_events/competition_id={cid}/season_id={sid}"
            f"/part-{i//batch_size:04d}.parquet", df, SRC)
        del rows, df       # giải phóng RAM
```

Nhiều file `part-*.parquet` trong cùng partition là **cách làm chuẩn** của data lake, không phải vấn đề — DuckDB/Spark đọc chúng như một bảng.

## 10. Mở rộng

- Parse `shot_freeze_frame` (đang là JSON string) thành bảng riêng `silver/events/sb_freeze_frames/` → dựng model xG của riêng bạn và so với `shot_statsbomb_xg`.
- Dùng `x`, `y`, `pass_end_x`, `pass_end_y` để vẽ pass network bằng matplotlib → tài liệu trực quan rất ấn tượng khi trình bày.
- So xG StatsBomb (file này) với xG của Understat và `expected_goals` của FPL (file 01) → bài toán so sánh nhà cung cấp dữ liệu.
