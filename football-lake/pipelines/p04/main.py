"""
Ingest StatsBomb Open Data: git clone -> bronze -> flatten nested JSON -> silver.
Dữ liệu tĩnh (1 lần clone), không cần key, free.
"""
import json
import subprocess
from pathlib import Path

import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, read_json_gz, exists, summary

SRC   = "statsbomb-open-data"
REPO  = "https://github.com/statsbomb/open-data.git"
LOCAL = Path("/tmp/statsbomb-open-data")

# (competition_id, season_id) muốn ingest
# World Cup 2022: 64 trận, ~250MB, dữ liệu 360 đầy đủ
# Euro 2020:      51 trận
# La Liga 20/21:  35 trận (có Messi ở Barca)
TARGETS = [
    (43, 106),   # FIFA World Cup 2022
    (55, 43),    # UEFA Euro 2020
    (11, 90),    # La Liga 2020/21 (Messi)
    (2,  44),    # Premier League 2003/04
]

# TEST_MODE: True = chỉ lấy 5 trận đầu của từng competition (nhanh)
# False = lấy tất cả
TEST_MODE = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _load(rel: str):
    return json.loads((LOCAL / "data" / rel).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# BƯỚC 0: Clone repo
# ---------------------------------------------------------------------------
def clone_repo():
    if LOCAL.exists():
        print(f"  · repo đã có tại {LOCAL}, đang git pull...")
        subprocess.run(["git", "-C", str(LOCAL), "pull", "--depth", "1"],
                       capture_output=True)
        return
    print("  · clone (~2 GB, mất vài phút với --depth 1)...")
    subprocess.run(
        ["git", "clone", "--depth", "1", REPO, str(LOCAL)],
        check=True)
    print("  ✓ clone xong")


# ---------------------------------------------------------------------------
# BRONZE
# ---------------------------------------------------------------------------
def ingest_competitions():
    comps = _load("competitions.json")
    put_json_gz("bronze/statsbomb/competitions/competitions.json.gz",
                comps, SRC, meta={"count": len(comps)})
    return comps


def ingest_competition(cid: int, sid: int) -> list:
    matches = _load(f"matches/{cid}/{sid}.json")
    put_json_gz(
        f"bronze/statsbomb/matches/competition_id={cid}/season_id={sid}/matches.json.gz",
        matches, SRC, meta={"matches": len(matches)})

    match_ids = [m["match_id"] for m in matches]
    if TEST_MODE:
        match_ids = match_ids[:5]
        print(f"  · TEST_MODE: chỉ lấy {len(match_ids)}/{len(matches)} trận")
    else:
        print(f"  · {len(match_ids)} trận cho comp={cid} season={sid}")

    for i, mid in enumerate(match_ids, 1):
        ekey = (f"bronze/statsbomb/events/competition_id={cid}"
                f"/season_id={sid}/match_id={mid}/events.json.gz")
        if exists(ekey):
            continue
        try:
            put_json_gz(ekey, _load(f"events/{mid}.json"), SRC,
                        meta={"match_id": mid})
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


# ---------------------------------------------------------------------------
# SILVER: flatten nested JSON
# ---------------------------------------------------------------------------
def _flatten_one(events: list, mid: int) -> list:
    rows = []
    for e in events:
        loc = e.get("location") or [None, None]
        rec = {
            "match_id":       mid,
            "event_id":       e["id"],
            "index":          e["index"],
            "period":         e["period"],
            "minute":         e["minute"],
            "second":         e["second"],
            "timestamp":      e["timestamp"],
            "type":           e["type"]["name"],
            "possession":     e.get("possession"),
            "possession_team": (e.get("possession_team") or {}).get("name"),
            "play_pattern":   (e.get("play_pattern") or {}).get("name"),
            "team":           (e.get("team") or {}).get("name"),
            "player":         (e.get("player") or {}).get("name"),
            "player_id":      (e.get("player") or {}).get("id"),
            "position":       (e.get("position") or {}).get("name"),
            "x":              loc[0],
            "y":              loc[1] if len(loc) > 1 else None,
            "duration":       e.get("duration"),
            "under_pressure": e.get("under_pressure", False),
        }

        if p := e.get("pass"):
            end = p.get("end_location") or [None, None]
            rec.update({
                "pass_end_x":       end[0],
                "pass_end_y":       end[1] if len(end) > 1 else None,
                "pass_length":      p.get("length"),
                "pass_angle":       p.get("angle"),
                "pass_height":      (p.get("height") or {}).get("name"),
                "pass_recipient":   (p.get("recipient") or {}).get("name"),
                "pass_outcome":     (p.get("outcome") or {}).get("name"),
                "pass_is_cross":    p.get("cross", False),
            })

        if s := e.get("shot"):
            end = s.get("end_location") or [None, None, None]
            rec.update({
                "shot_statsbomb_xg": s.get("statsbomb_xg"),
                "shot_outcome":      (s.get("outcome") or {}).get("name"),
                "shot_technique":    (s.get("technique") or {}).get("name"),
                "shot_body_part":    (s.get("body_part") or {}).get("name"),
                "shot_type":         (s.get("type") or {}).get("name"),
                "shot_end_x":        end[0],
                "shot_end_y":        end[1] if len(end) > 1 else None,
                "shot_end_z":        end[2] if len(end) > 2 else None,
                "shot_freeze_frame": json.dumps(s.get("freeze_frame"))
                                     if s.get("freeze_frame") else None,
            })

        if c := e.get("carry"):
            end = c.get("end_location") or [None, None]
            rec["carry_end_x"] = end[0]
            rec["carry_end_y"] = end[1] if len(end) > 1 else None

        if d := e.get("duel"):
            rec["duel_type"]    = (d.get("type") or {}).get("name")
            rec["duel_outcome"] = (d.get("outcome") or {}).get("name")

        rows.append(rec)
    return rows


def flatten_events(cid: int, sid: int, match_ids: list) -> pd.DataFrame:
    """Batch 10 trận 1 lần để tránh OOM với bộ data lớn."""
    BATCH = 10
    dfs = []
    for i in range(0, len(match_ids), BATCH):
        batch = match_ids[i:i+BATCH]
        rows = []
        for mid in batch:
            key = (f"bronze/statsbomb/events/competition_id={cid}"
                   f"/season_id={sid}/match_id={mid}/events.json.gz")
            if not exists(key):
                continue
            rows.extend(_flatten_one(read_json_gz(key), mid))
        if not rows:
            continue
        df = pd.DataFrame(rows)
        part_key = (f"silver/events/sb_events/competition_id={cid}"
                    f"/season_id={sid}/part-{i//BATCH:04d}.parquet")
        put_parquet(part_key, df, SRC, meta={"matches_in_batch": len(batch)})
        dfs.append(df)
        del rows, df

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def build_shots(df: pd.DataFrame, cid: int, sid: int):
    if df.empty or "shot_statsbomb_xg" not in df.columns:
        return
    shots = df[df["type"] == "Shot"].copy()
    shots["is_goal"]      = shots["shot_outcome"] == "Goal"
    shots["dist_to_goal"] = ((120 - shots["x"])**2 + (40 - shots["y"])**2)**0.5
    put_parquet(
        f"silver/events/sb_shots/competition_id={cid}/season_id={sid}/part-0.parquet",
        shots, SRC, meta={"shots": len(shots)})
    print(f"  ✓ {len(shots)} cú sút | xG={shots.shot_statsbomb_xg.sum():.1f} "
          f"| bàn thực={shots.is_goal.sum()}")


def build_matches_dim(cid: int, sid: int):
    key = (f"bronze/statsbomb/matches/competition_id={cid}"
           f"/season_id={sid}/matches.json.gz")
    rows = []
    for m in read_json_gz(key):
        rows.append({
            "match_id":   m["match_id"],
            "match_date": m["match_date"],
            "competition": m["competition"]["competition_name"],
            "season":      m["season"]["season_name"],
            "home_team":   m["home_team"]["home_team_name"],
            "away_team":   m["away_team"]["away_team_name"],
            "home_score":  m["home_score"],
            "away_score":  m["away_score"],
            "stadium":    (m.get("stadium") or {}).get("name"),
            "referee":    (m.get("referee") or {}).get("name"),
            "stage":      (m.get("competition_stage") or {}).get("name"),
        })
    df = pd.DataFrame(rows)
    df["match_date"] = pd.to_datetime(df["match_date"])
    put_parquet(
        f"silver/matches/sb_matches/competition_id={cid}/season_id={sid}/part-0.parquet",
        df, SRC)
    print(f"  ✓ {len(df)} trận -> sb_matches")


def run_pipeline():
    print(f"[1/4] clone repo (TEST_MODE={TEST_MODE})")
    clone_repo()

    print("\n[2/4] competitions")
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
    summary("silver/matches/sb_matches/")


if __name__ == "__main__":
    run_pipeline()
