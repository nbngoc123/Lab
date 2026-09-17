# 05 — European Soccer Database (SQLite) → MinIO

**Kiểu ingest:** Database quan hệ → lake (CDC-style full load)
**Tần suất:** 1 lần, dữ liệu tĩnh
**Độ khó:** ★★★☆☆ — dạy đúng mô hình "trích xuất từ RDBMS nghiệp vụ vào lake"

---

## 1. Vì sao chọn nguồn này

Bảy nguồn kia đều là API hoặc file. Nguồn này là **database thật**: 7 bảng có khóa ngoại, schema chuẩn hóa, và một cột XML nhúng. Nó mô phỏng đúng tình huống phổ biến nhất trong doanh nghiệp — kéo dữ liệu từ hệ thống OLTP vào lake.

Nội dung: **~25.000 trận, 11 giải châu Âu, mùa 2008–2016**, kèm chỉ số cầu thủ từ FIFA (overall rating, các thuộc tính kỹ thuật) theo thời gian.

## 2. Lấy file

Nguồn: Kaggle — dataset "European Soccer Database" (`hugomathien/soccer`). File `database.sqlite`, ~313 MB.

```bash
# cách 1: Kaggle CLI
pip install kaggle
# đặt kaggle.json (API token) vào ~/.kaggle/
kaggle datasets download -d hugomathien/soccer -p /tmp/soccer --unzip

# cách 2: tải thủ công từ web Kaggle rồi đặt vào /tmp/soccer/database.sqlite
```

## 3. Schema của database

| Bảng | Số dòng | Nội dung | Khóa |
|---|---|---|---|
| `Country` | 11 | Quốc gia | `id` |
| `League` | 11 | Giải đấu | `id`, FK `country_id` |
| `Team` | ~300 | Đội bóng | `team_api_id`, `team_fifa_api_id` |
| `Team_Attributes` | ~1.500 | Chỉ số chiến thuật đội theo thời điểm | FK `team_api_id` |
| `Player` | ~11.000 | Cầu thủ (tên, ngày sinh, chiều cao, cân nặng) | `player_api_id` |
| `Player_Attributes` | ~184.000 | Chỉ số FIFA theo từng ngày cập nhật | FK `player_api_id` |
| `Match` | ~25.000 | Trận đấu + đội hình + **odds** + **XML sự kiện** | `id`, FK `league_id`, `home_team_api_id` |

Bảng `Match` có **115 cột**, trong đó:
- `home_player_1..11`, `away_player_1..11`: ID cầu thủ đá chính
- `home_player_X1..X11`, `Y1..Y11`: tọa độ vị trí
- `B365H/D/A`, `BWH/D/A`, ...: odds 10 nhà cái
- `goal`, `shoton`, `shotoff`, `foulcommit`, `card`, `cross`, `corner`, `possession`: **XML string** chứa chi tiết sự kiện

Cột XML là điểm thú vị nhất — nó cho bạn cơ hội xử lý semi-structured data nhúng trong RDBMS.

## 4. Layout trong lake

```
bronze/european_soccer_db/_snapshot/ingest_date=2026-09-16/database.sqlite
bronze/european_soccer_db/tables/table=Match/ingest_date=2026-09-16/Match.parquet
bronze/european_soccer_db/tables/table=Player/ingest_date=2026-09-16/Player.parquet
... (7 bảng)
_meta/european_soccer_db/schema.json          ← DDL + row count mỗi bảng

silver/matches/esd_matches/league=EPL/season=2015_2016/part-0.parquet
silver/players/esd_player_attributes/part-0.parquet
silver/events/esd_match_goals/part-0.parquet   ← parse từ XML
silver/lineups/esd_lineups/part-0.parquet      ← unpivot 22 cột cầu thủ
```

Lưu **cả file .sqlite gốc** ở bronze là chủ ý: nếu sau này phát hiện parse sai, bạn replay được mà không cần tải lại từ Kaggle.

## 5. Script ingest — `pipelines/p05_european_soccer.py`

```python
"""Ingest SQLite database vào MinIO: full load + parse XML + unpivot lineup."""
import json
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd
from lake.minio_io import put_file, put_parquet, today, summary, S3, BUCKET

SRC = "european-soccer-db"
D = today()
DB = Path("/tmp/soccer/database.sqlite")

TABLES = ["Country", "League", "Team", "Team_Attributes",
          "Player", "Player_Attributes", "Match"]


def conn():
    if not DB.exists():
        raise FileNotFoundError(
            f"Không thấy {DB}. Tải dataset Kaggle 'hugomathien/soccer' trước.")
    return sqlite3.connect(DB)


# ---------- BRONZE ----------
def snapshot_raw_db():
    """Giữ nguyên file gốc — nguyên tắc bất biến của bronze."""
    put_file(
        f"bronze/european_soccer_db/_snapshot/ingest_date={D}/database.sqlite",
        DB, SRC, content_type="application/vnd.sqlite3",
        meta={"size_mb": round(DB.stat().st_size / 1024 / 1024, 1)})


def dump_schema(c):
    """Ghi lại DDL + row count — metadata quan trọng cho data catalog."""
    info = {}
    for t in TABLES:
        ddl = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (t,)).fetchone()[0]
        n = c.execute(f"SELECT COUNT(*) FROM `{t}`").fetchone()[0]
        cols = [r[1] for r in c.execute(f"PRAGMA table_info(`{t}`)")]
        info[t] = {"row_count": n, "n_columns": len(cols),
                   "columns": cols, "ddl": ddl}
        print(f"  · {t:<20} {n:>8,} dòng, {len(cols):>3} cột")

    S3.put_object(Bucket=BUCKET, Key="_meta/european_soccer_db/schema.json",
                  Body=json.dumps(info, indent=2).encode(),
                  ContentType="application/json")
    return info


def extract_tables(c):
    """Full load: mỗi bảng -> 1 Parquet ở bronze."""
    for t in TABLES:
        df = pd.read_sql_query(f"SELECT * FROM `{t}`", c)
        # SQLite lỏng kiểu -> ép object columns về string cho pyarrow
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).replace("None", None)
        put_parquet(
            f"bronze/european_soccer_db/tables/table={t}/ingest_date={D}/{t}.parquet",
            df, SRC, meta={"table": t})


# ---------- SILVER ----------
def build_matches(c):
    """Join Match với League/Country/Team để có bảng đọc được."""
    q = """
    SELECT m.id AS match_id, m.date AS match_date, m.season, m.stage,
           l.name AS league_name, co.name AS country,
           ht.team_long_name AS home_team, at.team_long_name AS away_team,
           m.home_team_goal, m.away_team_goal,
           m.B365H, m.B365D, m.B365A, m.BWH, m.BWD, m.BWA
    FROM Match m
    JOIN League  l  ON l.id = m.league_id
    JOIN Country co ON co.id = m.country_id
    JOIN Team    ht ON ht.team_api_id = m.home_team_api_id
    JOIN Team    at ON at.team_api_id = m.away_team_api_id
    """
    df = pd.read_sql_query(q, c)
    df["match_date"] = pd.to_datetime(df["match_date"])
    df["total_goals"] = df.home_team_goal + df.away_team_goal
    df["result"] = df.apply(
        lambda r: "H" if r.home_team_goal > r.away_team_goal
        else ("A" if r.home_team_goal < r.away_team_goal else "D"), axis=1)

    # partition theo giải + mùa
    df["league_key"] = df.league_name.str.replace(r"[^A-Za-z]", "", regex=True)
    for (lg, season), g in df.groupby(["league_key", "season"]):
        s = season.replace("/", "_")
        put_parquet(
            f"silver/matches/esd_matches/league={lg}/season={s}/part-0.parquet",
            g.drop(columns=["league_key"]), SRC)
    print(f"  ✓ {len(df):,} trận, {df.league_name.nunique()} giải")
    return df


def build_player_attributes(c):
    q = """
    SELECT pa.*, p.player_name, p.birthday, p.height, p.weight
    FROM Player_Attributes pa
    JOIN Player p ON p.player_api_id = pa.player_api_id
    """
    df = pd.read_sql_query(q, c)
    df["date"] = pd.to_datetime(df["date"])
    df["birthday"] = pd.to_datetime(df["birthday"])
    df["age_at_rating"] = ((df["date"] - df["birthday"]).dt.days / 365.25).round(1)
    put_parquet("silver/players/esd_player_attributes/part-0.parquet", df, SRC,
                meta={"players": df.player_api_id.nunique()})
    print(f"  ✓ {len(df):,} bản ghi chỉ số, {df.player_api_id.nunique():,} cầu thủ")
    return df


def parse_goal_xml(c):
    """
    Cột `goal` trong Match là XML dạng:
      <goal><value><id>..</id><elapsed>23</elapsed>
      <player1>30934</player1><team>10260</team>...</value>...</goal>
    Parse thành bảng event phẳng.
    """
    rows = []
    cur = c.execute(
        "SELECT id, goal FROM Match WHERE goal IS NOT NULL AND goal != ''")
    for match_id, xml in cur:
        try:
            root = ET.fromstring(xml)
        except ET.ParseError:
            continue
        for v in root.findall("value"):
            def g(tag):
                el = v.find(tag)
                return el.text if el is not None else None
            rows.append({
                "match_id": match_id,
                "minute": pd.to_numeric(g("elapsed"), errors="coerce"),
                "player_api_id": pd.to_numeric(g("player1"), errors="coerce"),
                "assist_api_id": pd.to_numeric(g("player2"), errors="coerce"),
                "team_api_id": pd.to_numeric(g("team"), errors="coerce"),
                "goal_type": g("subtype") or g("goal_type"),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        print("  ! không parse được goal XML")
        return df

    # gắn tên cầu thủ
    players = pd.read_sql_query(
        "SELECT player_api_id, player_name FROM Player", c)
    df = df.merge(players, on="player_api_id", how="left")

    put_parquet("silver/events/esd_match_goals/part-0.parquet", df, SRC,
                meta={"goals": len(df)})
    print(f"  ✓ parse XML: {len(df):,} bàn thắng từ "
          f"{df.match_id.nunique():,} trận")
    return df


def build_lineups(c):
    """Unpivot 22 cột home_player_1..11 / away_player_1..11 thành long format."""
    cols = (["id"]
            + [f"home_player_{i}" for i in range(1, 12)]
            + [f"away_player_{i}" for i in range(1, 12)]
            + [f"home_player_X{i}" for i in range(1, 12)]
            + [f"home_player_Y{i}" for i in range(1, 12)])
    df = pd.read_sql_query(f"SELECT {','.join(cols)} FROM Match", c)

    rows = []
    for side in ("home", "away"):
        for i in range(1, 12):
            pc = f"{side}_player_{i}"
            part = pd.DataFrame({
                "match_id": df["id"],
                "side": side,
                "slot": i,
                "player_api_id": pd.to_numeric(df[pc], errors="coerce"),
            })
            if side == "home":
                part["pos_x"] = pd.to_numeric(df[f"home_player_X{i}"],
                                              errors="coerce")
                part["pos_y"] = pd.to_numeric(df[f"home_player_Y{i}"],
                                              errors="coerce")
            rows.append(part)

    out = pd.concat(rows, ignore_index=True).dropna(subset=["player_api_id"])
    players = pd.read_sql_query(
        "SELECT player_api_id, player_name FROM Player", c)
    out = out.merge(players, on="player_api_id", how="left")

    put_parquet("silver/lineups/esd_lineups/part-0.parquet", out, SRC)
    print(f"  ✓ unpivot lineup: {len(out):,} dòng cầu thủ-trận")
    return out


if __name__ == "__main__":
    print("[1/6] snapshot file .sqlite gốc lên bronze")
    snapshot_raw_db()

    with conn() as c:
        print("[2/6] dump schema")
        dump_schema(c)

        print("[3/6] full load 7 bảng -> Parquet")
        extract_tables(c)

        print("[4/6] silver: matches")
        build_matches(c)

        print("[5/6] silver: player attributes")
        build_player_attributes(c)

        print("[6/6] silver: parse XML + unpivot lineup")
        parse_goal_xml(c)
        build_lineups(c)

    summary("bronze/european_soccer_db/")
    summary("silver/")
```

## 6. Kết quả mong đợi

```
[1/6] snapshot file .sqlite gốc lên bronze
  ✓ s3://football-lake/bronze/european_soccer_db/_snapshot/ingest_date=2026-09-16/database.sqlite  (328,098,304 B, from file)
[2/6] dump schema
  · Country                   11 dòng,   2 cột
  · League                    11 dòng,   3 cột
  · Team                     299 dòng,   5 cột
  · Team_Attributes        1,458 dòng,  25 cột
  · Player                11,060 dòng,   7 cột
  · Player_Attributes    183,978 dòng,  42 cột
  · Match                 25,979 dòng, 115 cột
[3/6] full load 7 bảng -> Parquet
  ✓ .../table=Match/ingest_date=2026-09-16/Match.parquet  (31,402,118 B, 25979 rows)
  ...
[4/6] silver: matches
  ✓ 25,979 trận, 11 giải
[5/6] silver: player attributes
  ✓ 183,978 bản ghi chỉ số, 11,060 cầu thủ
[6/6] silver: parse XML + unpivot lineup
  ✓ parse XML: 68,514 bàn thắng từ 21,374 trận
  ✓ unpivot lineup: 476,372 dòng cầu thủ-trận

[summary] bronze/european_soccer_db/: 8 objects, 372.44 MB
```

Số bàn thắng parse từ XML (~68.500) nên xấp xỉ tổng `home_team_goal + away_team_goal` của các trận có XML. Đây là **check kiểm chứng** tốt nhất: nếu lệch nhiều, parser XML của bạn bỏ sót.

## 7. Truy vấn kiểm chứng

```sql
-- So sánh 11 giải châu Âu: giải nào nhiều bàn nhất?
SELECT league_name, country, COUNT(*) AS so_tran,
       ROUND(AVG(total_goals), 2) AS ban_tb,
       ROUND(100.0 * SUM(result='H') / COUNT(*), 1) AS pct_thang_san_nha
FROM read_parquet('s3://football-lake/silver/matches/esd_matches/**/*.parquet')
GROUP BY 1, 2 ORDER BY ban_tb DESC;

-- Chỉ số FIFA có dự đoán được bàn thắng thực tế không?
WITH scorers AS (
  SELECT player_api_id, COUNT(*) AS ban
  FROM read_parquet('s3://football-lake/silver/events/esd_match_goals/*.parquet')
  GROUP BY 1
),
ratings AS (
  SELECT player_api_id, player_name,
         AVG(overall_rating) AS rating, AVG(finishing) AS finishing
  FROM read_parquet('s3://football-lake/silver/players/esd_player_attributes/*.parquet')
  GROUP BY 1, 2
)
SELECT r.player_name, ROUND(r.rating,1) AS rating,
       ROUND(r.finishing,1) AS finishing, s.ban
FROM ratings r JOIN scorers s USING (player_api_id)
ORDER BY s.ban DESC LIMIT 20;

-- Kiểm tra tính nhất quán: bàn từ XML vs cột tổng
SELECT
  (SELECT COUNT(*) FROM read_parquet('s3://football-lake/silver/events/esd_match_goals/*.parquet')) AS ban_tu_xml,
  (SELECT SUM(home_team_goal + away_team_goal)
   FROM read_parquet('s3://football-lake/silver/matches/esd_matches/**/*.parquet')) AS ban_tu_cot;
```

## 8. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `pyarrow` lỗi `Could not convert` ở bảng `Match` | SQLite lưu XML dài trong cột object | Script đã ép `astype(str)` |
| `ET.ParseError` | Một số XML bị cắt/rỗng | Đã bọc `try/except`, bỏ qua dòng hỏng |
| Upload file 328 MB timeout | boto3 mặc định chưa tối ưu | `upload_file` tự dùng multipart; nếu vẫn lỗi tăng `Config(read_timeout=300)` |
| Bàn từ XML ít hơn cột tổng | Một số trận không có XML (NULL) | Bình thường — đối chiếu bằng `WHERE goal IS NOT NULL` |
| `season` chứa dấu `/` làm hỏng path | `"2015/2016"` | Script thay `/` thành `_` |

## 9. Mở rộng

- Parse thêm các cột XML khác: `shoton`, `card`, `corner`, `possession` → mỗi cột thành một bảng silver, tổng cộng 8 bảng event từ 1 nguồn.
- Bảng `Team_Attributes` có các chỉ số chiến thuật (`buildUpPlaySpeed`, `defencePressure`) → join với kết quả trận để xem phong cách nào hiệu quả.
- Nguồn này chồng lấn thời gian với Football-Data.co.uk (file 03) ở các mùa 2015/16. Join hai nguồn theo (ngày, đội) và đối chiếu tỷ số → bài kiểm tra chất lượng dữ liệu cross-source rất thuyết phục.
