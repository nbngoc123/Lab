# 06 — Wikidata SPARQL (semantic/graph) → MinIO

**Kiểu ingest:** SPARQL endpoint, dữ liệu đồ thị có liên kết
**Tần suất:** hàng tháng (dữ liệu tham chiếu ít đổi)
**Độ khó:** ★★★☆☆ — khó ở viết query SPARQL, dễ ở phần ingest

---

## 1. Vì sao chọn nguồn này

Bảy nguồn kia đều cho bạn **fact** (chuyện gì đã xảy ra). Nguồn này cho bạn **dimension giàu** (bối cảnh): quốc tịch cầu thủ, ngày sinh, chiều cao, vị trí thi đấu, sân vận động với **tọa độ địa lý** và sức chứa, CLB với năm thành lập và huấn luyện viên.

Đây là lớp **enrichment** — thứ biến bảng "Arsenal 2-1 Chelsea" thành "trận derby London tại sân Emirates sức chứa 60.704, tọa độ 51.555°N -0.108°E".

Thêm nữa: nó dạy bạn làm việc với **dữ liệu đồ thị và định danh toàn cầu** (QID). QID của Wikidata là chìa khóa liên kết tốt nhất giữa các nguồn — tốt hơn nhiều so với so khớp tên.

## 2. Endpoint và cú pháp

Endpoint: `https://query.wikidata.org/sparql`
Giao diện thử query: `https://query.wikidata.org/`

Các property hay dùng:

| Property | Ý nghĩa |
|---|---|
| `wdt:P31` | instance of (là một loại gì) |
| `wdt:P54` | member of sports team (thi đấu cho CLB) |
| `wdt:P27` | country of citizenship |
| `wdt:P569` | date of birth |
| `wdt:P2048` | height |
| `wdt:P413` | position played |
| `wdt:P115` | home venue (sân nhà) |
| `wdt:P1083` | maximum capacity |
| `wdt:P625` | coordinate location |
| `wdt:P571` | inception (năm thành lập) |
| `wdt:P286` | head coach |
| `wdt:P118` | league |

Entity quan trọng: `wd:Q9448` = Premier League, `wd:Q476028` = football club, `wd:Q937857` = association football player.

**Quan trọng:** Wikidata yêu cầu User-Agent mô tả rõ ứng dụng của bạn, nếu không sẽ bị chặn 403.

## 3. Layout trong lake

```
bronze/wikidata/sparql/query=pl_clubs/ingest_date=2026-09-16/result.json.gz
bronze/wikidata/sparql/query=pl_players/ingest_date=2026-09-16/result.json.gz
bronze/wikidata/sparql/query=pl_stadiums/ingest_date=2026-09-16/result.json.gz
bronze/wikidata/sparql/query=pl_managers/ingest_date=2026-09-16/result.json.gz
_meta/wikidata/queries/pl_clubs.rq        ← lưu cả câu SPARQL đã dùng

silver/dim/wd_clubs/ingest_date=2026-09-16/part-0.parquet
silver/dim/wd_players/ingest_date=2026-09-16/part-0.parquet
silver/dim/wd_stadiums/ingest_date=2026-09-16/part-0.parquet
```

Lưu **cả câu query** ở `_meta/` là thực hành tốt: sau 6 tháng bạn sẽ không nhớ mình đã hỏi gì.

## 4. Script ingest — `pipelines/p06_wikidata.py`

```python
"""Ingest Wikidata qua SPARQL: dimension giàu cho lake bóng đá."""
import time
import pandas as pd
from lake.minio_io import put_json_gz, put_parquet, today, summary, S3, BUCKET
from lake.http import SESSION

SRC = "wikidata"
D = today()
ENDPOINT = "https://query.wikidata.org/sparql"

# Wikidata yêu cầu UA mô tả rõ — thiếu cái này sẽ bị 403
UA = "football-lake/0.1 (educational data engineering project; contact: you@example.com)"


QUERIES = {
    # ---- CLB Premier League hiện tại ----
    "pl_clubs": """
SELECT ?club ?clubLabel ?inception ?venue ?venueLabel ?capacity
       ?coord ?coachLabel ?websiteURL
WHERE {
  ?club wdt:P118 wd:Q9448 .                      # thuộc Premier League
  OPTIONAL { ?club wdt:P571  ?inception }
  OPTIONAL { ?club wdt:P115  ?venue .
             OPTIONAL { ?venue wdt:P1083 ?capacity }
             OPTIONAL { ?venue wdt:P625  ?coord } }
  OPTIONAL { ?club wdt:P286  ?coach }
  OPTIONAL { ?club wdt:P856  ?websiteURL }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""",

    # ---- Cầu thủ đang thuộc các CLB PL ----
    "pl_players": """
SELECT ?player ?playerLabel ?clubLabel ?dob ?height
       ?countryLabel ?positionLabel ?sexLabel
WHERE {
  ?club   wdt:P118 wd:Q9448 .
  ?player wdt:P54  ?club .
  ?player wdt:P106 wd:Q937857 .                  # nghề: cầu thủ bóng đá
  OPTIONAL { ?player wdt:P569  ?dob }
  OPTIONAL { ?player wdt:P2048 ?height }
  OPTIONAL { ?player wdt:P27   ?country }
  OPTIONAL { ?player wdt:P413  ?position }
  OPTIONAL { ?player wdt:P21   ?sex }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
LIMIT 5000
""",

    # ---- Sân vận động + tọa độ ----
    "pl_stadiums": """
SELECT ?venue ?venueLabel ?capacity ?coord ?opened ?cityLabel ?countryLabel
WHERE {
  ?club  wdt:P118 wd:Q9448 .
  ?club  wdt:P115 ?venue .
  OPTIONAL { ?venue wdt:P1083 ?capacity }
  OPTIONAL { ?venue wdt:P625  ?coord }
  OPTIONAL { ?venue wdt:P1619 ?opened }
  OPTIONAL { ?venue wdt:P131  ?city }
  OPTIONAL { ?venue wdt:P17   ?country }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""",

    # ---- HLV các CLB PL + quốc tịch ----
    "pl_managers": """
SELECT ?club ?clubLabel ?coach ?coachLabel ?coachDob ?coachCountryLabel
WHERE {
  ?club  wdt:P118 wd:Q9448 .
  ?club  wdt:P286 ?coach .
  OPTIONAL { ?coach wdt:P569 ?coachDob }
  OPTIONAL { ?coach wdt:P27  ?coachCountry }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""",
}


def run_sparql(query: str) -> dict:
    r = SESSION.get(
        ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
        timeout=120,          # query phức tạp có thể chạy lâu
    )
    r.raise_for_status()
    return r.json()


def sparql_to_df(result: dict) -> pd.DataFrame:
    """
    SPARQL JSON có dạng:
      {"head": {"vars": [...]},
       "results": {"bindings": [{"var": {"type":"uri","value":"..."}}, ...]}}
    Ta phẳng hóa: mỗi biến -> 1 cột giá trị.
    """
    rows = []
    for b in result["results"]["bindings"]:
        rec = {}
        for var, cell in b.items():
            rec[var] = cell["value"]
        rows.append(rec)
    df = pd.DataFrame(rows)
    # thêm cột QID rút gọn từ URI entity
    for c in [c for c in df.columns if not c.endswith("Label")]:
        if df[c].astype(str).str.startswith("http://www.wikidata.org/entity/").any():
            df[c + "_qid"] = df[c].str.rsplit("/", n=1).str[-1]
    return df


def parse_point(s):
    """'Point(-0.108611 51.554999)' -> (lon, lat)"""
    if not isinstance(s, str) or not s.startswith("Point("):
        return None, None
    lon, lat = s[6:-1].split()
    return float(lon), float(lat)


# ---------- BRONZE ----------
def ingest_all() -> dict:
    out = {}
    for name, q in QUERIES.items():
        print(f"  · chạy query '{name}'...")
        res = run_sparql(q)
        n = len(res["results"]["bindings"])
        put_json_gz(
            f"bronze/wikidata/sparql/query={name}/ingest_date={D}/result.json.gz",
            res, SRC, meta={"query": name, "rows": n})
        # lưu cả câu query để truy vết
        S3.put_object(Bucket=BUCKET, Key=f"_meta/wikidata/queries/{name}.rq",
                      Body=q.encode(), ContentType="text/plain")
        out[name] = res
        print(f"    -> {n} dòng")
        time.sleep(2)          # lịch sự với endpoint công cộng
    return out


# ---------- SILVER ----------
def build_clubs(res: dict):
    df = sparql_to_df(res)
    df["capacity"] = pd.to_numeric(df.get("capacity"), errors="coerce")
    df["inception"] = pd.to_datetime(df.get("inception"), errors="coerce", utc=True)
    if "coord" in df:
        df[["venue_lon", "venue_lat"]] = df["coord"].apply(
            lambda s: pd.Series(parse_point(s)))
    # 1 CLB có thể ra nhiều dòng do OPTIONAL -> gộp lại
    df = df.sort_values("capacity", ascending=False).drop_duplicates("clubLabel")
    put_parquet(f"silver/dim/wd_clubs/ingest_date={D}/part-0.parquet", df, SRC)
    print(f"  ✓ {len(df)} CLB")
    return df


def build_players(res: dict):
    df = sparql_to_df(res)
    df["dob"] = pd.to_datetime(df.get("dob"), errors="coerce", utc=True)
    df["height"] = pd.to_numeric(df.get("height"), errors="coerce")
    df["age"] = ((pd.Timestamp.now(tz="UTC") - df["dob"]).dt.days / 365.25).round(1)
    # gộp nhiều vị trí/quốc tịch của cùng 1 cầu thủ thành list
    agg = (df.groupby(["player_qid", "playerLabel"], dropna=False)
             .agg(clubs=("clubLabel", lambda s: sorted(set(s.dropna()))),
                  positions=("positionLabel", lambda s: sorted(set(s.dropna()))),
                  countries=("countryLabel", lambda s: sorted(set(s.dropna()))),
                  dob=("dob", "first"), height=("height", "first"),
                  age=("age", "first"))
             .reset_index())
    agg["clubs"] = agg["clubs"].astype(str)
    agg["positions"] = agg["positions"].astype(str)
    agg["countries"] = agg["countries"].astype(str)
    put_parquet(f"silver/dim/wd_players/ingest_date={D}/part-0.parquet", agg, SRC)
    print(f"  ✓ {len(agg)} cầu thủ (từ {len(df)} dòng thô)")
    return agg


def build_stadiums(res: dict):
    df = sparql_to_df(res)
    df["capacity"] = pd.to_numeric(df.get("capacity"), errors="coerce")
    if "coord" in df:
        df[["lon", "lat"]] = df["coord"].apply(lambda s: pd.Series(parse_point(s)))
    df = df.drop_duplicates("venueLabel").sort_values("capacity", ascending=False)
    put_parquet(f"silver/dim/wd_stadiums/ingest_date={D}/part-0.parquet", df, SRC)
    print(f"  ✓ {len(df)} sân vận động, sức chứa lớn nhất: "
          f"{df.capacity.max():,.0f}")
    return df


if __name__ == "__main__":
    print("[1/2] bronze: chạy 4 query SPARQL")
    res = ingest_all()

    print("[2/2] silver")
    build_clubs(res["pl_clubs"])
    build_players(res["pl_players"])
    build_stadiums(res["pl_stadiums"])

    summary("bronze/wikidata/")
    summary("silver/dim/")
```

## 5. Kết quả mong đợi

```
[1/2] bronze: chạy 4 query SPARQL
  · chạy query 'pl_clubs'...
  ✓ s3://football-lake/bronze/wikidata/sparql/query=pl_clubs/ingest_date=2026-09-16/result.json.gz  (8,441 B, ...)
    -> 24 dòng
  · chạy query 'pl_players'...
    -> 2,847 dòng
  · chạy query 'pl_stadiums'...
    -> 26 dòng
  · chạy query 'pl_managers'...
    -> 21 dòng
[2/2] silver
  ✓ 20 CLB
  ✓ 1,204 cầu thủ (từ 2,847 dòng thô)
  ✓ 22 sân vận động, sức chứa lớn nhất: 74,310

[summary] bronze/wikidata/: 4 objects, 0.94 MB
[summary] silver/dim/: 3 objects, 0.41 MB
```

Lưu ý chênh lệch **2.847 dòng thô → 1.204 cầu thủ**: đó là hệ quả của SPARQL trả về tích Descartes khi một cầu thủ có nhiều vị trí × nhiều quốc tịch. Xử lý đúng chuyện này là phần khó nhất của nguồn này.

## 6. Truy vấn kiểm chứng

```sql
-- Sân vận động PL xếp theo sức chứa, kèm tọa độ
SELECT venueLabel AS san, cityLabel AS thanh_pho,
       capacity AS suc_chua, ROUND(lat,4) AS lat, ROUND(lon,4) AS lon
FROM read_parquet('s3://football-lake/silver/dim/wd_stadiums/**/*.parquet')
WHERE capacity IS NOT NULL
ORDER BY capacity DESC;

-- Phân bố quốc tịch cầu thủ PL
SELECT countries, COUNT(*) AS so_cau_thu
FROM read_parquet('s3://football-lake/silver/dim/wd_players/**/*.parquet')
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- CLB lâu đời nhất
SELECT clubLabel, YEAR(inception) AS nam_thanh_lap,
       venueLabel AS san_nha, capacity
FROM read_parquet('s3://football-lake/silver/dim/wd_clubs/**/*.parquet')
WHERE inception IS NOT NULL
ORDER BY inception LIMIT 10;
```

Mẹo hay: có tọa độ sân rồi, bạn tính được **khoảng cách di chuyển của đội khách** cho từng trận — một feature thật sự hữu ích cho model dự đoán mà không nguồn nào khác trong bộ 8 cung cấp được.

```sql
-- Khoảng cách giữa 2 sân (công thức haversine, đơn vị km)
SELECT a.venueLabel AS san_a, b.venueLabel AS san_b,
  ROUND(6371 * 2 * ASIN(SQRT(
      POWER(SIN(RADIANS(b.lat - a.lat)/2), 2) +
      COS(RADIANS(a.lat)) * COS(RADIANS(b.lat)) *
      POWER(SIN(RADIANS(b.lon - a.lon)/2), 2))), 1) AS km
FROM read_parquet('s3://football-lake/silver/dim/wd_stadiums/**/*.parquet') a
CROSS JOIN read_parquet('s3://football-lake/silver/dim/wd_stadiums/**/*.parquet') b
WHERE a.venueLabel < b.venueLabel AND a.lat IS NOT NULL AND b.lat IS NOT NULL
ORDER BY km DESC LIMIT 10;
```

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `403 Forbidden` | Thiếu User-Agent mô tả | Đặt UA có tên project + email liên hệ |
| `429 Too Many Requests` | Gọi quá nhanh | `time.sleep(2)` giữa các query; endpoint công cộng có quota |
| Timeout sau 60s | Query quá phức tạp, thiếu `LIMIT` | Thêm `LIMIT`, bớt `OPTIONAL`, hoặc tách thành nhiều query nhỏ |
| Số dòng nhiều bất thường | Tích Descartes do nhiều `OPTIONAL` | Groupby + aggregate như trong script |
| Cột `xxxLabel` toàn QID thay vì tên | Quên `SERVICE wikibase:label` | Thêm dòng service, hoặc đảm bảo biến đặt đúng tên `?xLabel` |
| Thiếu CLB mới thăng hạng | Wikidata cập nhật bởi cộng đồng, có độ trễ | Chấp nhận; đối chiếu với FPL bootstrap (file 01) |

## 8. Mở rộng

- Query thêm: lịch sử chuyển nhượng (`P54` kèm qualifier `P580`/`P582` = từ ngày/đến ngày), danh hiệu (`P166`), số lần khoác áo đội tuyển.
- Dùng QID làm **khóa chính chuẩn** cho dimension cầu thủ, rồi map tên từ 7 nguồn kia về QID → giải quyết triệt để bài toán "Man United vs Manchester United".
- Kết hợp tọa độ sân với dữ liệu thời tiết lịch sử (Open-Meteo API miễn phí) → thêm một nguồn thứ 9 với chi phí gần như bằng 0.
