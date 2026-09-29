"""Ingest Wikidata qua SPARQL: dimension giàu cho lake bóng đá."""
import time
import pandas as pd
import os
from lake.minio_io import put_json_gz, put_parquet, today, summary, S3, BUCKET
from lake.http import SESSION

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "wikidata"
D = today()
ENDPOINT = "https://query.wikidata.org/sparql"

# Wikidata yêu cầu UA mô tả rõ — thiếu cái này sẽ bị 403
UA = "football-lake/0.1 (educational data engineering project; contact: admin@football-lake.local)"


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
""" + ("LIMIT 50" if TEST_MODE else ""),

    # ---- Cầu thủ đang thuộc các CLB PL (lọc theo P582 end time để tránh lấy alumni) ----
    "pl_players": """
SELECT ?player ?playerLabel ?clubLabel ?dob ?height
       ?countryLabel ?positionLabel ?sexLabel
WHERE {
  ?club   wdt:P118 wd:Q9448 .
  # P54 = member of sports team, chỉ lấy membership chưa có end date (vẫn đang thi đấu)
  ?player p:P54 ?membership .
  ?membership ps:P54 ?club .
  FILTER NOT EXISTS { ?membership pq:P582 ?endTime }   # không có end time
  ?player wdt:P106 wd:Q937857 .                        # nghề: cầu thủ bóng đá
  OPTIONAL { ?player wdt:P569  ?dob }
  OPTIONAL { ?player wdt:P2048 ?height }
  OPTIONAL { ?player wdt:P27   ?country }
  OPTIONAL { ?player wdt:P413  ?position }
  OPTIONAL { ?player wdt:P21   ?sex }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""" + ("LIMIT 50" if TEST_MODE else ""),

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

    print("[2/2] silver: build dimensions")
    build_clubs(res["pl_clubs"])
    build_players(res["pl_players"])
    build_stadiums(res["pl_stadiums"])
    # manager nằm luôn trong clubs ở data model này (bạn có thể mở rộng nếu cần)

    summary("bronze/wikidata/sparql/")
    summary("silver/dim/wd_")
