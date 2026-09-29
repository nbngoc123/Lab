"""Ingest Wikidata qua SPARQL: Chỉ lấy JSON raw -> MinIO (Bronze Layer)."""
import time
import os
from lake.minio_io import put_json_gz, exists, today, S3, BUCKET
from lake.http import SESSION

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "wikidata"
D = today()
ENDPOINT = "https://query.wikidata.org/sparql"

UA = "football-lake/0.1 (educational data engineering project; contact: admin@football-lake.local)"

QUERIES = {
    "pl_clubs": """
SELECT ?club ?clubLabel ?inception ?venue ?venueLabel ?capacity
       ?coord ?coachLabel ?websiteURL
WHERE {
  ?club wdt:P118 wd:Q9448 .
  OPTIONAL { ?club wdt:P571  ?inception }
  OPTIONAL { ?club wdt:P115  ?venue .
             OPTIONAL { ?venue wdt:P1083 ?capacity }
             OPTIONAL { ?venue wdt:P625  ?coord } }
  OPTIONAL { ?club wdt:P286  ?coach }
  OPTIONAL { ?club wdt:P856  ?websiteURL }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""" + ("LIMIT 50" if TEST_MODE else ""),

    "pl_players": """
SELECT ?player ?playerLabel ?clubLabel ?dob ?height
       ?countryLabel ?positionLabel ?sexLabel
WHERE {
  ?club   wdt:P118 wd:Q9448 .
  ?player p:P54 ?membership .
  ?membership ps:P54 ?club .
  FILTER NOT EXISTS { ?membership pq:P582 ?endTime }
  ?player wdt:P106 wd:Q937857 .
  OPTIONAL { ?player wdt:P569  ?dob }
  OPTIONAL { ?player wdt:P2048 ?height }
  OPTIONAL { ?player wdt:P27   ?country }
  OPTIONAL { ?player wdt:P413  ?position }
  OPTIONAL { ?player wdt:P21   ?sex }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
""" + ("LIMIT 50" if TEST_MODE else ""),

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

def get_partitions() -> list[dict]:
    """Tạo danh sách partitions dựa trên các câu query."""
    return [{"query_name": name} for name in QUERIES.keys()]

def run_sparql(query: str) -> dict:
    r = SESSION.get(
        ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"Accept": "application/sparql-results+json", "User-Agent": UA},
        timeout=120,
    )
    r.raise_for_status()
    return r.json()

def ingest(partition: dict) -> str:
    name = partition["query_name"]
    key = f"bronze/wikidata/sparql/query={name}/ingest_date={D}/result.json.gz"
    
    if exists(key):
        print(f"  · {name} đã lấy trong ngày {D}, bỏ qua tải lại.")
        return key

    q = QUERIES[name]
    print(f"  · chạy query '{name}'...")
    res = run_sparql(q)
    n = len(res["results"]["bindings"])
    
    put_json_gz(key, res, SRC, meta={"query": name, "rows": n})
    
    # lưu câu query để truy vết
    S3.put_object(Bucket=BUCKET, Key=f"_meta/wikidata/queries/{name}.rq",
                  Body=q.encode(), ContentType="text/plain")
                  
    print(f"  ✓ {name} -> {n} dòng")
    time.sleep(2) # rate limit bảo vệ server
    return key
