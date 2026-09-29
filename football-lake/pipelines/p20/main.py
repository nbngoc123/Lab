"""
p20: Ingest Wikipedia (Full-text) & Google News RSS -> MinIO.
Wikipedia (CC BY-SA) cho bách khoa, chiến thuật.
Google News cho tin tức báo chí tổng hợp tiếng Việt và Anh.
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.
"""
import gzip
import io
import json
import re
import time
import urllib.parse
import feedparser

from lake.minio_io import put_bytes, put_json_gz, exists, today
from lake.http import get

SRC_WIKI = "wikipedia-api"
SRC_NEWS = "google-news"
D = today()
TEST_MODE = False

# ---------------------------------------------------------------------------
# Cấu hình
# ---------------------------------------------------------------------------
WIKI_PAGES = {
    "team": [
        "Arsenal F.C.", "Manchester United F.C.", "Chelsea F.C.", 
        "Liverpool F.C.", "Manchester City F.C.", "Real Madrid CF", 
        "FC Barcelona", "FC Bayern Munich"
    ],
    "tactic": [
        "Gegenpressing", "Tiki-taka", "Total Football", "Catenaccio", 
        "Formation (association football)", "Zona mista"
    ],
    "history": [
        "The Invincibles (football)", "1999 UEFA Champions League final", 
        "2005 UEFA Champions League final", "Premier League"
    ]
}

NEWS_QUERIES = [
    # Tiếng Việt
    {"q": "Bóng đá Ngoại hạng Anh", "lang": "vi", "hl": "vi", "gl": "VN", "ceid": "VN:vi"},
    {"q": "Manchester United", "lang": "vi", "hl": "vi", "gl": "VN", "ceid": "VN:vi"},
    {"q": "Arsenal", "lang": "vi", "hl": "vi", "gl": "VN", "ceid": "VN:vi"},
    {"q": "Real Madrid", "lang": "vi", "hl": "vi", "gl": "VN", "ceid": "VN:vi"},
    {"q": "Đội tuyển Việt Nam", "lang": "vi", "hl": "vi", "gl": "VN", "ceid": "VN:vi"},
    # Tiếng Anh
    {"q": "Premier League", "lang": "en", "hl": "en-US", "gl": "US", "ceid": "US:en"},
    {"q": "Champions League", "lang": "en", "hl": "en-US", "gl": "US", "ceid": "US:en"},
]


# ---------------------------------------------------------------------------
# Wikipedia
# ---------------------------------------------------------------------------
def get_partitions_wiki() -> list[dict]:
    partitions = []
    langs = ["en", "vi"]
    for entity_type, titles in WIKI_PAGES.items():
        if TEST_MODE: titles = titles[:1]
        for title in titles:
            for lang in langs:
                partitions.append({"entity_type": entity_type, "title": title, "lang": lang})
    return partitions

def ingest_wiki(partition: dict) -> str:
    entity_type = partition["entity_type"]
    title = partition["title"]
    lang = partition["lang"]
    
    safe_title = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_")
    key = f"bronze/wikipedia_articles/entity={entity_type}/lang={lang}/article={safe_title}/fetched_date={D}/content.json.gz"
    
    if exists(key):
        print(f"  · [Wiki] {title} ({lang}) đã có.")
        return key

    url = f"https://{lang}.wikipedia.org/w/api.php?action=query&prop=extracts&explaintext=1&titles={urllib.parse.quote(title)}&format=json"
    try:
        resp = get(url, timeout=10).json()
        pages = resp.get("query", {}).get("pages", {})
        for page_id, page_data in pages.items():
            if page_id == "-1":
                continue # Không tìm thấy
            
            ext = page_data.get("extract", "")
            if ext:
                doc = {
                    "title": page_data.get("title"),
                    "entity_type": entity_type,
                    "lang": lang,
                    "page_id": page_id,
                    "extract": ext,
                    "word_count": len(ext.split())
                }
                
                raw = json.dumps(doc, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                buf = io.BytesIO()
                with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
                    gz.write(raw)
                put_bytes(key, buf.getvalue(), SRC_WIKI, content_type="application/gzip", meta={"title": doc["title"], "lang": lang})
                print(f"  ✓ [Wiki] {title} ({lang})")
                
    except Exception as e:
        print(f"    ! Lỗi Wiki {title} ({lang}): {e}")
        
    time.sleep(0.5)
    return key


# ---------------------------------------------------------------------------
# Google News RSS
# ---------------------------------------------------------------------------
def get_partitions_news() -> list[dict]:
    return NEWS_QUERIES[:2] if TEST_MODE else NEWS_QUERIES

def ingest_news(partition: dict) -> str:
    q_cfg = partition
    q = urllib.parse.quote(q_cfg["q"])
    safe_q = re.sub(r"[^a-zA-Z0-9]+", "_", q_cfg["q"]).strip("_")
    
    key = f"bronze/rss/google_news/query={safe_q}/lang={q_cfg['lang']}/ingest_date={D}/entries.jsonl.gz"
    if exists(key):
        print(f"  · [News] {q_cfg['q']} ({q_cfg['lang']}) đã có.")
        return key
        
    url = f"https://news.google.com/rss/search?q={q}&hl={q_cfg['hl']}&gl={q_cfg['gl']}&ceid={q_cfg['ceid']}"
    try:
        raw = get(url, timeout=15).content
        parsed = feedparser.parse(raw)
        entries = []
        for e in parsed.entries:
            entries.append({
                "feed": "google-news",
                "query": q_cfg["q"],
                "lang": q_cfg["lang"],
                "title": e.get("title"),
                "summary": re.sub(r"<[^>]+>", "", e.get("summary", "")),
                "link": e.get("link"),
                "published": e.get("published"),
                "source": (e.get("source") or {}).get("title", ""),
                "guid": e.get("id") or e.get("link"),
            })
        
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
            for r in entries:
                gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
        put_bytes(key, buf.getvalue(), SRC_NEWS, content_type="application/gzip", meta={"query": q_cfg["q"], "count": len(entries)})
        print(f"  ✓ [News] {q_cfg['q']} ({q_cfg['lang']}): {len(entries)} bài")
        
    except Exception as e:
        print(f"  ! Lỗi RSS {q_cfg['q']}: {e}")
        
    return key
