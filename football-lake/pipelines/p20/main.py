"""
p20: Ingest Wikipedia (Full-text) & Google News RSS -> MinIO.
Wikipedia (CC BY-SA) cho bách khoa, chiến thuật.
Google News cho tin tức báo chí tổng hợp tiếng Việt và Anh.
"""
import gzip
import io
import json
import re
import time
from datetime import datetime, timezone
import urllib.parse

import feedparser
import pandas as pd
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import get

SRC_WIKI = "wikipedia-api"
SRC_NEWS = "google-news"
D = today()

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
        "Catenaccio", "Formation (association football)", "Zona mista"
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
# Helpers
# ---------------------------------------------------------------------------
def put_json_gz(key: str, obj, source: str, meta=None):
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(raw)
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def clean_text(s: str) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

TEST_MODE = False

# ---------------------------------------------------------------------------
# Wikipedia
# ---------------------------------------------------------------------------
def ingest_wikipedia():
    all_articles = []
    langs = ["en", "vi"]
    
    for entity_type, titles in WIKI_PAGES.items():
        if TEST_MODE: titles = titles[:1] # Lấy 1 trang mỗi loại để test
        print(f"\n  [Wiki] Entity: {entity_type}")
        for title in titles:
            for lang in langs:
                url = f"https://{lang}.wikipedia.org/w/api.php?action=query&prop=extracts&explaintext=1&titles={urllib.parse.quote(title)}&format=json"
                try:
                    resp = get(url, timeout=10).json()
                    pages = resp.get("query", {}).get("pages", {})
                    for page_id, page_data in pages.items():
                        if page_id == "-1":
                            continue # Không tìm thấy trang ở ngôn ngữ này
                        
                        ext = page_data.get("extract", "")
                        if not ext:
                            continue
                            
                        doc = {
                            "title": page_data.get("title"),
                            "entity_type": entity_type,
                            "lang": lang,
                            "page_id": page_id,
                            "extract": ext,
                            "word_count": len(ext.split())
                        }
                        all_articles.append(doc)
                        
                        safe_title = re.sub(r"[^a-zA-Z0-9]+", "_", doc["title"]).strip("_")
                        put_json_gz(
                            f"bronze/wikipedia_articles/entity={entity_type}/lang={lang}/article={safe_title}/fetched_date={D}/content.json.gz",
                            doc, SRC_WIKI, meta={"title": doc["title"], "lang": lang}
                        )
                except Exception as e:
                    print(f"    ! Lỗi Wiki {title} ({lang}): {e}")
                time.sleep(0.5)
    print(f"  ✓ Tổng {len(all_articles)} bài viết Wiki")
    return all_articles

# ---------------------------------------------------------------------------
# Google News RSS
# ---------------------------------------------------------------------------
def ingest_google_news():
    all_entries = []
    queries = NEWS_QUERIES[:2] if TEST_MODE else NEWS_QUERIES
    for q_cfg in queries:
        q = urllib.parse.quote(q_cfg["q"])
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
            
            safe_q = re.sub(r"[^a-zA-Z0-9]+", "_", q_cfg["q"]).strip("_")
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
                for r in entries:
                    gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
            put_bytes(
                f"bronze/rss/google_news/query={safe_q}/lang={q_cfg['lang']}/ingest_date={D}/entries.jsonl.gz",
                buf.getvalue(), SRC_NEWS, content_type="application/gzip", meta={"query": q_cfg["q"], "count": len(entries)}
            )
            print(f"  · {q_cfg['q']} ({q_cfg['lang']}): {len(entries)} bài")
            all_entries.extend(entries)
        except Exception as e:
            print(f"  ! Lỗi RSS {q_cfg['q']}: {e}")
            
    return all_entries

# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
def build_wiki_silver(articles):
    if not articles: return
    df = pd.DataFrame(articles)
    df["fetched_date"] = D
    for entity in df.entity_type.unique():
        sub_df = df[df.entity_type == entity].copy()
        put_parquet(
            f"silver/text/wiki_articles/entity={entity}/fetched_date={D}/part-0.parquet",
            sub_df, SRC_WIKI, meta={"rows": len(sub_df)}
        )
    print(f"  ✓ {len(df)} bài Wiki -> silver")

def build_news_silver(entries):
    if not entries: return
    df = pd.DataFrame(entries)
    df["published_ts"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    df["text"] = (df["title"].fillna("") + ". " + df["summary"].fillna("")).map(clean_text)
    df = df.drop_duplicates("guid")
    df["ingest_date"] = D
    put_parquet(
        f"silver/text/google_news_articles/ingest_date={D}/part-0.parquet",
        df, SRC_NEWS, meta={"rows": len(df)}
    )
    print(f"  ✓ {len(df)} tin tức Google News -> silver")

def run_wikipedia_pipeline():
    print("[Wikipedia] Bắt đầu lấy dữ liệu...")
    wiki_data = ingest_wikipedia()
    build_wiki_silver(wiki_data)
    summary("bronze/wikipedia_articles/")
    summary("silver/text/wiki_articles/")

def run_news_pipeline():
    print("[Google News] Bắt đầu lấy dữ liệu...")
    news_data = ingest_google_news()
    build_news_silver(news_data)
    summary("bronze/rss/google_news/")
    summary("silver/text/google_news_articles/")

def run_pipeline():
    run_wikipedia_pipeline()
    run_news_pipeline()

if __name__ == "__main__":
    run_pipeline()
