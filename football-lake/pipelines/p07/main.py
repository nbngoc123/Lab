"""
Ingest Reddit (hot posts + comments) + News RSS -> MinIO.
Libraries: praw, feedparser (thêm vào requirements.txt)
"""
import gzip
import io
import json
import os
import re
import time
from datetime import datetime, timezone

import feedparser
import pandas as pd
import praw
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import get

SRC_REDDIT = "reddit-api"
SRC_RSS    = "news-rss"
D = today()

SUBREDDITS = [
    "soccer", "PremierLeague", "FantasyPL",
    "Gunners", "LiverpoolFC", "chelseafc", "reddevils", "MCFC", "coys",
    "BurntFelt",  # Newcastle
]
POSTS_PER_SUB        = 100
COMMENTS_PER_POST    = 50
TOP_POSTS_COMMENTS   = 10

FEEDS = {
    "bbc_football":           "https://feeds.bbci.co.uk/sport/football/rss.xml",
    "guardian_football":      "https://www.theguardian.com/football/rss",
    "guardian_premierleague": "https://www.theguardian.com/football/premierleague/rss",
    "skysports_football":     "https://www.skysports.com/rss/12040",
    "espn_soccer":            "https://www.espn.com/espn/rss/soccer/news",
}

# Regex đội bóng (dùng cho entity linking)
TEAM_PATTERNS = {
    "Arsenal":           r"\b(arsenal|gunners|afc)\b",
    "Chelsea":           r"\b(chelsea|blues|cfc)\b",
    "Liverpool":         r"\b(liverpool|lfc|reds)\b",
    "Manchester City":   r"\b(man city|manchester city|mcfc|cityzens)\b",
    "Manchester United": r"\b(man utd|man united|manchester united|mufc|red devils)\b",
    "Tottenham Hotspur": r"\b(tottenham|spurs|thfc)\b",
    "Newcastle United":  r"\b(newcastle|nufc|magpies)\b",
    "Aston Villa":       r"\b(aston villa|avfc|villa)\b",
    "West Ham United":   r"\b(west ham|whufc|hammers)\b",
    "Everton":           r"\b(everton|efc|toffees)\b",
    "Brighton":          r"\b(brighton|bhafc|seagulls)\b",
    "Nottingham Forest": r"\b(nottingham forest|nffc|forest)\b",
    "Brentford":         r"\b(brentford|bfc)\b",
    "Fulham":            r"\b(fulham|ffc|cottagers)\b",
    "Wolverhampton":     r"\b(wolves|wolverhampton|wba)\b",
    "Crystal Palace":    r"\b(crystal palace|cpfc|eagles)\b",
    "Bournemouth":       r"\b(bournemouth|afcb|cherries)\b",
    "Leicester City":    r"\b(leicester|lcfc|foxes)\b",
    "Ipswich Town":      r"\b(ipswich|itfc|tractor boys)\b",
    "Southampton":       r"\b(southampton|saints|scfc)\b",
}

SENTIMENT_LEXICON = {
    "pos": r"\b(brilliant|superb|excellent|amazing|clinical|dominant|"
           r"deserved|masterclass|world.class|outstanding|baller|goat|"
           r"incredible|fantastic|perfect|great|wonderful)\b",
    "neg": r"\b(shocking|awful|terrible|disgrace|woeful|abysmal|"
           r"clueless|embarrassing|disaster|shambles|pathetic|useless|"
           r"sacked|fired|crisis|worst|flop)\b",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def put_jsonl_gz(key: str, records: list, source: str, meta=None):
    """Mỗi record 1 dòng JSON, gzip toàn bộ."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for r in records:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    return put_bytes(key, buf.getvalue(), source,
                     content_type="application/gzip",
                     meta={**(meta or {}), "records": len(records)})


def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ---------------------------------------------------------------------------
# REDDIT
# ---------------------------------------------------------------------------
def reddit_client():
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "football-lake/0.1"),
        check_for_async=False,
    )


def ingest_reddit(r) -> list:
    all_posts = []
    for sub in SUBREDDITS:
        print(f"  · r/{sub}")
        posts = []
        for p in r.subreddit(sub).hot(limit=POSTS_PER_SUB):
            posts.append({
                "post_id": p.id,
                "subreddit": sub,
                "title": p.title,
                "selftext": p.selftext,
                "author": str(p.author) if p.author else None,
                "score": p.score,
                "upvote_ratio": p.upvote_ratio,
                "num_comments": p.num_comments,
                "created_utc": datetime.fromtimestamp(
                    p.created_utc, tz=timezone.utc).isoformat(),
                "url": p.url,
                "permalink": f"https://reddit.com{p.permalink}",
                "flair": p.link_flair_text,
                "is_self": p.is_self,
                "over_18": p.over_18,
            })
        put_jsonl_gz(
            f"bronze/reddit/posts/subreddit={sub}/ingest_date={D}/hot.jsonl.gz",
            posts, SRC_REDDIT, meta={"subreddit": sub})
        all_posts.extend(posts)

        # Lấy comment của các bài nóng nhất
        top = sorted(posts, key=lambda x: x["num_comments"],
                     reverse=True)[:TOP_POSTS_COMMENTS]
        for post in top:
            try:
                sm = r.submission(id=post["post_id"])
                sm.comments.replace_more(limit=0)
                cmts = []
                for c in sm.comments.list()[:COMMENTS_PER_POST]:
                    cmts.append({
                        "comment_id": c.id,
                        "post_id": post["post_id"],
                        "subreddit": sub,
                        "body": c.body,
                        "author": str(c.author) if c.author else None,
                        "score": c.score,
                        "created_utc": datetime.fromtimestamp(
                            c.created_utc, tz=timezone.utc).isoformat(),
                        "parent_id": c.parent_id,
                        "depth": c.depth,
                    })
                if cmts:
                    put_jsonl_gz(
                        f"bronze/reddit/comments/subreddit={sub}"
                        f"/ingest_date={D}/post_id={post['post_id']}.jsonl.gz",
                        cmts, SRC_REDDIT)
                time.sleep(0.6)
            except Exception as e:
                print(f"    ! comment lỗi cho {post['post_id']}: {e}")
    print(f"  ✓ tổng {len(all_posts)} post từ {len(SUBREDDITS)} subreddit")
    return all_posts


# ---------------------------------------------------------------------------
# RSS
# ---------------------------------------------------------------------------
def ingest_rss() -> list:
    all_entries = []
    for name, url in FEEDS.items():
        try:
            raw = get(url, timeout=15).content
        except Exception as e:
            print(f"  ! {name} lỗi: {e}")
            continue
        parsed = feedparser.parse(raw)
        entries = []
        for e in parsed.entries:
            entries.append({
                "feed": name,
                "title": e.get("title"),
                "summary": re.sub(r"<[^>]+>", "", e.get("summary", "")),
                "link": e.get("link"),
                "published": e.get("published"),
                "author": e.get("author"),
                "tags": [t.get("term") for t in e.get("tags", [])],
                "guid": e.get("id") or e.get("link"),
            })
        put_jsonl_gz(
            f"bronze/rss/feeds/feed={name}/ingest_date={D}/entries.jsonl.gz",
            entries, SRC_RSS, meta={"feed": name})
        print(f"  · {name}: {len(entries)} bài")
        all_entries.extend(entries)
    return all_entries


# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
def build_reddit_silver(posts: list) -> pd.DataFrame:
    df = pd.DataFrame(posts)
    if df.empty:
        return df
    df["created_utc"] = pd.to_datetime(df["created_utc"])
    df["text"] = (df["title"].fillna("") + " " + df["selftext"].fillna("")).map(clean_text)
    df["text_len"]   = df["text"].str.len()
    df["word_count"] = df["text"].str.split().str.len()
    df["engagement"] = df["score"] + df["num_comments"] * 2
    put_parquet(f"silver/text/reddit_posts/ingest_date={D}/part-0.parquet",
                df, SRC_REDDIT)
    print(f"  ✓ {len(df)} post -> silver")
    return df


def build_news_silver(entries: list) -> pd.DataFrame:
    df = pd.DataFrame(entries)
    if df.empty:
        return df
    df["published_ts"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    df["text"] = (df["title"].fillna("") + ". " + df["summary"].fillna("")).map(clean_text)
    df["tags"] = df["tags"].astype(str)
    df = df.drop_duplicates("guid")
    put_parquet(f"silver/text/news_articles/ingest_date={D}/part-0.parquet",
                df, SRC_RSS)
    print(f"  ✓ {len(df)} bài báo -> silver")
    return df


def build_entity_mentions(reddit_df, news_df) -> pd.DataFrame:
    """Entity linking: quét text -> tìm đội + sentiment."""
    rows = []
    sources = []
    if reddit_df is not None and not reddit_df.empty:
        sources.append(("reddit", reddit_df, "post_id", "created_utc"))
    if news_df is not None and not news_df.empty:
        sources.append(("news", news_df, "guid", "published_ts"))

    for kind, df, id_col, ts_col in sources:
        for _, row in df.iterrows():
            t = (row.get("text") or "").lower()
            if not t:
                continue
            pos = len(re.findall(SENTIMENT_LEXICON["pos"], t))
            neg = len(re.findall(SENTIMENT_LEXICON["neg"], t))
            for team, pat in TEAM_PATTERNS.items():
                n = len(re.findall(pat, t))
                if n == 0:
                    continue
                rows.append({
                    "doc_type":       kind,
                    "doc_id":         row.get(id_col),
                    "team":           team,
                    "mentions":       n,
                    "ts":             row.get(ts_col),
                    "pos_words":      pos,
                    "neg_words":      neg,
                    "sentiment_score": (pos - neg) / max(pos + neg, 1),
                    "engagement":     row.get("engagement", 0),
                })

    out = pd.DataFrame(rows)
    if out.empty:
        print("  ! không tìm thấy mention nào")
        return out
    out["ts"] = pd.to_datetime(out["ts"], errors="coerce", utc=True)
    put_parquet(f"silver/text/entity_mentions/ingest_date={D}/part-0.parquet",
                out, SRC_REDDIT)
    print(f"  ✓ {len(out)} mention, {out.team.nunique()} đội được nhắc")
    return out


def run_pipeline():
    print("[1/4] Reddit")
    try:
        posts = ingest_reddit(reddit_client())
    except Exception as e:
        print(f"  ! bỏ qua Reddit ({e}) — kiểm tra REDDIT_CLIENT_ID/SECRET trong .env")
        posts = []

    print("\n[2/4] RSS")
    entries = ingest_rss()

    print("\n[3/4] silver: text")
    rdf = build_reddit_silver(posts)
    ndf = build_news_silver(entries)

    print("\n[4/4] silver: entity linking + sentiment")
    build_entity_mentions(rdf, ndf)

    summary("bronze/reddit/")
    summary("bronze/rss/")
    summary("silver/text/")


if __name__ == "__main__":
    run_pipeline()
