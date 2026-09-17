# 07 — Reddit + News RSS (text phi cấu trúc) → MinIO

**Kiểu ingest:** OAuth API (Reddit) + XML feed (RSS), dữ liệu văn bản
**Tần suất:** 2–4 lần/ngày (tin tức có tính thời điểm)
**Độ khó:** ★★★☆☆ — dễ code, khó ở **liên kết text với entity** trong lake

---

## 1. Vì sao chọn nguồn này

Bảy nguồn kia đều là số. Nguồn này là **văn bản tự do** — thứ mà một data lake thật phải xử lý được, và là lý do người ta dùng lake thay vì warehouse.

Giá trị thực tế: dữ liệu số cho bạn biết *chuyện gì đã xảy ra*, văn bản cho bạn biết *người ta nghĩ gì về nó*. Ghép hai thứ lại bạn có được những phân tích như "chỉ số sentiment của fan Arsenal so với chuỗi kết quả" — không nguồn nào trong 7 nguồn kia làm được một mình.

Đây cũng là nguồn duy nhất sinh ra **JSONL** (một JSON mỗi dòng) — định dạng chuẩn cho dữ liệu text trong lake.

## 2. Hai nguồn con

### 2a. Reddit API

Cần đăng ký app:
1. Vào `reddit.com/prefs/apps` → Create app → chọn loại **script**
2. Lấy `client_id` (dưới tên app) và `client_secret`
3. Điền `.env`:
```ini
REDDIT_CLIENT_ID=xxxx
REDDIT_CLIENT_SECRET=xxxx
REDDIT_USER_AGENT=football-lake/0.1 by u/tenban
```

Subreddit hữu ích: `soccer`, `PremierLeague`, `FantasyPL`, `reddevils`, `Gunners`, `LiverpoolFC`, `chelseafc`, `coys`, `MCFC`.

Rate limit free: ~100 request/phút với OAuth — thoải mái cho nhu cầu này.

### 2b. News RSS

Không cần key, chỉ cần `feedparser`:

| Feed | URL |
|---|---|
| BBC Sport Football | `https://feeds.bbci.co.uk/sport/football/rss.xml` |
| Guardian Football | `https://www.theguardian.com/football/rss` |
| Sky Sports Football | `https://www.skysports.com/rss/12040` |
| ESPN Soccer | `https://www.espn.com/espn/rss/soccer/news` |
| Guardian Premier League | `https://www.theguardian.com/football/premierleague/rss` |

## 3. Layout trong lake

```
bronze/reddit/posts/subreddit=soccer/ingest_date=2026-09-16/hot.jsonl.gz
bronze/reddit/posts/subreddit=PremierLeague/ingest_date=2026-09-16/hot.jsonl.gz
bronze/reddit/comments/subreddit=soccer/ingest_date=2026-09-16/post_id=1abc234.jsonl.gz

bronze/rss/feeds/feed=bbc_football/ingest_date=2026-09-16/entries.jsonl.gz
bronze/rss/feeds/feed=guardian_football/ingest_date=2026-09-16/entries.jsonl.gz

silver/text/reddit_posts/ingest_date=2026-09-16/part-0.parquet
silver/text/reddit_comments/ingest_date=2026-09-16/part-0.parquet
silver/text/news_articles/ingest_date=2026-09-16/part-0.parquet
silver/text/entity_mentions/ingest_date=2026-09-16/part-0.parquet   ← liên kết với dim
```

**JSONL.gz ở bronze** thay vì JSON array: cho phép đọc từng dòng, append được, và mọi công cụ xử lý text đều hiểu.

## 4. Script ingest — `pipelines/p07_text.py`

```python
"""Ingest Reddit + RSS: text phi cấu trúc -> lake, kèm entity linking."""
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
SRC_RSS = "news-rss"
D = today()

SUBREDDITS = ["soccer", "PremierLeague", "FantasyPL"]
POSTS_PER_SUB = 100
COMMENTS_PER_POST = 50
TOP_POSTS_FOR_COMMENTS = 10

FEEDS = {
    "bbc_football": "https://feeds.bbci.co.uk/sport/football/rss.xml",
    "guardian_football": "https://www.theguardian.com/football/rss",
    "guardian_premierleague": "https://www.theguardian.com/football/premierleague/rss",
    "skysports_football": "https://www.skysports.com/rss/12040",
}


def put_jsonl_gz(key: str, records: list, source: str, meta=None):
    """Mỗi record 1 dòng JSON, gzip toàn bộ."""
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for r in records:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    return put_bytes(key, buf.getvalue(), source,
                     content_type="application/gzip",
                     meta={**(meta or {}), "records": len(records)})


# ================= REDDIT =================
def reddit_client():
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "football-lake/0.1"),
        check_for_async=False,
    )


def ingest_reddit(r):
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

        # lấy comment của các post nóng nhất
        top = sorted(posts, key=lambda x: x["num_comments"],
                     reverse=True)[:TOP_POSTS_FOR_COMMENTS]
        for post in top:
            try:
                sm = r.submission(id=post["post_id"])
                sm.comments.replace_more(limit=0)   # bỏ nút "load more"
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
    print(f"  ✓ tổng {len(all_posts)} post")
    return all_posts


# ================= RSS =================
def ingest_rss():
    all_entries = []
    for name, url in FEEDS.items():
        try:
            raw = get(url).content
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


# ================= SILVER =================
def clean_text(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = re.sub(r"http\S+", " ", s)          # bỏ URL
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def build_reddit_silver(posts: list):
    df = pd.DataFrame(posts)
    if df.empty:
        return df
    df["created_utc"] = pd.to_datetime(df["created_utc"])
    df["text"] = (df["title"].fillna("") + " " + df["selftext"].fillna("")).map(clean_text)
    df["text_len"] = df["text"].str.len()
    df["word_count"] = df["text"].str.split().str.len()
    df["engagement"] = df["score"] + df["num_comments"] * 2
    put_parquet(f"silver/text/reddit_posts/ingest_date={D}/part-0.parquet",
                df, SRC_REDDIT)
    print(f"  ✓ {len(df)} post -> silver")
    return df


def build_news_silver(entries: list):
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


# ---- entity linking: nối text với dimension đội bóng ----
TEAM_PATTERNS = {
    "Arsenal": r"\b(arsenal|gunners|afc)\b",
    "Chelsea": r"\b(chelsea|blues|cfc)\b",
    "Liverpool": r"\b(liverpool|lfc|reds)\b",
    "Manchester City": r"\b(man city|manchester city|mcfc|cityzens)\b",
    "Manchester United": r"\b(man utd|man united|manchester united|mufc|red devils)\b",
    "Tottenham Hotspur": r"\b(tottenham|spurs|thfc)\b",
    "Newcastle United": r"\b(newcastle|nufc|magpies)\b",
    "Aston Villa": r"\b(aston villa|avfc|villa)\b",
    "West Ham United": r"\b(west ham|whufc|hammers)\b",
    "Everton": r"\b(everton|efc|toffees)\b",
    "Brighton": r"\b(brighton|bhafc|seagulls)\b",
    "Nottingham Forest": r"\b(nottingham forest|nffc|forest)\b",
}

SENTIMENT_LEXICON = {
    "pos": r"\b(brilliant|superb|excellent|amazing|clinical|dominant|"
           r"deserved|masterclass|world class|outstanding)\b",
    "neg": r"\b(shocking|awful|terrible|disgrace|woeful|abysmal|"
           r"clueless|embarrassing|disaster|shambles)\b",
}


def build_entity_mentions(reddit_df, news_df):
    """
    Quét text tìm tên đội -> tạo bảng cầu nối giữa lớp text và lớp dimension.
    Đây là mảnh ghép làm cho nguồn text 'nói chuyện' được với 7 nguồn kia.
    """
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
                    "doc_type": kind,
                    "doc_id": row.get(id_col),
                    "team": team,
                    "mentions": n,
                    "ts": row.get(ts_col),
                    "pos_words": pos,
                    "neg_words": neg,
                    "sentiment_score": (pos - neg) / max(pos + neg, 1),
                    "engagement": row.get("engagement", 0),
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


if __name__ == "__main__":
    print("[1/4] Reddit")
    try:
        posts = ingest_reddit(reddit_client())
    except Exception as e:
        print(f"  ! bỏ qua Reddit ({e}) — kiểm tra credential trong .env")
        posts = []

    print("[2/4] RSS")
    entries = ingest_rss()

    print("[3/4] silver: text")
    rdf = build_reddit_silver(posts)
    ndf = build_news_silver(entries)

    print("[4/4] silver: entity linking")
    build_entity_mentions(rdf, ndf)

    summary("bronze/reddit/")
    summary("bronze/rss/")
    summary("silver/text/")
```

## 5. Kết quả mong đợi

```
[1/4] Reddit
  · r/soccer
  ✓ s3://football-lake/bronze/reddit/posts/subreddit=soccer/ingest_date=2026-09-16/hot.jsonl.gz  (48,221 B, ...)
  ✓ .../comments/subreddit=soccer/ingest_date=2026-09-16/post_id=1nabc23.jsonl.gz  (12,004 B, ...)
  · r/PremierLeague
  · r/FantasyPL
  ✓ tổng 300 post
[2/4] RSS
  · bbc_football: 42 bài
  · guardian_football: 58 bài
  · guardian_premierleague: 30 bài
  · skysports_football: 25 bài
[3/4] silver: text
  ✓ 300 post -> silver
  ✓ 148 bài báo -> silver
[4/4] silver: entity linking
  ✓ 217 mention, 12 đội được nhắc

[summary] bronze/reddit/: 33 objects, 1.82 MB
[summary] bronze/rss/: 4 objects, 0.21 MB
[summary] silver/text/: 4 objects, 1.34 MB
```

Chạy pipeline này **mỗi ngày** trong 2-3 tuần, bạn sẽ có time series sentiment thật sự dùng được — đó là lúc nguồn này phát huy giá trị.

## 6. Truy vấn kiểm chứng

```sql
-- Đội nào được bàn tán nhiều nhất, và tâm lý ra sao?
SELECT team,
       SUM(mentions)                     AS tong_mention,
       COUNT(DISTINCT doc_id)            AS so_bai,
       ROUND(AVG(sentiment_score), 3)    AS sentiment_tb,
       SUM(engagement)                   AS tong_tuong_tac
FROM read_parquet('s3://football-lake/silver/text/entity_mentions/**/*.parquet')
GROUP BY team ORDER BY tong_mention DESC;

-- Post nào gây tranh cãi nhất (nhiều comment nhưng upvote ratio thấp)?
SELECT subreddit, title, score, num_comments, upvote_ratio
FROM read_parquet('s3://football-lake/silver/text/reddit_posts/**/*.parquet')
WHERE num_comments > 100
ORDER BY upvote_ratio ASC LIMIT 10;

-- GHÉP NGUỒN: sentiment có đi theo kết quả trận không?
-- (cần đã chạy file 03 để có silver/matches/fd_matches)
SELECT m.home_team, m.match_date, m.result,
       ROUND(AVG(e.sentiment_score), 3) AS sentiment_quanh_tran
FROM read_parquet('s3://football-lake/silver/matches/fd_matches/division=E0/**/*.parquet') m
JOIN read_parquet('s3://football-lake/silver/text/entity_mentions/**/*.parquet') e
  ON e.team = m.home_team
 AND e.ts BETWEEN m.match_date AND m.match_date + INTERVAL 2 DAY
GROUP BY 1, 2, 3
ORDER BY m.match_date DESC LIMIT 20;
```

Truy vấn cuối là **thứ đáng khoe nhất trong cả 8 pipeline**: nó nối dữ liệu số (CSV từ 2003) với dữ liệu text (Reddit hôm nay) thông qua một entity chung. Đó chính là mục đích tồn tại của data lake.

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `401 Unauthorized` (Reddit) | Sai client_id/secret, hoặc chọn nhầm loại app | Phải chọn loại **script** khi tạo app |
| `429 Too Many Requests` | Vượt rate limit | `praw` tự xử lý phần lớn; thêm `time.sleep` giữa các post |
| RSS trả HTML thay vì XML | Feed đã đổi URL hoặc chặn bot | Đặt User-Agent, kiểm tra URL bằng trình duyệt |
| `published` parse ra NaT | Mỗi feed dùng format khác nhau | `pd.to_datetime(errors="coerce", utc=True)` đã xử lý |
| Mention sai ("forest" khớp bài về rừng) | Regex quá lỏng | Siết pattern, hoặc đòi hỏi ngữ cảnh bóng đá trong cùng câu |
| `tags` là list, pyarrow báo lỗi | Kiểu lồng | Đã `astype(str)`; cách tốt hơn là dùng `pa.list_(pa.string())` |

## 8. Lưu ý về đạo đức và pháp lý

- Reddit API có điều khoản sử dụng. Dự án học tập/nghiên cứu thường được chấp nhận; **đừng** phát tán lại nội dung người dùng hay dùng cho mục đích thương mại mà không xem kỹ ToS.
- Trường `author` là thông tin định danh. Với dự án phân tích tổng hợp, nên hash nó: `hashlib.sha256(author.encode()).hexdigest()[:16]`.
- RSS được thiết kế để máy đọc — lấy tiêu đề và tóm tắt là hoàn toàn bình thường. Nhưng **đừng crawl toàn văn bài báo** rồi lưu lại, đó là vấn đề bản quyền. Lưu link, đọc ở nguồn.

## 9. Mở rộng

- Thay lexicon thủ công bằng model thật: `cardiffnlp/twitter-roberta-base-sentiment` qua `transformers` → sentiment chính xác hơn nhiều.
- Trích xuất tên cầu thủ bằng NER (spaCy `en_core_web_sm`) rồi map về QID Wikidata (file 06) → entity linking đúng nghĩa thay vì regex.
- Thêm nguồn: bình luận YouTube trên video highlight (YouTube Data API, free quota 10.000 đơn vị/ngày).
- Chạy hàng ngày qua cron, sau 1 mùa giải bạn có bộ corpus text bóng đá có nhãn thời gian — tài sản hiếm.
