"""
p21: Ingest YouTube Data API (Search & Comments) -> MinIO.
Lấy video highlights và comment của fan (có nhiều tiếng Việt).
Yêu cầu: YOUTUBE_API_KEY trong .env
"""
import gzip
import io
import json
import os
import re
import time
from datetime import datetime, timezone
import urllib.parse

import pandas as pd
from lake.minio_io import put_bytes, put_parquet, today, summary
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "youtube-api"
D = today()

YT_QUERIES = [
    "Ngoại hạng Anh highlight", 
    "Arsenal highlight", 
    "Manchester United tin tức",
    "Real Madrid highlight",
    "Bóng đá Việt Nam highlight"
]

MAX_VIDEOS_PER_QUERY = 2 if TEST_MODE else 20
MAX_COMMENTS_PER_VIDEO = 20 if TEST_MODE else 100   # Issue #8: giới hạn thực tế của API là 100
MAX_COMMENT_PAGES = 1 if TEST_MODE else 3          # tối đa 3 trang × 100 = 300 comments/video

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def put_jsonl_gz(key: str, records: list, source: str, meta=None):
    if not records: return None
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for r in records:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def clean_text(s: str) -> str:
    if not isinstance(s, str): return ""
    s = re.sub(r"http\S+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# ---------------------------------------------------------------------------
# YouTube API
# ---------------------------------------------------------------------------
def ingest_youtube():
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("  ! Thiếu YOUTUBE_API_KEY trong .env")
        return [], []
        
    all_videos = []
    all_comments = []
    
    for q in YT_QUERIES:
        print(f"\n  [YT] Search: {q}")
        safe_q = re.sub(r"[\W]+", "_", q, flags=re.UNICODE).strip("_")  # Issue C: giữ Unicode
        search_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={urllib.parse.quote(q)}&type=video&maxResults={MAX_VIDEOS_PER_QUERY}&key={api_key}"
        
        try:
            search_resp = get(search_url).json()
            if "error" in search_resp:
                print(f"    ! Lỗi API: {search_resp['error']['message']}")
                continue
                
            videos = []
            for item in search_resp.get("items", []):
                vid_id = item["id"]["videoId"]
                snippet = item["snippet"]
                videos.append({
                    "video_id": vid_id,
                    "query": q,
                    "published_at": snippet["publishedAt"],
                    "channel_id": snippet["channelId"],
                    "title": snippet["title"],
                    "description": snippet["description"],
                    "channel_title": snippet["channelTitle"]
                })
                
                # Fetch comments — Issue #8: pagination + error check
                cmts = []
                page_token = ""
                for _page in range(MAX_COMMENT_PAGES):
                    cmt_url = (
                        f"https://www.googleapis.com/youtube/v3/commentThreads"
                        f"?part=snippet&videoId={vid_id}"
                        f"&maxResults={MAX_COMMENTS_PER_VIDEO}&key={api_key}"
                        + (f"&pageToken={page_token}" if page_token else "")
                    )
                    cmt_resp = get(cmt_url).json()
                    if "error" in cmt_resp:
                        print(f"    ! Comment API lỗi cho {vid_id}: "
                              f"{cmt_resp['error'].get('message', '')}")
                        break
                    for c_item in cmt_resp.get("items", []):
                        c_snippet = c_item["snippet"]["topLevelComment"]["snippet"]
                        cmts.append({
                            "comment_id": c_item["id"],
                            "video_id": vid_id,
                            "author": c_snippet.get("authorDisplayName", ""),
                            "text": c_snippet.get("textOriginal", ""),
                            "like_count": c_snippet.get("likeCount", 0),
                            "published_at": c_snippet.get("publishedAt", "")
                        })
                    page_token = cmt_resp.get("nextPageToken", "")
                    if not page_token:
                        break
                
                if cmts:
                    put_jsonl_gz(
                        f"bronze/youtube/comments/video_id={vid_id}/ingest_date={D}/comments.jsonl.gz",
                        cmts, SRC, meta={"video_id": vid_id, "count": len(cmts)}
                    )
                    all_comments.extend(cmts)
                time.sleep(0.5)
                
            if videos:
                put_jsonl_gz(
                    f"bronze/youtube/videos/query={safe_q}/ingest_date={D}/videos.jsonl.gz",
                    videos, SRC, meta={"query": q, "count": len(videos)}
                )
                all_videos.extend(videos)
                
        except Exception as e:
            print(f"    ! Lỗi khi crawl {q}: {e}")
            
    print(f"\n  ✓ Tổng: {len(all_videos)} videos, {len(all_comments)} comments")
    return all_videos, all_comments

# ---------------------------------------------------------------------------
# SILVER
# ---------------------------------------------------------------------------
def build_yt_silver(videos, comments):
    if videos:
        df_v = pd.DataFrame(videos)
        df_v["published_ts"] = pd.to_datetime(df_v["published_at"], errors="coerce", utc=True)
        df_v["title"] = df_v["title"].map(clean_text)
        df_v["ingest_date"] = D
        put_parquet(
            f"silver/text/youtube_videos/ingest_date={D}/part-0.parquet",
            df_v, SRC, meta={"rows": len(df_v)}
        )
        print(f"  ✓ {len(df_v)} videos -> silver")
        
    if comments:
        df_c = pd.DataFrame(comments)
        df_c["published_ts"] = pd.to_datetime(df_c["published_at"], errors="coerce", utc=True)
        df_c["text"] = df_c["text"].map(clean_text)
        df_c["ingest_date"] = D
        put_parquet(
            f"silver/text/youtube_comments/ingest_date={D}/part-0.parquet",
            df_c, SRC, meta={"rows": len(df_c)}
        )
        print(f"  ✓ {len(df_c)} comments -> silver")

def run_pipeline():
    print("[1/2] YouTube Data API")
    videos, comments = ingest_youtube()
    
    print("\n[2/2] Silver build")
    build_yt_silver(videos, comments)
    
    summary("bronze/youtube/")
    summary("silver/text/youtube_videos/")
    summary("silver/text/youtube_comments/")

if __name__ == "__main__":
    run_pipeline()
