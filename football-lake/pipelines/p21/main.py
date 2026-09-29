"""
p21: Ingest YouTube Data API (Search & Comments) -> MinIO.
Lấy video highlights và comment của fan (có nhiều tiếng Việt).
Yêu cầu: YOUTUBE_API_KEY trong .env
Chỉ lấy Bronze Layer. Hỗ trợ Dynamic Task Mapping.
"""
import gzip
import io
import json
import os
import re
import time
import urllib.parse
from lake.minio_io import put_bytes, exists, today
from lake.http import get

TEST_MODE = os.getenv("TEST_MODE") == "1"

SRC = "youtube-api"
D = today()

YT_QUERIES = [
    "Tin tức Ngoại hạng Anh", 
    "Tin tức bóng đá La Liga", 
    "Tin tức bóng đá Serie A",
    "Tin tức bóng đá Bundesliga",
    "Tin tức bóng đá Ligue 1",
    "Điểm tin bóng đá thế giới",
    "Highlight bóng đá quốc tế mới nhất",
    "Bản tin bóng đá 24h"
]

MAX_VIDEOS_PER_QUERY = 2 if TEST_MODE else 20
MAX_COMMENTS_PER_VIDEO = 20 if TEST_MODE else 100
MAX_COMMENT_PAGES = 1 if TEST_MODE else 3

def put_jsonl_gz(key: str, records: list, source: str, meta=None):
    if not records: return None
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        for r in records:
            gz.write((json.dumps(r, ensure_ascii=False) + "\n").encode("utf-8"))
    return put_bytes(key, buf.getvalue(), source, content_type="application/gzip", meta=meta)

def get_partitions() -> list[dict]:
    return [{"query": q} for q in YT_QUERIES]

def ingest(partition: dict) -> str:
    q = partition["query"]
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("  ! Thiếu YOUTUBE_API_KEY trong .env")
        return ""
        
    safe_q = re.sub(r"[\W]+", "_", q, flags=re.UNICODE).strip("_")
    v_key = f"raw/youtube/videos/query={safe_q}/ingest_date={D}/videos.jsonl.gz"
    
    if exists(v_key):
        print(f"  · [YT] {q} đã có.")
        return v_key

    search_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={urllib.parse.quote(q)}&type=video&maxResults={MAX_VIDEOS_PER_QUERY}&key={api_key}"
    
    try:
        search_resp = get(search_url).json()
        if "error" in search_resp:
            print(f"    ! Lỗi API: {search_resp['error']['message']}")
            return ""
            
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
                    f"raw/youtube/comments/video_id={vid_id}/ingest_date={D}/comments.jsonl.gz",
                    cmts, SRC, meta={"video_id": vid_id, "count": len(cmts)}
                )
            time.sleep(0.5)
            
        if videos:
            put_jsonl_gz(v_key, videos, SRC, meta={"query": q, "count": len(videos)})
            print(f"  ✓ [YT] {q}: {len(videos)} videos")
            
    except Exception as e:
        print(f"    ! Lỗi khi crawl {q}: {e}")
        
    return v_key
