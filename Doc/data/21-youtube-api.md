# P21: YouTube Highlights & Fan Comments

## 1. Giới thiệu
Pipeline này tích hợp YouTube Data API v3 để kéo thông tin các video highlight bóng đá và toàn bộ các bình luận (comments) của fan. Nguồn dữ liệu này cung cấp bộ Corpus tiếng Việt cực lớn, tự nhiên và mang tính cảm xúc cao, rất phù hợp cho bài toán Fan Sentiment Analysis (Phân tích cảm xúc người hâm mộ).

## 2. Nguồn dữ liệu (Sources)
- **API**: `YouTube Data API v3` (Yêu cầu phải có `YOUTUBE_API_KEY` trong file `.env`).
- **Endpoint 1 (Search)**: Tìm kiếm các video mới nhất liên quan đến bóng đá (`/search`).
- **Endpoint 2 (Comments)**: Lấy luồng bình luận của từng video (`/commentThreads`).
- **Truy vấn (Queries)**: Các từ khóa phổ biến của fan Việt Nam:
  - "Ngoại hạng Anh highlight"
  - "Arsenal highlight"
  - "Manchester United tin tức"
  - "Real Madrid highlight"
  - "Bóng đá Việt Nam highlight"

## 3. Kiến trúc Data Lake

### Bronze Layer (Raw)
Lưu raw JSONL nén GZIP nguyên gốc từ API trả về:
- **Videos**: `bronze/youtube/videos/query={query}/ingest_date={YYYY-MM-DD}/videos.jsonl.gz`
- **Comments**: `bronze/youtube/comments/video_id={video_id}/ingest_date={YYYY-MM-DD}/comments.jsonl.gz`

### Silver Layer (Cleaned & Tabular)
Làm sạch văn bản, chuyển timestamp thành UTC datetime chuẩn, và gộp chung thành bảng Parquet:
- **Videos**: `silver/text/youtube_videos/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema*: video_id, query, published_ts, channel_id, channel_title, title, description.
- **Comments**: `silver/text/youtube_comments/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema*: comment_id, video_id, author, text (nội dung bình luận), like_count, published_ts.

## 4. Automation (Airflow DAG)
- **DAG**: `dag_p21_youtube`
- **Lịch trình**: `@daily` (Chạy hàng ngày để lấy video mới đăng và comment nóng hổi).
- **Giới hạn API**: Để tiết kiệm Quota của YouTube API, hiện tại cấu hình lấy `MAX_VIDEOS_PER_QUERY = 5` và `MAX_COMMENTS_PER_VIDEO = 50`. (Có thể tăng lên nếu nâng cấp Quota của Google Cloud).
