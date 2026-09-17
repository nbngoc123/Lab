# P20: Wikipedia Full Text & Google News RSS

## 1. Giới thiệu
Pipeline này chuyên thu thập dữ liệu dạng văn bản (Text Data) nhằm phục vụ các bài toán Xử lý ngôn ngữ tự nhiên (NLP) như Topic Modeling, Text Classification, và Sentiment Analysis. 
Dữ liệu được bao phủ từ kiến thức bách khoa (Wikipedia) đến tin tức sự kiện hàng ngày (Google News).

## 2. Nguồn dữ liệu (Sources)

### 2.1 Wikipedia (Bách khoa & Chiến thuật)
- **API**: `https://{lang}.wikipedia.org/w/api.php?action=query&prop=extracts&explaintext=1&titles={title}`
- **Bản quyền**: CC BY-SA (Sạch 100% để train model).
- **Ngôn ngữ**: Cào đồng thời cả tiếng Anh (`en`) và tiếng Việt (`vi`).
- **Nội dung thu thập**:
  - `Team`: Các đội bóng lớn (Arsenal, Manchester United, Real Madrid...)
  - `Tactic`: Chiến thuật bóng đá (Gegenpressing, Tiki-taka, Catenaccio...)
  - `History`: Các sự kiện/thành tựu lớn (The Invincibles, Miracle of Istanbul...)

### 2.2 Google News RSS (Tin tức báo chí)
- **API**: `https://news.google.com/rss/search?q={query}&hl={hl}&gl={gl}&ceid={ceid}`
- **Ngôn ngữ**: Tiếng Việt (Tổng hợp từ VnExpress, Tuổi Trẻ, Thanh Niên...) và Tiếng Anh (ESPN, SkySports...).
- **Nội dung thu thập**:
  - Truy vấn theo từ khóa (Keywords): "Bóng đá Ngoại hạng Anh", "Manchester United", "Arsenal", "Premier League"...
  - Dữ liệu trả về gồm: Tiêu đề (Title), Tóm tắt (Summary/Snippet), Ngày đăng (Published Date), Link nguồn.

## 3. Kiến trúc Data Lake

### Bronze Layer (Raw)
Dữ liệu được nén lại bằng GZIP để tiết kiệm dung lượng:
- **Wiki**: `bronze/wikipedia_articles/entity={entity_type}/lang={lang}/article={title}/fetched_date={YYYY-MM-DD}/content.json.gz`
- **News**: `bronze/rss/google_news/query={query}/lang={lang}/ingest_date={YYYY-MM-DD}/entries.jsonl.gz`

### Silver Layer (Cleaned & Tabular)
Làm sạch văn bản (loại bỏ HTML, URL, khoảng trắng thừa) và lưu dưới dạng Parquet:
- **Wiki**: `silver/text/wiki_articles/entity={entity_type}/fetched_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema*: title, entity_type, lang, page_id, extract (văn bản gốc), word_count.
- **News**: `silver/text/google_news_articles/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - *Schema*: feed, query, lang, title, text (tiêu đề + tóm tắt), published_ts, source, guid.

## 4. Automation (Airflow DAGs)
Để tối ưu tài nguyên, pipeline này được tách làm 2 DAG với lịch trình khác nhau:
1. `dag_p20_wikipedia`: Chạy `@weekly` (Hàng tuần) — Vì nội dung bách khoa ít bị thay đổi.
2. `dag_p20_google_news`: Chạy `@daily` (Hàng ngày) — Cập nhật tin tức liên tục.
