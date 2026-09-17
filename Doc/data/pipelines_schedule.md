# Danh sách các Pipeline (Luồng dữ liệu) đã triển khai

Tài liệu này tổng hợp toàn bộ các Pipeline đã được thực hiện trong dự án Football Data Lake và chu kỳ lên lịch (Schedule Interval) của chúng trên hệ thống Apache Airflow.

## 1. Nhóm Dữ liệu Nền tảng (Master Data & Dimensions)
Các luồng này thường chỉ cần cào 1 lần vào đầu mùa giải hoặc cập nhật với tần suất thấp (Hàng tuần/Hàng tháng).
- **`p01_fpl` (Fantasy Premier League):** Lấy dữ liệu cơ sở của hệ thống Fantasy (Danh sách cầu thủ, đội bóng, lịch thi đấu FPL). Lên lịch: **@weekly**.
- **`p06_wikidata`:** Lấy thông tin định danh (Club, Player, Stadium) từ Wikidata. Lên lịch: **@monthly**.
- **`p08_thesportsdb`:** Lấy Logo đội bóng, hình ảnh cầu thủ, phù hiệu. Lên lịch: **@monthly**.

## 2. Nhóm Dữ liệu Thống kê & Trận đấu (Match & Stats)
Các luồng này cần cập nhật liên tục theo ngày để lấy kết quả bóng đá.
- **`p09_football_data`:** Cào lịch thi đấu, kết quả (Matches) và Bảng xếp hạng (Standings) của 6 giải đấu hàng đầu Châu Âu từ Football-Data.org. Lên lịch: **@daily**.
- **`p24_api_football`:** Cào kết quả trực tiếp, chi tiết bàn thắng, thẻ phạt (Fixtures) từ API-Sports (trực tiếp). Lên lịch: **@daily**.
- **`p19_understat`:** Cào chỉ số Bàn thắng kỳ vọng (xG) cực kỳ chi tiết của từng cú sút, cầu thủ và trận đấu. Lên lịch: **@weekly** (Do data xG ít biến động hàng ngày, tuần cập nhật sau mỗi vòng đấu là đẹp).
- **`p16_physioroom`:** Cào dữ liệu chấn thương của các cầu thủ Ngoại Hạng Anh (Loại chấn thương, ngày dự kiến trở lại). Lên lịch: **@daily**.

## 3. Nhóm Dữ liệu Tỷ lệ kèo & Nhà cái (Betting)
- **`p22_odds_api`:** Cào tỷ lệ kèo đối đầu (H2H), kèo Châu Á, Tài Xỉu trực tiếp từ nhiều nhà cái (Pinnacle, DraftKings...) thông qua The Odds API. Lên lịch: **@hourly** (Cập nhật hàng giờ vì tỷ lệ kèo biến động liên tục trước giờ bóng lăn).

## 4. Nhóm Dữ liệu Mạng xã hội, Tin tức & Cảm xúc (Social, News & NLP)
Các luồng này phục vụ cho bài toán Xử lý ngôn ngữ tự nhiên (NLP) và đánh giá độ hot (Sentiment/Trending).
- **`p20_google_news`:** Cào các bài báo, tin tức bóng đá từ hệ thống Google News RSS. Lên lịch: **@daily**.
- **`p25_football_news`:** Cào các bài tin tức nóng, tin chuyển nhượng từ API Football News Aggregator (RapidAPI). Lên lịch: **@daily**.
- **`p21_youtube`:** Cào video highlight, video phân tích chiến thuật và toàn bộ Comments của người hâm mộ trên YouTube. Lên lịch: **@daily**.
- **`p07_reddit_rss`:** Lấy các bài post và thảo luận nóng nhất từ mạng xã hội Reddit. Lên lịch: **@daily**.
- **`p10_wikimedia` (Pageviews):** Cào dữ liệu lượt truy cập trang (Pageviews) của các cầu thủ trên Wikipedia để đo lường "độ hot" và sự quan tâm của công chúng. Lên lịch: **@daily**.
- **`p20_wikipedia` (Articles):** Cào toàn văn nội dung bài viết về các Câu lạc bộ, Cầu thủ, Lịch sử, Chiến thuật trên Wikipedia để làm kho tri thức (Knowledge Base). Lên lịch: **@weekly**.

## 5. Nhóm Dữ liệu Môi trường (Environment)
- **`p13_open_meteo`:** Cào dữ liệu Thời tiết (Nhiệt độ, lượng mưa, sức gió) tại các Sân vận động vào thời điểm diễn ra trận đấu (Phục vụ phân tích ảnh hưởng của thời tiết đến kết quả trận đấu). Lên lịch: **@daily**.

---
**Ghi chú về Airflow Cron (Schedule Interval):**
- `@hourly`: Chạy 1 lần mỗi giờ (Thích hợp cho Tỷ lệ kèo).
- `@daily`: Chạy 1 lần vào nửa đêm mỗi ngày (Thích hợp cho Tin tức, Kết quả trận đấu, Youtube).
- `@weekly`: Chạy 1 lần mỗi tuần (Thích hợp cho xG, Wiki).
- `@monthly`: Chạy 1 lần mỗi tháng (Thích hợp cho Logo, Master Data).
