# P23: FBref (Thống kê chuyên sâu bóng đá)

## 1. Giới thiệu
**FBref** hiện đang là nền tảng thống kê số 1 thế giới về bóng đá, cung cấp dữ liệu chi tiết đến mức độ cá nhân cầu thủ (xG, xA, Progressive Passes, Tiền lương). 
Tuy không có API chính thức, nhưng FBref cho phép thu thập qua HTML (Web Scraping) miễn là tuân thủ Rate Limit.

## 2. Nguồn dữ liệu
- **Nguồn (Scraping)**: Quét trực tiếp các thẻ `<table>` từ URL `https://fbref.com/en/comps/9/Premier-League-Stats`.
- **Thư viện xử lý**: Sử dụng `pandas.read_html` kết hợp với `html5lib`.
- **Rate Limiting**: Bắt buộc phải có `time.sleep(4)` giữa các request để không bị WAF (Tường lửa) chặn IP 403 Forbidden.

## 3. Kiến trúc Data Lake
- **Bronze Layer**: Lưu giữ nguyên vẹn file CSV để dễ backup.
  - `bronze/fbref/epl_standings/ingest_date={YYYY-MM-DD}/standings.csv`
  - `bronze/fbref/epl_squad_stats/ingest_date={YYYY-MM-DD}/squad_stats.csv`
- **Silver Layer**: Xử lý làm phẳng các cột MultiIndex thành các cột chuẩn, dọn dẹp ký tự thừa.
  - Bảng Xếp Hạng: `silver/teams/fbref_standings/ingest_date={YYYY-MM-DD}/part-0.parquet`
  - Thống Kê Đội Hình: `silver/teams/fbref_squad_stats/ingest_date={YYYY-MM-DD}/part-0.parquet`

## 4. Airflow Automation
- **DAG**: `dag_p23_fbref`
- **Schedule**: `@weekly` (Hàng tuần). Vì đây là dữ liệu tổng hợp nguyên mùa giải, việc kéo về mỗi tuần một lần sau vòng đấu cuối tuần là hợp lý và an toàn nhất.
