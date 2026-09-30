# Viết lại lớp staging (38 model)

## Thay đổi áp dụng cho tất cả
- Bỏ `columns={'json_column': 'JSON'}` (trả NULL vì file không có key `json_column`, model xanh nhưng rỗng).
  Thay bằng `read_json_objects` + `json_extract` -> body lỗi/thiếu key cho 0 dòng thay vì vỡ.
- `try_cast` thay `::type`: ô lỗi thành NULL, test `not_null` bắt lỗi thay vì 1 dòng xấu làm sập cả model.
- Partition lấy từ path bằng regex (`league`, `season`, `comp`, `venue`, `ingest_date`...) - không phụ thuộc
  hive_partitioning nên không đụng tên cột trong JSON.
- Dedup bằng `QUALIFY row_number()` giữ snapshot mới nhất. Giữ nguyên lịch sử ở `stg_odds_base` và `stg_physioroom`.
- Macro dùng chung ở `macros/lake.sql` (đường dẫn S3, path_part, jget, jts, jarray, wd_*).

## Sửa lỗi
| Model | Lỗi cũ | Sửa |
|---|---|---|
| physioroom, (wikimedia) | `{ config(` sai Jinja | viết lại; `stg_wikimedia` XÓA (glob sai path, trùng pageviews) |
| google_news | cast RFC-822 thất bại | `strptime` |
| thesportsdb_teams | đọc sai path (`metadata/entity=teams`) | đọc `raw/thesportsdb/teams/league=*/` |
| understat_* | mất league/season | lấy từ path |
| odds x3 | xếp chồng mọi snapshot | base + bản mới nhất/(match, nhà cái, outcome) |
| open_meteo | nhân bản theo số ngày chạy | dedup (sân, giờ) |
| wikimedia_pageviews | nhân bản theo số ngày chạy | dedup (article, lang, ngày) |
| football_data_co_uk | không có season, cast lỗi khi ô trống, ngày 2 định dạng | season từ path, all_varchar + try_cast |
| wikidata | chỉ binding thô | 4 model typed, tách lat/lon từ WKT |

## Model mới
`stg_odds_base`, `stg_open_meteo_forecast`, `stg_api_football_standings`, `stg_understat_team_matches`,
`stg_wikidata_{clubs,players,stadiums,managers}`, `stg_wikimedia_top_daily`.

## Đổi kiểu / thêm cột (kiểm tra ref phía sau)
- `ingest_date` giờ là DATE (trước là VARCHAR ở open_meteo, wikimedia).
- Odds thêm `bookmaker_key`, `sport_key`, `snapshot_date`, `outcome_type`.
- YouTube comments thêm `author_hash`.

## Model TẮT mặc định (chưa thấy code ingest ghi raw path tương ứng)
fdo competitions/teams/players/standings, thesportsdb players/honors/former_teams.
Bật bằng vars trong `dbt_project.staging.snippet.yml`.

## Chưa xác minh
- `stg_football_news`: chưa thấy p25, đang giả định dạng NewsAPI/GNews.
- `stg_physioroom`: dạng long vì chưa biết tên cột từng bảng.
- Test trên DuckDB 1.5.6 với dữ liệu mẫu tự dựng đúng cấu trúc ingest; chưa chạy với `dbt build` và S3 thật.
