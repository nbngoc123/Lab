{{ config(materialized='external') }}
{# Bảng AUDIT CẤP TRẬN CHỐNG RÒ RỈ THỜI GIAN:
   Lưu vết tường minh từng trận:
   - ord_ts: mốc thời gian dự đoán (kickoff_utc hoặc 00:00:00 của match_date nếu thiếu giờ).
   - known_ts: mốc thời gian kết quả chính thức khả dụng (kickoff_utc + 3h hoặc 23:59:59 cuối ngày nếu thiếu giờ).
   - is_missing_kickoff: cờ đánh dấu trận thiếu giờ đá.
   - split: phân chia thời gian cố định theo ngày.
   - f_home_n_prev / f_away_n_prev: số trận lịch sử đã tính trước thời điểm dự đoán.
   Bảng này phục vụ việc truy vết trực tiếp, kiểm toán dữ liệu và debug mô hình ML. #}

select
    m.match_key,
    m.competition_key,
    m.season,
    m.match_date,
    m.kickoff_utc,
    (m.kickoff_utc is null)                                                 as is_missing_kickoff,
    coalesce(cast(m.kickoff_utc as timestamp), cast(m.match_date as timestamp)) as ord_ts,
    case
        when m.kickoff_utc is not null then cast(m.kickoff_utc as timestamp) + interval 3 hour
        else cast(m.match_date as timestamp) + interval 23 hour + interval 59 minute + interval 59 second
    end                                                                     as known_ts,
    m.split,
    m.home_key,
    m.away_key,
    m.f_home_n_prev,
    m.f_away_n_prev,
    m.is_warm,
    m.odds_src,
    m.has_odds,
    m.is_finished,
    m.is_labeled,
    m.y_result,
    m.y_home_goals,
    m.y_away_goals
from {{ ref('mart_ml_match_features') }} m
