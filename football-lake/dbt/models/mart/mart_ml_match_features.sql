{{ config(materialized='external') }}
{# ML mart CHÍNH: 1 dòng / trận (cả trận đã đá lẫn sắp đá). Dùng cho dự đoán kết quả 1X2, tổng bàn, O/U, BTTS...

   QUY ƯỚC CỘT:  f_* = đặc trưng (chỉ thông tin TRƯỚC giờ đá) | y_* = nhãn SAU trận | còn lại = khoá/metadata/cờ.
   => khi train chỉ cần: X = các cột bắt đầu bằng 'f_'. (Nhớ: train.py cũ lấy "mọi cột không nằm trong danh sách khoá" làm
      đặc trưng, nên đừng thêm cột metadata không có tiền tố f_/y_ rồi chọn theo kiểu loại trừ.)

   split : 'train' | 'valid' | 'test' chia THEO THỜI GIAN trên các trận đã có nhãn (vars ml_train_until / ml_valid_until);
           'predict' = trận chưa đá (suy luận). Không dùng chia ngẫu nhiên: sẽ rò rỉ tương lai.
   is_warm : cả 2 đội có >= ml_min_history trận trước đó (đặc trưng phong độ đầy đủ). Lọc is_warm khi train.

   Đặc trưng NULL = nguồn không phủ trận/đội đó (xG chỉ có ở giải Understat, odds chỉ có ở giải co.uk/odds-api, thời tiết chỉ
   một số giải). XGBoost/LightGBM xử lý NaN gốc; với mô hình khác hãy impute + dùng cờ has_*.

   BẢNG NÀY CHỈ GỒM DỮ LIỆU CÓ CẤU TRÚC TỪ CORE. Lượt xem Wikipedia ở bảng phụ mart_ml_match_attention (join theo match_key),
   văn bản (tin/YouTube/Wikipedia) ở mart_ml_text_docs: cả hai tách riêng vì độ phủ thấp và logic nối tên đội không nên
   nằm trong bảng huấn luyện chính. #}

{% set num = ml_team_numeric_features() %}

with m as (
    select *, coalesce(cast(kickoff_utc as timestamp), cast(match_date as timestamp)) as ord_ts
    from {{ ref('fact_match') }}
    where match_date >= date '{{ var('ml_min_match_date') }}'
),

h as (select * from {{ ref('mart_ml_team_match_features') }} where side = 'h'),
a as (select * from {{ ref('mart_ml_team_match_features') }} where side = 'a'),

lab as (
    select match_key, percent_rank() over (order by ord_ts, match_key) as pr
    from m
    where is_finished and home_goals is not null and away_goals is not null
),

-- ---------- tỉ lệ cược -> xác suất ngầm định (chọn nguồn đầy đủ nhất, ưu tiên thị trường trung bình) ----------
o as (
    select
        match_key,
        case when mkt_avg_home > 1 and mkt_avg_draw > 1 and mkt_avg_away > 1 then 'mkt_avg'
             when b365_home > 1 and b365_draw > 1 and b365_away > 1 then 'b365'
             -- odds-api chỉ nhận khi snapshot lấy TRƯỚC giờ đá (tránh tỉ lệ in-play / sau trận)
             when odds_api_home > 1 and odds_api_draw > 1 and odds_api_away > 1
                  and (odds_api_updated_at is null or cast(odds_api_updated_at as timestamp) <= ord_ts) then 'odds_api'
        end as odds_src,
        mkt_avg_home, mkt_avg_draw, mkt_avg_away, b365_home, b365_draw, b365_away,
        odds_api_home, odds_api_draw, odds_api_away
    from m
),
op as (
    select match_key, odds_src,
        case odds_src when 'mkt_avg' then mkt_avg_home when 'b365' then b365_home when 'odds_api' then odds_api_home end as oh,
        case odds_src when 'mkt_avg' then mkt_avg_draw when 'b365' then b365_draw when 'odds_api' then odds_api_draw end as od,
        case odds_src when 'mkt_avg' then mkt_avg_away when 'b365' then b365_away when 'odds_api' then odds_api_away end as oa
    from o
),

-- ---------- trọng tài: thống kê các trận TRƯỚC (50 trận gần nhất) ----------
ref_roll as (
    select referee, known_ts,
        count(*) over w                                                    as n,
        case when count(*) over w >= 10 then avg(cards) over w end         as avg_cards,
        case when count(fouls) over w >= 10 then avg(fouls) over w end     as avg_fouls
    from (
        select referee, ord_ts + interval 3 hour as known_ts, ord_ts, match_key,
               home_yellows + away_yellows + coalesce(home_reds, 0) + coalesce(away_reds, 0) as cards,
               home_fouls + away_fouls as fouls
        from m
        where referee is not null and is_finished and home_yellows is not null and away_yellows is not null
    )
    window w as (partition by referee order by known_ts, match_key rows between 49 preceding and current row)
)

select
    -- ===== khoá & metadata =====
    m.match_key, m.competition_key, m.season, m.match_date, m.kickoff_utc, m.home_key, m.away_key,
    m.fdo_match_id, m.understat_match_id, m.af_fixture_id, m.openliga_match_id,
    m.status, m.is_finished,
    (m.is_finished and m.home_goals is not null and m.away_goals is not null)             as is_labeled,
    case
        when not (m.is_finished and m.home_goals is not null and m.away_goals is not null) then 'predict'
        when l.pr < {{ var('ml_train_until') }} then 'train'
        when l.pr < {{ var('ml_valid_until') }} then 'valid'
        else 'test'
    end                                                                                    as split,
    (h.f_n_prev >= {{ var('ml_min_history') }} and a.f_n_prev >= {{ var('ml_min_history') }}) as is_warm,
    op.odds_src,
    (op.odds_src is not null)                                                             as has_odds,
    (m.home_xg is not null)                                                               as has_xg,
    m.goals_conflict, m.in_fdo, m.in_couk, m.in_understat, m.in_api_football, m.in_openliga, m.in_odds_api,

    -- ===== đặc trưng: phong độ đội (nhà / khách / chênh lệch) =====
{% for f in num %}
{% if f == 'h2h_n' %}
    h.f_h2h_n as f_h2h_n,        -- số lần đối đầu là CHUNG cho 2 đội (hiệu home-away luôn = 0 nên không tạo f_diff_)
{% else %}
    h.f_{{ f }} as f_home_{{ f }}, a.f_{{ f }} as f_away_{{ f }}, h.f_{{ f }} - a.f_{{ f }} as f_diff_{{ f }},
{% endif %}
{% endfor %}
    h.f_form5 as f_home_form5, a.f_form5 as f_away_form5,

    -- ===== đặc trưng: thị trường cược (xác suất đã bỏ biên nhà cái) =====
    {{ implied_prob('op.oh', 'op.od', 'op.oa') }}                                          as f_mkt_p_home,
    {{ implied_prob('op.od', 'op.oh', 'op.oa') }}                                          as f_mkt_p_draw,
    {{ implied_prob('op.oa', 'op.oh', 'op.od') }}                                          as f_mkt_p_away,
    case when op.oh > 1 and op.od > 1 and op.oa > 1 then 1.0 / op.oh + 1.0 / op.od + 1.0 / op.oa - 1 end as f_mkt_overround,

    -- ===== đặc trưng: bối cảnh trận =====
    isodow(m.match_date)                                                                   as f_dow,
    month(m.match_date)                                                                    as f_month,
    (isodow(m.match_date) >= 6)::int                                                       as f_is_weekend,
    hour(cast(m.kickoff_utc as timestamp))                                                 as f_kickoff_hour_utc,
    try_cast(m.matchday as integer)                                                        as f_matchday,
    dc.tier                                                                                as f_comp_tier,
    ht.stadium_capacity                                                                    as f_home_stadium_capacity,
    m.weather_temp_c as f_weather_temp_c, m.weather_precip_mm as f_weather_precip_mm,
    m.weather_wind_kmh as f_weather_wind_kmh, m.weather_humidity_pct as f_weather_humidity_pct,

    -- ===== đặc trưng: trọng tài (các trận trước) =====
    coalesce(rr.n, 0)                                                                      as f_ref_n_prev,
    rr.avg_cards                                                                           as f_ref_avg_cards,
    rr.avg_fouls                                                                           as f_ref_avg_fouls,

    -- ===== nhãn (SAU trận) =====
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then m.home_goals end      as y_home_goals,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then m.away_goals end      as y_away_goals,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then m.total_goals end     as y_total_goals,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then m.home_goals - m.away_goals end as y_goal_diff,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then m.result end          as y_result,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null
         then case m.result when 'A' then 0 when 'D' then 1 else 2 end end                                       as y_result_code,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null
         then (m.home_goals > 0 and m.away_goals > 0)::int end                                                   as y_btts,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then (m.total_goals > 1)::int end as y_over_1_5,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then (m.total_goals > 2)::int end as y_over_2_5,
    case when m.is_finished and m.home_goals is not null and m.away_goals is not null then (m.total_goals > 3)::int end as y_over_3_5,
    m.ht_home_goals as y_ht_home_goals, m.ht_away_goals as y_ht_away_goals,
    -- "forecast" của Understat KHÔNG phải dự báo trước trận: đo trên dữ liệu thật nó tương quan 0.95 với xG của CHÍNH trận và
    -- đạt acc 61% / logloss 0.87 (nhà cái 0.98 trên cùng trận) => được tính từ các cú sút sau trận. Chỉ dùng làm benchmark (y_).
    m.forecast_home_win as y_us_xg_p_home, m.forecast_draw as y_us_xg_p_draw, m.forecast_away_win as y_us_xg_p_away,
    m.home_xg as y_home_xg, m.away_xg as y_away_xg,
    m.home_shots as y_home_shots, m.away_shots as y_away_shots,
    m.home_shots_target as y_home_shots_target, m.away_shots_target as y_away_shots_target,
    m.home_corners + m.away_corners as y_total_corners,
    m.home_yellows + m.away_yellows + coalesce(m.home_reds, 0) + coalesce(m.away_reds, 0) as y_total_cards
from m
join h on h.match_key = m.match_key
join a on a.match_key = m.match_key
left join lab l on l.match_key = m.match_key
left join op on op.match_key = m.match_key
left join {{ ref('dim_competition') }} dc on dc.competition_key = m.competition_key
left join {{ ref('dim_team') }} ht on ht.team_key = m.home_key
asof left join ref_roll rr on rr.referee = m.referee and m.ord_ts > rr.known_ts
