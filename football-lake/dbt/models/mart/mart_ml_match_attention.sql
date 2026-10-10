{{ config(materialized='external') }}
{# BẢNG PHỤ (join tuỳ chọn theo match_key) - mức độ quan tâm Wikipedia của 2 đội: lượt xem trung bình/ngày trong 7 và 28 ngày
   TRƯỚC ngày đá (không gồm ngày đá). Tách khỏi bảng chính vì độ phủ thấp (đo thật: ~24% đội-trận, ~11% trận có cả 2 đội;
   chỉ CLB lớn do p10 thu ~50 bài đội, lịch sử từ 2023-01) và vì phải map tên bài -> team_key.
   Chỉ có dòng cho trận mà ÍT NHẤT một đội có >= 14 ngày dữ liệu trong cửa sổ; trận khác = không có dòng (left join -> NULL). #}

with m as (
    select match_key, match_date, home_key, away_key
    from {{ ref('fact_match') }}
    where match_date >= date '{{ var('ml_min_match_date') }}'
),
{% for side in ['home', 'away'] %}
att_{{ side }} as (
    select m.match_key,
        avg(t.views_total) filter (where t.view_date >= m.match_date - 7) as v7,
        avg(t.views_total)                                                 as v28,
        count(*)                                                           as n_days
    from m
    join {{ ref('mart_ml_team_attention_daily') }} t
      on t.team_key = m.{{ side }}_key and t.view_date < m.match_date and t.view_date >= m.match_date - 28
    group by m.match_key
){% if not loop.last %},{% endif %}
{% endfor %}

select
    m.match_key,
{% for side in ['home', 'away'] %}
{% set al = 'ah' if side == 'home' else 'aa' %}
    case when {{ al }}.n_days >= 14 then {{ al }}.v7 end                                  as f_{{ side }}_att_7d,
    case when {{ al }}.n_days >= 14 then {{ al }}.v28 end                                 as f_{{ side }}_att_28d,
    case when {{ al }}.n_days >= 14 then {{ al }}.v7 / nullif({{ al }}.v28, 0) end        as f_{{ side }}_att_ratio,
{% endfor %}
    case when ah.n_days >= 14 and aa.n_days >= 14 and ah.v7 > 0 and aa.v7 > 0 then ln(ah.v7 / aa.v7) end as f_att_log_ratio_7d
from m
left join att_home ah on ah.match_key = m.match_key
left join att_away aa on aa.match_key = m.match_key
where ah.n_days >= 14 or aa.n_days >= 14
