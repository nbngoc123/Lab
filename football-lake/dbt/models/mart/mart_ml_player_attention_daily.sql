{{ config(materialized='external') }}
{# Mức độ quan tâm theo CẦU THỦ x NGÀY (Wikipedia pageviews). player_key NULL nếu tên bài không khớp duy nhất
   với dim_player (so khớp theo name_norm); vẫn giữ dòng để dùng sau. #}

with uniq as (
    select name_norm, any_value(player_key) as player_key
    from {{ ref('dim_player') }}
    where name_norm is not null
    group by name_norm
    having count(distinct player_key) = 1
)
select
    p.article                                           as player_article,
    u.player_key,
    p.view_date,
    sum(p.views)                                        as views_total,
    sum(p.views) filter (where p.lang = 'en')           as views_en,
    count(distinct p.lang)                              as n_langs
from {{ ref('stg_wikimedia_pageviews') }} p
left join uniq u on u.name_norm = {{ name_norm("replace(replace(p.article, '%26', '&'), '_', ' ')") }}
where p.entity_type = 'player' and p.view_date is not null and p.views is not null
group by p.article, u.player_key, p.view_date
