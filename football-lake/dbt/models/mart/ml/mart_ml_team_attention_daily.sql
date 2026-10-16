{{ config(materialized='external') }}
{# Mức độ quan tâm theo ĐỘI x NGÀY (lượt xem trang Wikipedia của CLB, cộng các ngôn ngữ).
   Tên bài (vd 'Brighton_%26_Hove_Albion_F.C.', 'A.C._Milan') -> team_key qua alias; thử vài biến thể (bỏ hậu tố F.C./CF/AFC,
   bỏ tiền tố FC/AC/AS..., bỏ 'de') và so khớp dạng không dấu cách. Chỉ chứa đội map được (mart_ml_match_features dùng bảng này, đặc trưng NULL
   nếu đội không có dữ liệu). Phạm vi do p10 quyết định (~40 CLB hàng đầu châu Âu). #}

with arts as (
    select distinct article,
        {{ name_norm("replace(article, '%26', '&')") }} as n0
    from {{ ref('stg_wikimedia_pageviews') }}
    where entity_type = 'team'
),
cand as (
    select article, 1 as rk, replace(n0, ' ', '') as c from arts
    union all
    select article, 2, replace(regexp_replace(n0, ' (f c|fc|cf|afc|a c|ac|sc|ssc|s s c)$', ''), ' ', '') from arts
    union all
    select article, 3, replace(replace(regexp_replace(n0, ' (f c|fc|cf|afc|a c|ac|sc|ssc|s s c)$', ''), ' de ', ' '), ' ', '') from arts
    union all
    -- bỏ tiền tố câu lạc bộ (FC_Bayern_Munich -> bayern munich)
    select article, 4, replace(replace(regexp_replace(regexp_replace(n0, ' (f c|fc|cf|afc|a c|ac|sc|ssc|s s c)$', ''),
                       '^(fc|afc|ac|as|ssc|sv|vfl|vfb|tsg|rc|ud|cd|ca|sd|us|ss) ', ''), ' de ', ' '), ' ', '') from arts
),
art_team as (
    select c.article, a.team_key
    from cand c
    join {{ ref('int_ml_alias') }} a on a.alias_compact = c.c
    qualify row_number() over (partition by c.article order by c.rk, a.team_key) = 1
)
select
    t.team_key,
    p.view_date,
    sum(p.views)                                       as views_total,
    sum(p.views) filter (where p.lang = 'en')          as views_en,
    count(distinct p.lang)                             as n_langs
from {{ ref('stg_wikimedia_pageviews') }} p
join art_team t on t.article = p.article
where p.entity_type = 'team' and p.view_date is not null and p.views is not null
group by t.team_key, p.view_date
