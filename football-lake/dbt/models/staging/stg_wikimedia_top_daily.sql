{{ config(materialized='view') }}
{# MỚI: p10 có ghi top_daily nhưng chưa có staging. Bao gồm cả trang không liên quan bóng đá (Main_Page...) - lọc ở mart. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/wikimedia_pageviews/top_daily/**/*.json.gz') }}, filename=true)
),
its as (select filename, {{ jarray('doc', '$.items') }} as it from raw),
arts as (select filename, {{ jarray('it', '$.articles') }} as a from its)

select
    {{ path_part('lang') }}                    as lang,
    {{ path_date('date') }}                    as view_date,
    {{ jget('a', '$.article') }}               as article,
    {{ jget('a', '$.views', 'int') }}          as views,
    {{ jget('a', '$.rank', 'int') }}           as rank
from arts
where json_extract_string(a, '$.article') is not null
qualify row_number() over (partition by lang, view_date, article order by filename desc) = 1
