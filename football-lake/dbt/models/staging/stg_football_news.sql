{{ config(materialized='view') }}
{# CHƯA thấy code p25: giả định file là mảng bài báo HOẶC {"articles":[...]}; tên trường theo dạng NewsAPI/GNews.
   Thiếu trường nào => NULL (không vỡ). Kiểm tra lại khi có mẫu raw. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/football_news/**/*.json.gz') }}, filename=true)
),

articles as (
    select filename, {{ jarray('doc', '$.articles') }} as a
    from raw where json_extract(doc, '$.articles') is not null
    union all
    select filename, doc as a
    from raw where json_extract(doc, '$.articles') is null
)

select
    {{ jget('a', '$.title') }}                                            as title,
    {{ jget('a', '$.url') }}                                              as url,
    coalesce({{ jget('a', '$.image') }}, {{ jget('a', '$.urlToImage') }}) as image_url,
    coalesce({{ jget('a', '$.source.name') }}, {{ jget('a', '$.source') }}) as source_name,
    {{ jts('a', '$.publishedAt') }}                                       as published_at,
    {{ path_date() }}                                                     as ingest_date
from articles
where json_extract_string(a, '$.url') is not null
qualify row_number() over (partition by url order by ingest_date desc nulls last, filename desc) = 1
