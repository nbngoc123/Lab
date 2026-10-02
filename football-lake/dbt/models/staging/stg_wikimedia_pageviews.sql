{# p10 tải lại toàn bộ lịch sử từ 2023 mỗi ngày => dedup giữ ingest_date mới nhất cho mỗi (article, lang, ngày).
   'article' giữ nguyên dạng URL-encoded như trong path (vd Brighton_%26_Hove_Albion_F.C.). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/wikimedia_pageviews/per_article/**/*.json.gz') }}, filename=true)
),

items as (
    select
        {{ path_part('entity') }}    as entity_type,
        {{ path_part('lang') }}      as lang,
        {{ path_part('article') }}   as article,
        {{ path_date() }}            as ingest_date,
        {{ jarray('doc', '$.items') }} as item
    from raw
)

select
    entity_type, lang, article, ingest_date,
    {{ jget('item', '$.timestamp') }}                                   as timestamp_raw,
    try_strptime(left(json_extract_string(item, '$.timestamp'), 8), '%Y%m%d')::date as view_date,
    {{ jget('item', '$.views', 'int') }}                                as views
from items
where json_extract_string(item, '$.timestamp') is not null
qualify row_number() over (
    partition by entity_type, lang, article, view_date
    order by ingest_date desc nulls last) = 1
