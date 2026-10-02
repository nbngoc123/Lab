{# published là RFC-822 ("Tue, 29 Sep 2026 10:00:00 GMT") nên cast thẳng ::timestamp sẽ lỗi -> dùng strptime.
   Grain: (news_id, search_query, language) - cùng 1 bài có thể xuất hiện ở nhiều query. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/rss/google_news/**/*.jsonl.gz') }},
                           format='newline_delimited', filename=true)
)

select
    {{ jget('doc', '$.guid') }}       as news_id,
    {{ jget('doc', '$.title') }}      as title,
    {{ jget('doc', '$.summary') }}    as summary,
    {{ jget('doc', '$.link') }}       as url,
    {{ jget('doc', '$.source') }}     as publisher,
    coalesce(
        try_strptime(regexp_replace(json_extract_string(doc, '$.published'), ' (GMT|UTC|Z)$', ''),
                     '%a, %d %b %Y %H:%M:%S'),
        try_cast(json_extract_string(doc, '$.published') as timestamp)
    )                                 as published_at,
    {{ jget('doc', '$.query') }}      as search_query,
    {{ jget('doc', '$.lang') }}       as language,
    {{ jget('doc', '$.feed') }}       as source_type,
    {{ path_date() }}                 as ingest_date
from raw
where json_extract_string(doc, '$.guid') is not null
qualify row_number() over (
    partition by news_id, search_query, language
    order by ingest_date desc nulls last, filename desc) = 1
