{{ config(materialized='view') }}
{# Grain: (video_id, search_query) - 1 video có thể lên nhiều query. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/youtube/videos/**/*.jsonl.gz') }},
                           format='newline_delimited', filename=true)
)

select
    {{ jget('doc', '$.video_id') }}       as video_id,
    {{ jget('doc', '$.query') }}          as search_query,
    {{ jts('doc', '$.published_at') }}    as published_at,
    {{ jget('doc', '$.channel_id') }}     as channel_id,
    {{ jget('doc', '$.title') }}          as title,
    {{ jget('doc', '$.description') }}    as description,
    {{ jget('doc', '$.channel_title') }}  as channel_title,
    {{ path_date() }}                     as ingest_date
from raw
where json_extract_string(doc, '$.video_id') is not null
qualify row_number() over (
    partition by video_id, search_query
    order by ingest_date desc nulls last, filename desc) = 1
