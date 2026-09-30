{{ config(materialized='view') }}
{# Path có fetched_date (không phải ingest_date). Grain: (page_id, language, category). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/wikipedia_articles/**/*.json.gz') }}, filename=true)
)

select
    {{ jget('doc', '$.page_id') }}               as page_id,
    {{ jget('doc', '$.title') }}                 as title,
    coalesce({{ jget('doc', '$.entity_type') }}, {{ path_part('entity') }}) as category,
    coalesce({{ jget('doc', '$.lang') }}, {{ path_part('lang') }})          as language,
    {{ jget('doc', '$.extract') }}               as content,
    {{ jget('doc', '$.word_count', 'int') }}     as word_count,
    {{ path_date('fetched_date') }}              as fetched_date
from raw
where json_extract_string(doc, '$.page_id') is not null
qualify row_number() over (
    partition by page_id, language, category
    order by fetched_date desc nulls last, filename desc) = 1
