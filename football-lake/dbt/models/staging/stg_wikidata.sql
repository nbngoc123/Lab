{{ config(materialized='view') }}
{# Base: bindings thô của SPARQL, CHỈ snapshot mới nhất của mỗi query.
   Các model stg_wikidata_* bên dưới parse thành cột typed. #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/wikidata/sparql/**/*.json.gz') }}
),

unnested as (
    select
        {{ path_part('query') }}  as sparql_query_name,
        {{ path_date() }}         as ingest_date,
        {{ jarray('doc', '$.results.bindings') }} as binding_obj
    from raw
)

select sparql_query_name, ingest_date, binding_obj
from unnested
qualify dense_rank() over (partition by sparql_query_name order by ingest_date desc nulls last) = 1
