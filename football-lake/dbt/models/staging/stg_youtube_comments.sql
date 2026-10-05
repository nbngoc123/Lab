
{# Comment bị tải lại mỗi ngày => giữ bản mới nhất (like_count đổi theo thời gian).
   author_hash: dùng cái này cho phân tích; nếu cần ẩn danh hoàn toàn, bỏ cột author ở đây. #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/youtube/comments/**/*.jsonl.gz', "format='newline_delimited'") }}
)

select
    {{ jget('doc', '$.comment_id') }}          as comment_id,
    {{ jget('doc', '$.video_id') }}            as video_id,
    {{ jget('doc', '$.author') }}              as author,
    md5({{ jget('doc', '$.author') }})         as author_hash,
    {{ jget('doc', '$.text') }}                as text,
    {{ jget('doc', '$.like_count', 'int') }}   as like_count,
    {{ jts('doc', '$.published_at') }}         as published_at,
    {{ path_date() }}                          as ingest_date
from raw
where json_extract_string(doc, '$.comment_id') is not null
qualify row_number() over (
    partition by comment_id
    order by ingest_date desc nulls last, filename desc) = 1
