{{ config(materialized='external') }}
{# TEXT ML (tách riêng): YouTube video. 1 dòng / video. n_comments_collected / comment_likes_sum nối theo ID video (không theo tên). #}
with v as (
    select video_id, any_value(title) as title, any_value(description) as description, any_value(channel_id) as channel_id,
           any_value(channel_title) as channel_title, min(published_at) as published_at,
           list(distinct search_query order by search_query) as search_queries,
           min(ingest_date) as first_seen_date, max(ingest_date) as last_seen_date
    from {{ ref('stg_youtube_videos') }}
    group by video_id
),
c as (
    select video_id, count(*) as n_comments_collected, coalesce(sum(like_count), 0) as comment_likes_sum
    from {{ ref('stg_youtube_comments') }}
    group by video_id
)
select v.*, cast(v.published_at as date) as doc_date,
       trim(coalesce(v.title, '') || ' ' || coalesce(v.description, ''))                          as text,
       len(string_split(trim(coalesce(v.title, '') || ' ' || coalesce(v.description, '')), ' '))  as n_words,
       md5(lower(trim(coalesce(v.title, ''))))                                                    as title_hash,
       coalesce(c.n_comments_collected, 0)                                                        as n_comments_collected,
       coalesce(c.comment_likes_sum, 0)                                                           as comment_likes_sum
from v left join c using (video_id)
