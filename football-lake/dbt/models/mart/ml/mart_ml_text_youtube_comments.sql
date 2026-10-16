{{ config(materialized='external') }}
{# TEXT ML (tách riêng): bình luận YouTube. 1 dòng / bình luận, kèm ngữ cảnh video cha (nối theo video_id). #}
with c as (
    select * from {{ ref('stg_youtube_comments') }}
    qualify row_number() over (partition by comment_id order by ingest_date desc) = 1
),
v as (
    select video_id, any_value(title) as video_title, any_value(channel_title) as video_channel_title,
           min(published_at) as video_published_at
    from {{ ref('stg_youtube_videos') }}
    group by video_id
)
select c.comment_id, c.video_id, c.author, c.author_hash, c.text, c.like_count, c.published_at,
       cast(c.published_at as date)                                  as doc_date,
       len(string_split(trim(coalesce(c.text, '')), ' '))            as n_words,
       md5(lower(trim(coalesce(c.text, ''))))                        as text_hash,
       v.video_title, v.video_channel_title, v.video_published_at,
       date_diff('day', cast(v.video_published_at as date), cast(c.published_at as date)) as days_after_video,
       c.ingest_date
from c left join v using (video_id)
