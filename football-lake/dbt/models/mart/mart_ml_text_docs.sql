{{ config(materialized='external') }}
{# KHO VĂN BẢN hợp nhất cho NLP (sentiment, chủ đề, NER, embedding...). 1 dòng / tài liệu.
   Nguồn: Google News, football_news, YouTube (video + bình luận), Wikipedia.

   TÁCH RIÊNG CÓ CHỦ Ý - KHÔNG nối vào đội/trận. Lý do (đo trên dữ liệu thật): tin Google chỉ phủ ~2 tháng gần nhất cho 3 đội,
   YouTube là kênh tin tức tiếng Việt chung (video từ 2017, bình luận từ 2020) không gắn với trận nào, trong khi các trận trải
   2020-2027. Dò tên đội trong tiêu đề cho rất ít dòng khớp và nhiều khớp nhầm. Khi cần, hãy nối ở bước phân tích riêng
   (NER / embedding) thay vì nhúng vào mart. Quan hệ DUY NHẤT có trong bảng là theo ID: bình luận -> video (parent_doc_id). #}

with gn as (
    select 'gnews:' || news_id as doc_id, 'news' as doc_type, 'google_news' as source,
           min(published_at) as published_at, any_value(language) as language,
           any_value(title) as title, any_value(summary) as body, any_value(url) as url,
           any_value(publisher) as publisher, list(distinct search_query) as search_queries,
           cast(null as integer) as like_count, cast(null as varchar) as parent_doc_id,
           cast(max(ingest_date) as date) as ingest_date
    from {{ ref('stg_google_news') }}
    group by news_id
),
fn as (
    select 'news:' || md5(url), 'news', 'football_news', min(published_at), cast(null as varchar),
           any_value(title), cast(null as varchar), url, any_value(source_name), cast([] as varchar[]),
           cast(null as integer), cast(null as varchar), cast(max(ingest_date) as date)
    from {{ ref('stg_football_news') }}
    group by url
),
yv as (
    select 'yt:' || video_id, 'video', 'youtube', min(published_at), cast(null as varchar),
           any_value(title), any_value(description), 'https://www.youtube.com/watch?v=' || video_id,
           any_value(channel_title), list(distinct search_query),
           cast(null as integer), cast(null as varchar), cast(max(ingest_date) as date)
    from {{ ref('stg_youtube_videos') }}
    group by video_id
),
yc as (
    select 'ytc:' || comment_id, 'comment', 'youtube', published_at, cast(null as varchar),
           cast(null as varchar), text, cast(null as varchar), cast(null as varchar), cast([] as varchar[]),
           like_count, 'yt:' || video_id, cast(ingest_date as date)
    from {{ ref('stg_youtube_comments') }}
),
wk as (
    select 'wiki:' || page_id || ':' || language || ':' || coalesce(category, ''), 'wiki_article', 'wikipedia',
           cast(fetched_date as timestamp), language, title, content, cast(null as varchar), cast(null as varchar),
           cast([] as varchar[]), cast(null as integer), cast(null as varchar), cast(fetched_date as date)
    from {{ ref('stg_wikipedia') }}
),

docs as (
    select * from gn union all select * from fn union all select * from yv
    union all select * from yc union all select * from wk
)

select
    d.doc_id, d.doc_type, d.source, d.published_at, cast(d.published_at as date) as doc_date, d.language,
    d.title, d.body, d.url, d.publisher, d.search_queries, d.like_count, d.parent_doc_id,
    len(string_split(trim(coalesce(d.title, '') || ' ' || coalesce(d.body, '')), ' ')) as n_words,
    d.ingest_date
from docs d
