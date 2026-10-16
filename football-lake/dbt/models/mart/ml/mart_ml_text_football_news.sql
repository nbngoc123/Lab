{{ config(materialized='external') }}
{# TEXT ML (tách riêng): football_news. 1 dòng / bài (khoá = md5(url)). Chỉ có tiêu đề (không có tóm tắt). #}
select md5(url) as news_id, any_value(title) as title, url, any_value(image_url) as image_url,
       any_value(source_name) as source_name, min(published_at) as published_at,
       cast(min(published_at) as date) as doc_date,
       any_value(title) as text,
       len(string_split(trim(coalesce(any_value(title), '')), ' '))  as n_words,
       md5(lower(trim(coalesce(any_value(title), ''))))              as title_hash,
       min(ingest_date) as first_seen_date, max(ingest_date) as last_seen_date
from {{ ref('stg_football_news') }}
where url is not null
group by url
