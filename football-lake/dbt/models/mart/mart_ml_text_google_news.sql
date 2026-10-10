{{ config(materialized='external') }}
{# TEXT ML (tách riêng, KHÔNG nối đội/trận): Google News. 1 dòng / bài (news_id), gộp các truy vấn đã thấy bài đó.
   Dùng cho: phân loại chủ đề, sentiment, NER, embedding, phát hiện trùng lặp (title_hash). Giữ nguyên cột gốc. #}
with g as (
    select news_id, any_value(title) as title, any_value(summary) as summary, any_value(url) as url,
           any_value(publisher) as publisher, min(published_at) as published_at, any_value(language) as language,
           any_value(source_type) as source_type, list(distinct search_query order by search_query) as search_queries,
           min(ingest_date) as first_seen_date, max(ingest_date) as last_seen_date
    from {{ ref('stg_google_news') }}
    group by news_id
)
select g.*, cast(published_at as date) as doc_date,
       trim(coalesce(title, '') || ' ' || coalesce(summary, ''))                                  as text,
       len(string_split(trim(coalesce(title, '') || ' ' || coalesce(summary, '')), ' '))          as n_words,
       md5(lower(trim(coalesce(title, ''))))                                                      as title_hash
from g
