{{ config(materialized='external') }}
{# TEXT ML (tách riêng): bài Wikipedia (nội dung dài). 1 dòng / (page_id, language, category), lấy bản mới nhất. #}
select page_id, title, category, language, content, word_count, fetched_date,
       len(string_split(trim(coalesce(content, '')), ' ')) as n_words_counted
from {{ ref('stg_wikipedia') }}
qualify row_number() over (partition by page_id, language, category order by fetched_date desc) = 1
