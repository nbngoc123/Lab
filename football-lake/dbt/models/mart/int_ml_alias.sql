{{ config(materialized='view') }}
{# Alias đội đã chuẩn hoá (bỏ dấu, a-z, khoảng trắng) và dạng không dấu cách. CHỈ dùng để map tên bài Wikipedia -> team_key
   (mart_ml_team_attention_daily). Văn bản tự do (tin tức/YouTube) KHÔNG được nối vào đội bằng bảng này. #}
select
    alias_norm,
    replace(alias_norm, ' ', '') as alias_compact,
    team_key
from (
    select distinct {{ name_norm('alias_lower') }} as alias_norm, team_key
    from {{ ref('int_team_alias') }}
)
where alias_norm <> ''
