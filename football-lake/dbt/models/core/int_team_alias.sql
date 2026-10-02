{{ config(materialized='view') }}
{# 1 alias -> đúng 1 team_key. Gồm alias trong seed + chính tên canonical (tự map về chính nó).
   Nếu seed gán cùng 1 alias cho 2 team_key khác nhau thì test unique ở schema.yml sẽ FAIL.
   Loại các key không phải CLB: 'unknown' (video/người, dùng cho NLP) và tên giải đấu (nhãn từ Wikimedia). #}

with a as (
    select lower(trim(alias)) as alias_lower, trim(team_key) as team_key
    from {{ ref('team_alias') }}
    where alias is not null and team_key is not null and trim(team_key) not in {{ var('non_team_keys') }}
    union
    select lower(trim(team_key)), trim(team_key)
    from {{ ref('team_alias') }}
    where team_key is not null and trim(team_key) not in {{ var('non_team_keys') }}
)
select distinct alias_lower, team_key from a
