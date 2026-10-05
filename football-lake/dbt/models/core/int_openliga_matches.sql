{{ config(materialized='view') }}
{# Trận OpenLigaDB đưa vào core: chỉ bóng đá (sport_id = 1), có đủ 2 đội và giờ đá.
   Giới hạn giải: --vars '{openliga_core_shortcuts: [bl1, bl2]}' (mặc định rỗng = mọi giải bóng đá >= openliga_min_season). #}
{% set core_sc = var('openliga_core_shortcuts', []) %}

select *
from {{ ref('stg_openliga_matches') }}
where sport_id = {{ var('openliga_football_sport_id', 1) }}
  and home_team_id is not null and away_team_id is not null
  and match_time_utc is not null
  {% if core_sc %}and league_shortcut in ({{ core_sc | map('tojson') | join(', ') | replace('"', "'") }}){% endif %}
