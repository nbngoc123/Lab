{{ config(materialized='view') }}
{# Trận OpenLigaDB đưa vào core: chỉ bóng đá (sport_id = 1), có đủ 2 đội và giờ đá.
   Giới hạn giải: --vars '{openliga_core_shortcuts: [bl1, bl2]}' (mặc định rỗng = mọi giải bóng đá >= openliga_min_season). #}
{% set core_sc = var('openliga_core_shortcuts', []) %}

select *
from {{ ref('stg_openliga_matches') }}
where sport_id = {{ var('openliga_football_sport_id', 1) }}
  and home_team_id is not null and away_team_id is not null
  and home_team_id <> away_team_id
  and trim(home_team) <> trim(away_team)
  and match_time_utc is not null
  and match_time_utc >= timestamp '2000-01-01'   -- OpenLigaDB có giải rác với giờ đá placeholder 1970-01-01
  -- Loại các giải test / game ảo / giải dummy có bằng chứng rõ ràng:
  --   test-del, tst1: giải test của người dùng
  --   goalunited23, bulimo: game quản lý bóng đá ảo
  --   klb: giải thử nghiệm Kreisliga B
  --   essl: giải 'Empty Stands Dummy Team' (chứa toàn bộ đội ảo)
  --   blclaude: giải test tự tạo
  and lower(league_shortcut) not in ('test-del', 'goalunited23', 'bulimo', 'klb', 'essl', 'blclaude', 'tst1')
  {% if core_sc %}and league_shortcut in ({{ core_sc | map('tojson') | join(', ') | replace('"', "'") }}){% endif %}
