{{ config(materialized='view') }}
{# Tên đội KHÔNG có trong seed/team_alias.csv. Thêm từng dòng (source,alias,team_key) vào seed để
   các nguồn ghép được với nhau. Dòng nào còn ở đây thì trận của đội đó có thể bị tách đôi trong fact_match. #}

with names as (
    select 'fdo_matches' as source, home_team as raw_name from {{ ref('stg_football_data_org_matches') }}
    union all select 'fdo_matches', away_team from {{ ref('stg_football_data_org_matches') }}
    union all select 'football_data_co_uk', home_team from {{ ref('stg_football_data_co_uk') }}
    union all select 'football_data_co_uk', away_team from {{ ref('stg_football_data_co_uk') }}
    union all select 'understat', home_team_name from {{ ref('stg_understat_matches') }}
    union all select 'understat', away_team_name from {{ ref('stg_understat_matches') }}
    union all select 'api_football', home_team_name from {{ ref('stg_fixtures') }}
    union all select 'api_football', away_team_name from {{ ref('stg_fixtures') }}
    union all select 'odds_api', home_team from {{ ref('stg_odds_h2h') }}
    union all select 'odds_api', away_team from {{ ref('stg_odds_h2h') }}
    union all select 'fdo_teams', name from {{ ref('stg_football_data_org_teams') }}
    union all select 'thesportsdb', team_name from {{ ref('stg_thesportsdb_teams') }}
    union all select 'wikidata', club_name from {{ ref('stg_wikidata_clubs') }}
)
select source, raw_name, count(*) as n_rows
from names
where raw_name is not null and trim(raw_name) <> ''
  and not {{ team_mapped('raw_name') }}
group by source, raw_name
order by n_rows desc, source, raw_name
