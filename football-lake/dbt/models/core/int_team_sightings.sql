{{ config(materialized='view') }}
{# Mọi lần 1 CLB xuất hiện ở mọi nguồn (kể cả suy ra từ bảng trận). prio nhỏ = ưu tiên khi gộp thuộc tính. #}

with fdo_teams as (
    select 'fdo' as source, 1 as prio, cast(team_id as varchar) as source_team_id, name as raw_name,
           short_name, tla, founded, area as country, venue as venue_name, cast(null as varchar) as venue_city,
           cast(null as integer) as venue_capacity, cast(null as varchar) as logo_url, cast(null as varchar) as badge_url,
           cast(null as varchar) as website, club_colors, coach_name
    from {{ ref('stg_football_data_org_teams') }}
),
fdo_match_teams as (
    select distinct 'fdo', 1, cast(home_team_id as varchar), home_team, cast(null as varchar), home_tla, cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer), cast(null as varchar),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_football_data_org_matches') }}
    union
    select distinct 'fdo', 1, cast(away_team_id as varchar), away_team, cast(null as varchar), away_tla, cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer), cast(null as varchar),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_football_data_org_matches') }}
),
api_teams as (
    select 'api_football', 2, cast(team_id as varchar), name, cast(null as varchar), code, founded, country,
           venue_name, venue_city, venue_capacity, logo_url, cast(null as varchar), cast(null as varchar),
           cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_teams') }}
),
api_fixture_teams as (
    select distinct 'api_football', 2, cast(home_team_id as varchar), home_team_name, cast(null as varchar), cast(null as varchar),
           cast(null as integer), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_fixtures') }}
    union
    select distinct 'api_football', 2, cast(away_team_id as varchar), away_team_name, cast(null as varchar), cast(null as varchar),
           cast(null as integer), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_fixtures') }}
),
tsdb as (
    select 'thesportsdb', 3, team_id, team_name, team_short, cast(null as varchar), formed_year, country,
           stadium, stadium_location, stadium_capacity, cast(null as varchar), badge_url, website,
           cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_thesportsdb_teams') }}
),
wd as (
    select 'wikidata', 4, club_qid, club_name, cast(null as varchar), cast(null as varchar), year(inception_date), cast(null as varchar),
           venue_name, cast(null as varchar), venue_capacity, cast(null as varchar), cast(null as varchar), website_url,
           cast(null as varchar), coach_name
    from {{ ref('stg_wikidata_clubs') }}
),
us as (
    select distinct 'understat', 5, cast(team_id as varchar), team_name, cast(null as varchar), cast(null as varchar), cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer), cast(null as varchar),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from {{ ref('stg_understat_teams') }}
),
couk as (
    select distinct 'football_data_co_uk', 6, cast(null as varchar), t, cast(null as varchar), cast(null as varchar), cast(null as integer),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as integer), cast(null as varchar),
           cast(null as varchar), cast(null as varchar), cast(null as varchar), cast(null as varchar)
    from (select home_team as t from {{ ref('stg_football_data_co_uk') }}
          union select away_team from {{ ref('stg_football_data_co_uk') }})
),
u as (
    select * from fdo_teams
    union all select * from fdo_match_teams
    union all select * from api_teams
    union all select * from api_fixture_teams
    union all select * from tsdb
    union all select * from wd
    union all select * from us
    union all select * from couk
)

select
    source, prio, source_team_id, raw_name,
    {{ team_key('raw_name') }} as team_key,
    {{ team_mapped('raw_name') }} as is_mapped,
    short_name, tla, founded, country, venue_name, venue_city, venue_capacity,
    logo_url, badge_url, website, club_colors, coach_name
from u
where raw_name is not null and trim(raw_name) <> ''
