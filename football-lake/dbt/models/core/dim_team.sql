{{ config(materialized='table') }}
{# 1 dòng / CLB. Khóa = team_key (theo seed team_alias). Tên không có trong seed vẫn có dòng (in_alias_seed=false). #}

with s as (select * from {{ ref('int_team_sightings') }}),

agg as (
    select
        team_key,
        arg_min(short_name,       prio) filter (where short_name is not null)       as short_name,
        arg_min(tla,              prio) filter (where tla is not null)              as tla,
        arg_min(country,          prio) filter (where country is not null)          as country,
        arg_min(founded,          prio) filter (where founded is not null)          as founded,
        arg_min(venue_name,       prio) filter (where venue_name is not null)       as stadium_name,
        arg_min(venue_city,       prio) filter (where venue_city is not null)       as stadium_city,
        arg_min(venue_capacity,   prio) filter (where venue_capacity is not null)   as stadium_capacity,
        arg_min(logo_url,         prio) filter (where logo_url is not null)         as logo_url,
        arg_min(badge_url,        prio) filter (where badge_url is not null)        as badge_url,
        arg_min(website,          prio) filter (where website is not null)          as website,
        arg_min(club_colors,      prio) filter (where club_colors is not null)      as club_colors,
        arg_min(coach_name,       prio) filter (where coach_name is not null)       as coach_name,
        max(source_team_id) filter (where source = 'fdo')          as fdo_team_id,
        max(source_team_id) filter (where source = 'api_football') as api_football_team_id,
        max(source_team_id) filter (where source = 'thesportsdb')  as thesportsdb_team_id,
        max(source_team_id) filter (where source = 'understat')    as understat_team_id,
        max(source_team_id) filter (where source = 'wikidata')     as wikidata_qid,
        bool_or(is_mapped)                  as in_alias_seed,
        count(distinct source)              as n_sources
    from s
    where team_key is not null
    group by team_key
),

home_venue as (
    select * from {{ ref('dim_venue') }}
    where team_key is not null
    qualify row_number() over (partition by team_key order by capacity desc nulls last, venue_qid) = 1
)

select
    a.team_key,
    a.team_key              as team_name,
    a.short_name, a.tla, a.country, a.founded,
    a.stadium_name, a.stadium_city, a.stadium_capacity,
    a.logo_url, a.badge_url, a.website, a.club_colors, a.coach_name,
    v.venue_qid, v.lat as venue_lat, v.lon as venue_lon, v.openmeteo_venue_key,
    a.fdo_team_id, a.api_football_team_id, a.thesportsdb_team_id, a.understat_team_id, a.wikidata_qid,
    a.in_alias_seed, a.n_sources
from agg a
left join home_venue v on v.team_key = a.team_key
