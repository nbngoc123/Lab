{{ config(materialized='view') }}
{# p08 ghi: raw/thesportsdb/teams/league=<slug>/ingest_date=<D>/teams.json.gz (trước đây model trỏ sai vào metadata/entity=teams). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/thesportsdb/teams/**/*.json.gz') }}, filename=true)
),
items as (select filename, {{ jarray('doc', '$.teams') }} as t from raw)

select
    {{ jget('t', '$.idTeam') }}                     as team_id,
    {{ jget('t', '$.strTeam') }}                    as team_name,
    {{ jget('t', '$.strTeamShort') }}               as team_short,
    {{ jget('t', '$.strTeamAlternate') }}           as alternate_names,
    {{ jget('t', '$.intFormedYear', 'int') }}       as formed_year,
    {{ jget('t', '$.idLeague') }}                   as league_id,
    {{ jget('t', '$.strLeague') }}                  as league,
    {{ path_part('league') }}                       as league_slug,
    {{ jget('t', '$.strCountry') }}                 as country,
    {{ jget('t', '$.strStadium') }}                 as stadium,
    {{ jget('t', '$.intStadiumCapacity', 'int') }}  as stadium_capacity,
    {{ jget('t', '$.strLocation') }}                as stadium_location,
    {{ jget('t', '$.strWebsite') }}                 as website,
    {{ jget('t', '$.strBadge') }}                   as badge_url,
    left({{ jget('t', '$.strDescriptionEN') }}, 2000) as description_en,
    {{ path_date() }}                               as ingest_date
from items
where json_extract_string(t, '$.idTeam') is not null
qualify row_number() over (partition by team_id order by ingest_date desc nulls last, filename desc) = 1
