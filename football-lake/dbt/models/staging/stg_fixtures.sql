{# Dedup: giữ snapshot mới nhất của mỗi fixture (kết quả/trạng thái thay đổi theo thời gian). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/api_football/fixtures/**/*.json.gz') }}, filename=true)
),

items as (
    select filename, {{ jarray('doc', '$.response') }} as f from raw
)

select
    {{ jget('f', '$.fixture.id', 'int') }}              as fixture_id,
    {{ jts('f', '$.fixture.date') }}                    as fixture_date,
    {{ jget('f', '$.fixture.referee') }}                as referee,
    {{ jget('f', '$.fixture.venue.id', 'int') }}        as venue_id,
    {{ jget('f', '$.fixture.venue.name') }}             as venue_name,
    {{ jget('f', '$.fixture.status.long') }}            as status_long,
    {{ jget('f', '$.fixture.status.short') }}           as status_short,
    {{ jget('f', '$.fixture.status.elapsed', 'int') }}  as elapsed_minutes,

    {{ jget('f', '$.league.id', 'int') }}               as league_id,
    {{ jget('f', '$.league.season', 'int') }}           as season,
    {{ jget('f', '$.league.round') }}                   as round,

    {{ jget('f', '$.teams.home.id', 'int') }}           as home_team_id,
    {{ jget('f', '$.teams.home.name') }}                as home_team_name,
    {{ jget('f', '$.teams.away.id', 'int') }}           as away_team_id,
    {{ jget('f', '$.teams.away.name') }}                as away_team_name,

    {{ jget('f', '$.goals.home', 'int') }}              as home_goals,
    {{ jget('f', '$.goals.away', 'int') }}              as away_goals,
    {{ jget('f', '$.score.halftime.home', 'int') }}     as ht_home_goals,
    {{ jget('f', '$.score.halftime.away', 'int') }}     as ht_away_goals,
    {{ jget('f', '$.score.penalty.home', 'int') }}      as pen_home_goals,
    {{ jget('f', '$.score.penalty.away', 'int') }}      as pen_away_goals,
    {{ path_date() }}                                   as ingest_date
from items
where json_extract_string(f, '$.fixture.id') is not null
qualify row_number() over (
    partition by fixture_id
    order by ingest_date desc nulls last, filename desc) = 1
