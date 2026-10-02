{{ config(materialized='view') }}
{# Mọi lần 1 cầu thủ xuất hiện ở mọi nguồn. prio nhỏ = ưu tiên khi gộp thuộc tính. #}

with fdo as (
    select 'fdo' as source, 1 as prio, cast(player_id as varchar) as source_player_id, name as player_name,
           cast(null as varchar) as last_name_hint, date_of_birth as dob, nationality, position,
           team_name as team_name_raw, cast(null as integer) as height_cm, cast(null as varchar) as photo_url,
           cast(null as integer) as season, cast(null as varchar) as league
    from {{ ref('stg_football_data_org_players') }}
),
api as (
    select 'api_football', 2, cast(player_id as varchar),
           coalesce(nullif(trim(coalesce(firstname, '') || ' ' || coalesce(lastname, '')), ''), name),
           lastname, birth_date, nationality, position, team_name,
           try_cast(regexp_extract(height, '(\d+)', 1) as integer), photo_url, season, cast(null as varchar)
    from {{ ref('stg_players') }}
),
wd as (
    select 'wikidata', 3, player_qid, player_name, cast(null as varchar), date_of_birth, country, position, club_name,
           cast(case when height_cm < 3 then height_cm * 100 else height_cm end as integer),
           cast(null as varchar), cast(null as integer), cast(null as varchar)
    from {{ ref('stg_wikidata_players') }}
),
tsdb as (
    select 'thesportsdb', 4, player_id, player_name, cast(null as varchar), birth_date, nationality, position,
           cast(null as varchar), cast(null as integer), cast(null as varchar), cast(null as integer), cast(null as varchar)
    from {{ ref('stg_thesportsdb_players') }}
),
us as (
    select 'understat', 5, cast(player_id as varchar), player_name, cast(null as varchar), cast(null as date),
           cast(null as varchar), position,
           list_extract(string_split(team_name, ','), -1),      -- chuyển đội giữa mùa: lấy đội cuối
           cast(null as integer), cast(null as varchar), season, league
    from {{ ref('stg_understat_players') }}
),
u as (
    select * from fdo union all select * from api union all select * from wd
    union all select * from tsdb union all select * from us
)

select
    source, prio, source_player_id, player_name,
    {{ name_norm('player_name') }}                              as name_norm,
    {{ last_word("coalesce(last_name_hint, player_name)") }}    as last_norm,
    dob, nationality, position,
    {{ team_key('team_name_raw') }}                             as team_key,
    height_cm, photo_url, season, league
from u
where player_name is not null and trim(player_name) <> ''
