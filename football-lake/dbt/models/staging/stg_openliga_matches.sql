{# 1 dòng / trận (trạng thái hiện tại), đã gắn giải-mùa, tên đội, sân và tỉ số từng loại kết quả.
   Chỉ lấy mùa >= var('openliga_min_season') (mặc định 2024). Đổi: --vars '{openliga_min_season: 2021}'.
   Tên đội/giải là tiếng Đức như API; ánh xạ sang team_key ở tầng core (seed team_alias). #}

with m as (
    select
        {{ jget('a', '$.match_id', 'int') }}              as match_id,
        {{ jget('a', '$.league_id', 'int') }}             as league_id,
        {{ jget('a', '$.group_id', 'int') }}              as group_id,
        {{ jget('a', '$.team1_id', 'int') }}              as team1_id,
        {{ jget('a', '$.team2_id', 'int') }}              as team2_id,
        {{ jget('a', '$.location_id', 'int') }}           as location_id,
        {{ cdc_ts('a', '$.match_time_utc') }}             as match_time_utc,
        {{ cdc_ts('a', '$.match_time_local') }}           as match_time_local,
        {{ jget('a', '$.time_zone_id') }}                 as time_zone_id,
        coalesce({{ jget('a', '$.is_finished', 'boolean') }}, false) as is_finished,
        {{ jget('a', '$.viewers', 'int') }}               as viewers,
        {{ cdc_ts('a', '$.api_updated_at') }}             as api_updated_at
    from {{ cdc_current('matches', 'match_id') }} as c
),

r as (
    select
        match_id,
        max(points_team1) filter (where result_type_kind = 'HalfTime')       as ht_home_goals,
        max(points_team2) filter (where result_type_kind = 'HalfTime')       as ht_away_goals,
        max(points_team1) filter (where result_type_kind = 'After90Minutes') as home_goals,
        max(points_team2) filter (where result_type_kind = 'After90Minutes') as away_goals,
        max(points_team1) filter (where result_type_kind = 'AfterExtraTime') as et_home_goals,
        max(points_team2) filter (where result_type_kind = 'AfterExtraTime') as et_away_goals,
        max(points_team1) filter (where result_type_kind = 'AfterPenalties') as pen_home_goals,
        max(points_team2) filter (where result_type_kind = 'AfterPenalties') as pen_away_goals
    from {{ ref('stg_openliga_match_results') }}
    group by match_id
)

select
    m.match_id,
    m.league_id,
    l.league_shortcut,
    l.season,
    l.league_name,
    l.sport_id,
    m.group_id,
    g.group_order,
    g.group_name,
    m.team1_id                      as home_team_id,
    t1.team_name                    as home_team,
    t1.short_name                   as home_short,
    m.team2_id                      as away_team_id,
    t2.team_name                    as away_team,
    t2.short_name                   as away_short,
    loc.stadium                     as venue_name,
    loc.city                        as venue_city,
    m.match_time_utc,
    m.match_time_local,
    m.time_zone_id,
    m.is_finished,
    case when m.is_finished then 'FINISHED' else 'SCHEDULED' end as status,
    m.viewers,
    r.home_goals, r.away_goals,
    r.ht_home_goals, r.ht_away_goals,
    r.et_home_goals, r.et_away_goals,
    r.pen_home_goals, r.pen_away_goals,
    m.api_updated_at
from m
join {{ ref('stg_openliga_leagues') }} l on l.league_id = m.league_id      -- đồng thời lọc theo mùa
left join {{ ref('stg_openliga_groups') }} g on g.group_id = m.group_id
left join {{ ref('stg_openliga_teams') }} t1 on t1.team_id = m.team1_id
left join {{ ref('stg_openliga_teams') }} t2 on t2.team_id = m.team2_id
left join {{ ref('stg_openliga_locations') }} loc on loc.location_id = m.location_id
left join r on r.match_id = m.match_id
