{{ config(materialized='external') }}
{# Thống kê cầu thủ theo mùa/giải từ Understat. Grain: player_key + competition_key + season + team_key. #}

with k as (
    select distinct source_player_id, player_key
    from {{ ref('int_player_keys') }}
    where source = 'understat'
)
select
    k.player_key,
    coalesce(c.competition_key, p.league)                            as competition_key,
    p.season,
    {{ team_key("list_extract(string_split(p.team_name, ','), -1)") }} as team_key,
    p.team_name                                                      as team_names_raw,
    p.position, p.games, p.minutes_played, p.goals, p.assists, p.shots, p.key_passes,
    p.yellow_cards, p.red_cards, p.non_penalty_goals,
    p.xg, p.xa, p.non_penalty_xg, p.xg_chain, p.xg_buildup,
    case when p.minutes_played > 0 then round(p.goals * 90.0 / p.minutes_played, 3) end as goals_per90,
    case when p.minutes_played > 0 then round(p.xg    * 90.0 / p.minutes_played, 3) end as xg_per90,
    case when p.minutes_played > 0 then round(p.xa    * 90.0 / p.minutes_played, 3) end as xa_per90
from {{ ref('stg_understat_players') }} p
join k on k.source_player_id = cast(p.player_id as varchar)
left join {{ ref('dim_competition') }} c on c.understat_league = p.league
