{{ config(materialized='external') }}
{# 1 dòng / cầu thủ (đã gộp qua các nguồn). is_matched_across_sources=false nghĩa là chỉ thấy ở 1 nguồn. #}

select
    player_key,
    arg_min(player_name, prio)  filter (where player_name is not null)  as player_name,
    arg_min(name_norm,   prio)                                          as name_norm,
    arg_min(dob,         prio)  filter (where dob is not null)          as date_of_birth,
    arg_min(nationality, prio)  filter (where nationality is not null)  as nationality,
    arg_min(position,    prio)  filter (where position is not null)     as position,
    arg_min(team_key,    prio)  filter (where team_key is not null)     as current_team_key,
    arg_min(height_cm,   prio)  filter (where height_cm is not null)    as height_cm,
    arg_min(photo_url,   prio)  filter (where photo_url is not null)    as photo_url,
    max(source_player_id) filter (where source = 'fdo')          as fdo_player_id,
    max(source_player_id) filter (where source = 'api_football') as api_football_player_id,
    max(source_player_id) filter (where source = 'wikidata')     as wikidata_qid,
    max(source_player_id) filter (where source = 'thesportsdb')  as thesportsdb_player_id,
    max(source_player_id) filter (where source = 'understat')    as understat_player_id,
    count(distinct source)                                       as n_sources,
    count(distinct source) > 1                                   as is_matched_across_sources
from {{ ref('int_player_keys') }}
group by player_key
