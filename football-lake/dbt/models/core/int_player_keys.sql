{{ config(materialized='view') }}
{# Gán player_key chuẩn cho từng sighting:
   1) có ngày sinh      -> 'p_' + md5(dob | họ)               (fdo, api_football, wikidata, thesportsdb)
   2) understat (không dob) -> ghép vào cầu thủ có dob nếu TÌM ĐƯỢC ĐÚNG 1 ứng viên
        (trùng tên đầy đủ, hoặc trùng họ + cùng đội); mơ hồ/không thấy -> 'u_' + id
   3) còn lại           -> '<source>_<id>' #}

with s as (
    select *,
           case when dob is not null
                then 'p_' || substr(md5(cast(dob as varchar) || '|' || last_norm), 1, 16) end as key_dob
    from {{ ref('int_player_sightings') }}
),

dob_set as (
    select distinct key_dob, name_norm, last_norm, team_key from s where key_dob is not null
),

cand as (
    select u.source_player_id, d.key_dob
    from (select distinct source_player_id, name_norm, last_norm, team_key from s where source = 'understat') u
    join dob_set d
      on d.name_norm = u.name_norm
      or (d.last_norm = u.last_norm and d.team_key = u.team_key)
    group by u.source_player_id, d.key_dob
),

resolved as (
    select source_player_id, any_value(key_dob) as key_dob
    from cand
    group by source_player_id
    having count(*) = 1
)

select
    s.*,
    coalesce(s.key_dob, r.key_dob, s.source || '_' || s.source_player_id) as player_key,
    (s.key_dob is null and r.key_dob is not null)                          as matched_by_name
from s
left join resolved r on s.source = 'understat' and r.source_player_id = s.source_player_id
