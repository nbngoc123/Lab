{# 1 dòng / bàn thắng. scoring_team_id là đội được TÍNH bàn (bàn phản lưới: đội hưởng lợi, không phải đội cầu thủ). #}

select
    {{ jget('a', '$.goal_id', 'int') }}           as goal_id,
    {{ jget('a', '$.match_id', 'int') }}          as match_id,
    {{ jget('a', '$.minute', 'int') }}            as minute,
    {{ jget('a', '$.scorer_id', 'int') }}         as scorer_id,
    {{ jget('a', '$.scorer_name') }}              as scorer_name,
    {{ jget('a', '$.scoring_team_id', 'int') }}   as scoring_team_id,
    {{ jget('a', '$.score_team1', 'int') }}       as score_team1,
    {{ jget('a', '$.score_team2', 'int') }}       as score_team2,
    coalesce({{ jget('a', '$.is_penalty', 'boolean') }}, false)   as is_penalty,
    coalesce({{ jget('a', '$.is_own_goal', 'boolean') }}, false)  as is_own_goal,
    coalesce({{ jget('a', '$.is_overtime', 'boolean') }}, false)  as is_overtime,
    {{ jget('a', '$.comment') }}                  as comment
from {{ cdc_current('goals', 'goal_id') }} as c
where {{ jget('a', '$.match_id', 'int') }} in (select match_id from {{ ref('stg_openliga_matches') }})
