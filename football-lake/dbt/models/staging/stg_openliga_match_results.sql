{# Mỗi trận có nhiều kết quả theo loại: HalfTime / After90Minutes / AfterExtraTime / AfterPenalties. #}

select
    {{ jget('a', '$.result_id', 'int') }}         as result_id,
    {{ jget('a', '$.match_id', 'int') }}          as match_id,
    {{ jget('a', '$.result_name') }}              as result_name,
    {{ jget('a', '$.result_order', 'int') }}      as result_order,
    {{ jget('a', '$.result_type_id', 'int') }}    as result_type_id,
    {{ jget('a', '$.result_type_kind') }}         as result_type_kind,
    {{ jget('a', '$.points_team1', 'int') }}      as points_team1,
    {{ jget('a', '$.points_team2', 'int') }}      as points_team2
from {{ cdc_current('match_results', 'result_id') }} as c
