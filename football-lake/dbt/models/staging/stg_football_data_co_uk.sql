{{ config(materialized='view') }}
{# all_varchar + try_cast: ô trống / cột lệch giữa các mùa không làm vỡ model.
   Ngày có 2 định dạng (dd/mm/yy và dd/mm/yyyy). Season lấy từ path. #}

{% set raw_cols = ['Div','Date','Time','HomeTeam','AwayTeam','FTHG','FTAG','FTR','HTHG','HTAG','HTR','Referee',
                   'HS','AS','HST','AST','HF','AF','HC','AC','HY','AY','HR','AR',
                   'B365H','B365D','B365A','AvgH','AvgD','AvgA','MaxH','MaxD','MaxA'] %}

with raw as (
{%- if lake_has_files('raw/football_data_couk/**/*.csv') | trim == 'true' %}
    select *
    from read_csv({{ lake_path('raw/football_data_couk/**/*.csv') }},
                  header=true, union_by_name=true, all_varchar=true,
                  filename=true, ignore_errors=true)
{%- else %}
    -- chưa có CSV nào trong raw: trả bảng rỗng cùng schema
    select null::varchar as filename,
           {% for c in raw_cols %}null::varchar as "{{ c }}"{{ ',' if not loop.last }} {% endfor %}
    where false
{%- endif %}
)

select
    nullif(trim(raw.Div), '')                                   as division,
    {{ path_part('season') }}                               as season_code,      -- vd 2425
    2000 + try_cast(left({{ path_part('season') }}, 2) as int) as season_start_year,
    nullif(raw.Date, '')                                        as match_date_raw,
    nullif(raw."Time", '')                                      as match_time_raw,
    coalesce(try_strptime(raw.Date, '%d/%m/%Y'), try_strptime(raw.Date, '%d/%m/%y'))::date as match_date,
    try_cast(nullif(raw."Time", '') as time)                    as match_time,
    trim(raw.HomeTeam)                                          as home_team,
    trim(raw.AwayTeam)                                          as away_team,

    try_cast(raw.FTHG as int)  as home_goals,
    try_cast(raw.FTAG as int)  as away_goals,
    nullif(raw.FTR, '')        as match_result,
    try_cast(raw.HTHG as int)  as ht_home_goals,
    try_cast(raw.HTAG as int)  as ht_away_goals,
    nullif(raw.HTR, '')        as ht_result,

    try_cast(raw.HS as int)    as home_shots,
    try_cast(raw."AS" as int)  as away_shots,
    try_cast(raw.HST as int)   as home_shots_target,
    try_cast(raw.AST as int)   as away_shots_target,
    try_cast(raw.HC as int)    as home_corners,
    try_cast(raw.AC as int)    as away_corners,
    try_cast(raw.HF as int)    as home_fouls,
    try_cast(raw.AF as int)    as away_fouls,
    try_cast(raw.HY as int)    as home_yellows,
    try_cast(raw.AY as int)    as away_yellows,
    try_cast(raw.HR as int)    as home_reds,
    try_cast(raw.AR as int)    as away_reds,
    nullif(raw.Referee, '')    as referee,

    -- tỉ lệ kèo (Bet365, trung bình & cao nhất thị trường)
    try_cast(raw.B365H as double) as b365_home,
    try_cast(raw.B365D as double) as b365_draw,
    try_cast(raw.B365A as double) as b365_away,
    try_cast(raw.AvgH as double)  as avg_home,
    try_cast(raw.AvgD as double)  as avg_draw,
    try_cast(raw.AvgA as double)  as avg_away,
    try_cast(raw.MaxH as double)  as max_home,
    try_cast(raw.MaxD as double)  as max_draw,
    try_cast(raw.MaxA as double)  as max_away
from raw
where nullif(trim(raw.HomeTeam), '') is not null
qualify row_number() over (
    partition by division, season_code, match_date, home_team, away_team
    order by filename) = 1
