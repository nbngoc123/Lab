{# all_varchar + try_cast: ô trống / cột lệch giữa các mùa không làm vỡ model.
   Ngày có 2 định dạng (dd/mm/yy và dd/mm/yyyy). Season lấy từ path. #}

with raw as (
    select *
    from read_csv({{ lake_path('raw/football_data_couk/**/*.csv') }},
                  header=true, union_by_name=true, all_varchar=true,
                  filename=true, ignore_errors=true)
)

select
    nullif(trim(Div), '')                                   as division,
    {{ path_part('season') }}                               as season_code,      -- vd 2425
    2000 + try_cast(left({{ path_part('season') }}, 2) as int) as season_start_year,
    nullif(Date, '')                                        as match_date_raw,
    nullif("Time", '')                                      as match_time_raw,
    coalesce(try_strptime(Date, '%d/%m/%Y'), try_strptime(Date, '%d/%m/%y'))::date as match_date,
    try_cast(nullif("Time", '') as time)                    as match_time,
    trim(HomeTeam)                                          as home_team,
    trim(AwayTeam)                                          as away_team,

    try_cast(FTHG as int)  as home_goals,
    try_cast(FTAG as int)  as away_goals,
    nullif(FTR, '')        as match_result,
    try_cast(HTHG as int)  as ht_home_goals,
    try_cast(HTAG as int)  as ht_away_goals,
    nullif(HTR, '')        as ht_result,

    try_cast(HS as int)    as home_shots,
    try_cast("AS" as int)  as away_shots,
    try_cast(HST as int)   as home_shots_target,
    try_cast(AST as int)   as away_shots_target,
    try_cast(HC as int)    as home_corners,
    try_cast(AC as int)    as away_corners,
    try_cast(HF as int)    as home_fouls,
    try_cast(AF as int)    as away_fouls,
    try_cast(HY as int)    as home_yellows,
    try_cast(AY as int)    as away_yellows,
    try_cast(HR as int)    as home_reds,
    try_cast(AR as int)    as away_reds,
    nullif(Referee, '')    as referee,

    -- tỉ lệ kèo (Bet365, trung bình & cao nhất thị trường)
    try_cast(B365H as double) as b365_home,
    try_cast(B365D as double) as b365_draw,
    try_cast(B365A as double) as b365_away,
    try_cast(AvgH as double)  as avg_home,
    try_cast(AvgD as double)  as avg_draw,
    try_cast(AvgA as double)  as avg_away,
    try_cast(MaxH as double)  as max_home,
    try_cast(MaxD as double)  as max_draw,
    try_cast(MaxA as double)  as max_away
from raw
where nullif(trim(HomeTeam), '') is not null
qualify row_number() over (
    partition by division, season_code, match_date, home_team, away_team
    order by filename) = 1
