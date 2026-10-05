{{ config(materialized='external') }}
{# 1 dòng / trận ngoài đời thật, gộp 5 nguồn trận đấu (fdo, co.uk, understat, api_football, openliga) + odds + thời tiết.
   Khóa trận = md5(đội nhà | đội khách | ngày) sau khi chuẩn hóa tên đội bằng seed team_alias,
   nên cùng 1 trận ở football-data.org / .co.uk / Understat / API-Football ghép được với nhau.
   Ưu tiên giá trị: fdo > co.uk > understat > api_football > openliga. #}

with comp as (select * from {{ ref('dim_competition') }}),

-- ---------- 1. football-data.org (có cả trận sắp đá) ----------
fdo_raw as (
    select
        cast(m.match_id as varchar)                           as fdo_match_id,
        coalesce(c.competition_key, upper(m.competition_code)) as competition_key,
        cast(m.utc_date as date)                              as match_date,
        m.utc_date                                            as kickoff_utc,
        {{ team_key('m.home_team') }}                         as home_key,
        {{ team_key('m.away_team') }}                         as away_key,
        m.status, m.stage, m.matchday, m.referee,
        m.home_goals, m.away_goals, m.ht_home_goals, m.ht_away_goals,
        (m.status = 'FINISHED')                               as is_finished
    from {{ ref('stg_football_data_org_matches') }} m
    left join comp c on c.fdo_code = m.competition_code
    where m.utc_date is not null
),
fdo as (
    select *, {{ match_key('home_key', 'away_key', 'match_date') }} as match_key,
           cast(home_goals as varchar) || '-' || cast(away_goals as varchar) as score_str
    from fdo_raw
    qualify row_number() over (partition by {{ match_key('home_key', 'away_key', 'match_date') }}
                               order by kickoff_utc) = 1
),

-- ---------- 2. football-data.co.uk (kết quả + thống kê + kèo lịch sử) ----------
couk_raw as (
    select
        coalesce(c.competition_key, s.division)               as competition_key,
        s.match_date,
        case when s.match_time is not null
             then timezone('UTC', timezone('Europe/London', s.match_date + s.match_time)) end as kickoff_utc,
        {{ team_key('s.home_team') }}                         as home_key,
        {{ team_key('s.away_team') }}                         as away_key,
        s.referee,
        s.home_goals, s.away_goals, s.ht_home_goals, s.ht_away_goals,
        s.home_shots, s.away_shots, s.home_shots_target, s.away_shots_target,
        s.home_corners, s.away_corners, s.home_fouls, s.away_fouls,
        s.home_yellows, s.away_yellows, s.home_reds, s.away_reds,
        s.b365_home, s.b365_draw, s.b365_away, s.avg_home, s.avg_draw, s.avg_away,
        s.max_home, s.max_draw, s.max_away
    from {{ ref('stg_football_data_co_uk') }} s
    left join comp c on c.couk_division = s.division
    where s.match_date is not null
),
couk as (
    select *, {{ match_key('home_key', 'away_key', 'match_date') }} as match_key,
           cast(home_goals as varchar) || '-' || cast(away_goals as varchar) as score_str
    from couk_raw
    qualify row_number() over (partition by {{ match_key('home_key', 'away_key', 'match_date') }}
                               order by match_date) = 1
),

-- ---------- 3. Understat (xG) ----------
us_raw as (
    select
        cast(u.match_id as varchar)                           as understat_match_id,
        coalesce(c.competition_key, u.league)                 as competition_key,
        cast(u.match_date as date)                            as match_date,
        {{ team_key('u.home_team_name') }}                    as home_key,
        {{ team_key('u.away_team_name') }}                    as away_key,
        u.is_result, u.home_goals, u.away_goals, u.home_xg, u.away_xg,
        u.forecast_home_win, u.forecast_draw, u.forecast_away_win
    from {{ ref('stg_understat_matches') }} u
    left join comp c on c.understat_league = u.league
    where u.match_date is not null
),
us as (
    select *, {{ match_key('home_key', 'away_key', 'match_date') }} as match_key,
           cast(home_goals as varchar) || '-' || cast(away_goals as varchar) as score_str
    from us_raw
    qualify row_number() over (partition by {{ match_key('home_key', 'away_key', 'match_date') }}
                               order by match_date) = 1
),

-- ---------- 4. API-Football ----------
af_raw as (
    select
        cast(a.fixture_id as varchar)                         as af_fixture_id,
        coalesce(c.competition_key, cast(a.league_id as varchar)) as competition_key,
        cast(a.fixture_date as date)                          as match_date,
        a.fixture_date                                        as kickoff_utc,
        {{ team_key('a.home_team_name') }}                    as home_key,
        {{ team_key('a.away_team_name') }}                    as away_key,
        a.referee, a.venue_name, a.round, a.status_short,
        a.home_goals, a.away_goals
    from {{ ref('stg_fixtures') }} a
    left join comp c on c.api_football_league_id = a.league_id
    where a.fixture_date is not null
),
af as (
    select *, {{ match_key('home_key', 'away_key', 'match_date') }} as match_key,
           cast(home_goals as varchar) || '-' || cast(away_goals as varchar) as score_str
    from af_raw
    qualify row_number() over (partition by {{ match_key('home_key', 'away_key', 'match_date') }}
                               order by kickoff_utc) = 1
),

-- ---------- 4b. OpenLigaDB (CDC từ Postgres football_source; có cả trận sắp đá) ----------
ol_raw as (
    select
        cast(o.match_id as varchar)                           as openliga_match_id,
        coalesce(c.competition_key, upper(o.league_shortcut)) as competition_key,
        cast(o.match_time_utc as date)                        as match_date,
        o.match_time_utc                                      as kickoff_utc,
        {{ team_key('o.home_team') }}                         as home_key,
        {{ team_key('o.away_team') }}                         as away_key,
        o.status, o.group_order                               as matchday, o.venue_name,
        o.home_goals, o.away_goals, o.ht_home_goals, o.ht_away_goals,
        (o.is_finished and o.home_goals is not null and o.away_goals is not null) as is_finished
    from {{ ref('int_openliga_matches') }} o
    left join comp c on c.openliga_shortcut = o.league_shortcut
),
ol as (
    select *, {{ match_key('home_key', 'away_key', 'match_date') }} as match_key,
           cast(home_goals as varchar) || '-' || cast(away_goals as varchar) as score_str
    from ol_raw
    qualify row_number() over (partition by {{ match_key('home_key', 'away_key', 'match_date') }}
                               order by kickoff_utc) = 1
),

-- ---------- 5. The Odds API (kèo hiện tại, trung bình qua các nhà cái) ----------
odds_k as (
    select
        {{ team_key('home_team') }} as home_key,
        {{ team_key('away_team') }} as away_key,
        cast(commence_time as date) as match_date,
        outcome_type, price, bookmaker_key, last_update
    from {{ ref('stg_odds_h2h') }}
),
odds as (
    select
        {{ match_key('home_key', 'away_key', 'match_date') }}          as match_key,
        avg(price) filter (where outcome_type = 'home')                as odds_api_home,
        avg(price) filter (where outcome_type = 'draw')                as odds_api_draw,
        avg(price) filter (where outcome_type = 'away')                as odds_api_away,
        count(distinct bookmaker_key)                                  as odds_api_n_bookmakers,
        max(last_update)                                               as odds_api_updated_at
    from odds_k
    group by 1
),

-- ---------- Spine: hợp mọi khóa trận từ 4 nguồn ----------
spine as (
    select match_key, min(match_date) as match_date, min(home_key) as home_key, min(away_key) as away_key
    from (
        select match_key, match_date, home_key, away_key from fdo
        union all select match_key, match_date, home_key, away_key from couk
        union all select match_key, match_date, home_key, away_key from us
        union all select match_key, match_date, home_key, away_key from af
        union all select match_key, match_date, home_key, away_key from ol
    ) x
    group by match_key
),

-- ---------- Thời tiết: sân nhà -> Open-Meteo, đúng giờ địa phương lúc giao bóng ----------
venue_tz as (
    select venue_name, any_value(timezone) as tz from {{ ref('stg_open_meteo') }} group by venue_name
),
wx as (select * from {{ ref('stg_open_meteo') }}),

base as (
    select
        s.match_key, s.match_date, s.home_key, s.away_key,
        coalesce(f.competition_key, c.competition_key, u.competition_key, a.competition_key, l.competition_key) as competition_key,
        coalesce(f.kickoff_utc, c.kickoff_utc, a.kickoff_utc, l.kickoff_utc)                 as kickoff_utc,
        f.fdo_match_id, u.understat_match_id, a.af_fixture_id, l.openliga_match_id,
        coalesce(f.status, l.status) as status, f.stage, coalesce(f.matchday, l.matchday) as matchday, a.round as af_round,
        coalesce(f.referee, c.referee, a.referee)    as referee,
        coalesce(a.venue_name, l.venue_name) as venue_name,
        (coalesce(f.is_finished, false) or (c.home_goals is not null)
         or coalesce(u.is_result, false)
         or coalesce(a.status_short in ('FT', 'AET', 'PEN'), false)
         or coalesce(l.is_finished, false))                                      as is_finished,
        coalesce(f.home_goals, c.home_goals, u.home_goals, a.home_goals, l.home_goals) as home_goals_any,
        coalesce(f.away_goals, c.away_goals, u.away_goals, a.away_goals, l.away_goals) as away_goals_any,
        coalesce(f.ht_home_goals, c.ht_home_goals, l.ht_home_goals) as ht_home_goals,
        coalesce(f.ht_away_goals, c.ht_away_goals, l.ht_away_goals) as ht_away_goals,
        (select count(distinct v) from (values (f.score_str), (c.score_str), (u.score_str), (a.score_str), (l.score_str)) t(v)
         where v is not null)                                                    as n_distinct_scores,
        -- thống kê trận (co.uk)
        c.home_shots, c.away_shots, c.home_shots_target, c.away_shots_target,
        c.home_corners, c.away_corners, c.home_fouls, c.away_fouls,
        c.home_yellows, c.away_yellows, c.home_reds, c.away_reds,
        -- kèo lịch sử trước trận (co.uk)
        c.b365_home, c.b365_draw, c.b365_away, c.avg_home as mkt_avg_home, c.avg_draw as mkt_avg_draw, c.avg_away as mkt_avg_away,
        c.max_home as mkt_max_home, c.max_draw as mkt_max_draw, c.max_away as mkt_max_away,
        -- xG (understat) của chính trận này
        u.home_xg, u.away_xg, u.forecast_home_win, u.forecast_draw, u.forecast_away_win,
        -- kèo hiện tại (odds api)
        o.odds_api_home, o.odds_api_draw, o.odds_api_away, o.odds_api_n_bookmakers, o.odds_api_updated_at,
        (f.match_key is not null) as in_fdo, (c.match_key is not null) as in_couk,
        (u.match_key is not null) as in_understat, (a.match_key is not null) as in_api_football,
        (l.match_key is not null) as in_openliga,
        (o.match_key is not null) as in_odds_api
    from spine s
    left join fdo  f on f.match_key = s.match_key
    left join couk c on c.match_key = s.match_key
    left join us   u on u.match_key = s.match_key
    left join af   a on a.match_key = s.match_key
    left join ol   l on l.match_key = s.match_key
    left join odds o on o.match_key = s.match_key
),

with_wx as (
    select
        b.*,
        t.openmeteo_venue_key,
        date_trunc('hour', timezone(z.tz, timezone('UTC', b.kickoff_utc))) as kickoff_local_hour
    from base b
    left join {{ ref('dim_team') }} t on t.team_key = b.home_key
    left join venue_tz z on z.venue_name = t.openmeteo_venue_key
)

select
    w.match_key,
    w.competition_key,
    {{ season_of('w.match_date') }}                                       as season,
    w.match_date,
    w.kickoff_utc,
    w.home_key, w.away_key,
    w.fdo_match_id, w.understat_match_id, w.af_fixture_id, w.openliga_match_id,
    w.status, w.stage, w.matchday, w.af_round, w.referee, w.venue_name,
    w.is_finished,
    case when w.is_finished then w.home_goals_any end                     as home_goals,
    case when w.is_finished then w.away_goals_any end                     as away_goals,
    case when w.is_finished and w.home_goals_any is not null then
        case when w.home_goals_any > w.away_goals_any then 'H'
             when w.home_goals_any = w.away_goals_any then 'D' else 'A' end
    end                                                                   as result,
    case when w.is_finished then w.home_goals_any + w.away_goals_any end  as total_goals,
    w.ht_home_goals, w.ht_away_goals,
    w.n_distinct_scores > 1                                               as goals_conflict,
    w.home_shots, w.away_shots, w.home_shots_target, w.away_shots_target,
    w.home_corners, w.away_corners, w.home_fouls, w.away_fouls,
    w.home_yellows, w.away_yellows, w.home_reds, w.away_reds,
    w.b365_home, w.b365_draw, w.b365_away,
    w.mkt_avg_home, w.mkt_avg_draw, w.mkt_avg_away, w.mkt_max_home, w.mkt_max_draw, w.mkt_max_away,
    w.odds_api_home, w.odds_api_draw, w.odds_api_away, w.odds_api_n_bookmakers, w.odds_api_updated_at,
    w.home_xg, w.away_xg, w.forecast_home_win, w.forecast_draw, w.forecast_away_win,
    x.temperature_c   as weather_temp_c,
    x.precipitation_mm as weather_precip_mm,
    x.windspeed_kmh   as weather_wind_kmh,
    x.humidity_pct    as weather_humidity_pct,
    x.weather_code    as weather_code,
    w.in_fdo, w.in_couk, w.in_understat, w.in_api_football, w.in_openliga, w.in_odds_api,
    (x.venue_name is not null)                                            as has_weather
from with_wx w
left join wx x on x.venue_name = w.openmeteo_venue_key and x.weather_time = w.kickoff_local_hour
