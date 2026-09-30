{{ config(materialized='view') }}
{# Làm phẳng 1 lần: mỗi dòng = 1 outcome / 1 nhà cái / 1 market / 1 snapshot.
   GIỮ TOÀN BỘ snapshot (dùng cho phân tích biến động kèo). Các model h2h/spreads/totals lấy bản mới nhất. #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/odds/**/*.json.gz') }}, filename=true)
),

matches as (
    select
        filename,
        {{ jget('doc', '$.id') }}            as match_id,
        {{ jget('doc', '$.sport_key') }}     as sport_key,
        {{ jget('doc', '$.home_team') }}     as home_team,
        {{ jget('doc', '$.away_team') }}     as away_team,
        {{ jts('doc', '$.commence_time') }}  as commence_time,
        {{ jarray('doc', '$.bookmakers') }}  as bm
    from raw
    where json_extract_string(doc, '$.id') is not null   -- loại body lỗi {"message": ...}
),

markets as (
    select
        filename, match_id, sport_key, home_team, away_team, commence_time,
        {{ jget('bm', '$.key') }}          as bookmaker_key,
        {{ jget('bm', '$.title') }}        as bookmaker,
        {{ jts('bm', '$.last_update') }}   as bookmaker_last_update,
        {{ jarray('bm', '$.markets') }}    as mk
    from matches
),

outcomes as (
    select
        filename, match_id, sport_key, home_team, away_team, commence_time,
        bookmaker_key, bookmaker, bookmaker_last_update,
        {{ jget('mk', '$.key') }}          as market_key,
        {{ jts('mk', '$.last_update') }}   as market_last_update,
        {{ jarray('mk', '$.outcomes') }}   as oc
    from markets
)

select
    match_id, sport_key, home_team, away_team, commence_time,
    bookmaker_key, bookmaker,
    coalesce(market_last_update, bookmaker_last_update) as last_update,
    market_key,
    {{ jget('oc', '$.name') }}                 as outcome_name,
    {{ jget('oc', '$.point', 'double') }}      as point,
    {{ jget('oc', '$.price', 'double') }}      as price,
    {{ path_date() }}                          as snapshot_date
from outcomes
