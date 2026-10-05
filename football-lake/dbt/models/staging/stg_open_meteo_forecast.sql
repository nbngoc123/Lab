{{ config(materialized='view') }}
{# Dự báo 7 ngày (p13 có ghi nhưng trước đây chưa có staging). Giữ bản dự báo mới nhất cho mỗi (sân, giờ). #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/open_meteo/forecast/**/*.json.gz') }}
),

hourly as (
    select
        {{ path_part('venue') }}                         as venue_name,
        {{ path_date() }}                                as forecast_issued_date,
        {{ jget('doc', '$.timezone') }}                  as timezone,
        unnest(from_json(json_extract(doc, '$.hourly.time'),                '["VARCHAR"]')) as weather_time_raw,
        unnest(from_json(json_extract(doc, '$.hourly.temperature_2m'),      '["DOUBLE"]'))  as temperature_c,
        unnest(from_json(json_extract(doc, '$.hourly.precipitation'),       '["DOUBLE"]'))  as precipitation_mm,
        unnest(from_json(json_extract(doc, '$.hourly.windspeed_10m'),       '["DOUBLE"]'))  as windspeed_kmh,
        unnest(from_json(json_extract(doc, '$.hourly.relative_humidity_2m'),'["DOUBLE"]'))  as humidity_pct,
        unnest(from_json(json_extract(doc, '$.hourly.weathercode'),         '["INTEGER"]')) as weather_code
    from raw
)

select
    venue_name, forecast_issued_date, timezone,
    try_cast(weather_time_raw as timestamp) as weather_time,
    temperature_c, precipitation_mm, windspeed_kmh, humidity_pct, weather_code
from hourly
where try_cast(weather_time_raw as timestamp) is not null
qualify row_number() over (
    partition by venue_name, try_cast(weather_time_raw as timestamp)
    order by forecast_issued_date desc nulls last) = 1
