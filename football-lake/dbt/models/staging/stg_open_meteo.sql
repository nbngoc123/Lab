{{ config(materialized='table') }}
{# Bảng vì: ~50k giờ x số sân x số ngày ingest. Dedup giữ bản ingest mới nhất cho mỗi (sân, giờ). #}

with raw as (
    select filename, "json" as doc
    from {{ lake_objects('raw/open_meteo/historical/**/*.json.gz', "maximum_object_size=67108864") }}
),

hourly as (
    select
        {{ path_part('venue') }}                         as venue_name,
        {{ path_date() }}                                as ingest_date,
        {{ jget('doc', '$.latitude', 'double') }}        as latitude,
        {{ jget('doc', '$.longitude', 'double') }}       as longitude,
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
    venue_name, ingest_date, latitude, longitude, timezone,
    try_cast(weather_time_raw as timestamp) as weather_time,   -- giờ địa phương của sân
    temperature_c, precipitation_mm, windspeed_kmh, humidity_pct, weather_code
from hourly
where try_cast(weather_time_raw as timestamp) is not null
qualify row_number() over (
    partition by venue_name, try_cast(weather_time_raw as timestamp)
    order by ingest_date desc nulls last) = 1
