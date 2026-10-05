{# ===== Helper cho nguồn CDC (p26 OpenLigaDB: Debezium -> Kafka -> raw/openliga/<bảng>/...) =====
   Raw là NHẬT KÝ SỰ KIỆN (append-only): mỗi file = {"table":..,"events":[{op,before,after,lsn,kafka_offset,..}]}.
   cdc_current dựng lại TRẠNG THÁI HIỆN TẠI của bảng: lấy sự kiện mới nhất của từng khóa rồi bỏ khóa đã bị xoá (op='d'). #}

{# Timestamp của Debezium: TIMESTAMPTZ -> chuỗi ISO ("...Z"); TIMESTAMP -> số micro-giây kể từ epoch.
   Hàm nhận cả hai dạng, trả TIMESTAMP (UTC với timestamptz, giờ địa phương với timestamp). #}
{% macro cdc_ts(col, path) -%}
coalesce(
    try_cast(regexp_replace(json_extract_string({{ col }}, '{{ path }}'), '(Z|[+-]00:00)$', '') as timestamp),
    make_timestamp(try_cast(json_extract_string({{ col }}, '{{ path }}') as bigint))
)
{%- endmacro %}

{# Trả về 1 subquery (cần đặt alias khi dùng) với các cột: a (JSON 'after'), event_ts, lsn, kafka_offset.
   Thứ tự sự kiện cùng khóa: lsn (WAL) rồi tới offset Kafka. Thiếu file raw => 0 dòng, không vỡ build. #}
{% macro cdc_current(table, pk) -%}
(
    with raw as (
        select filename, "json" as doc
        from {{ lake_objects('raw/openliga/' ~ table ~ '/**/*.json.gz') }}
    ),
    ev as (
        select filename, {{ jarray('doc', '$.events') }} as e from raw
    ),
    ranked as (
        select
            json_extract(e, '$.after')                                        as a,
            json_extract_string(e, '$.op')                                    as op,
            epoch_ms(try_cast(json_extract_string(e, '$.ts_ms') as bigint))   as event_ts,
            try_cast(json_extract_string(e, '$.lsn') as bigint)               as lsn,
            try_cast(json_extract_string(e, '$.kafka_offset') as bigint)      as kafka_offset,
            filename,
            coalesce(json_extract_string(e, '$.after.{{ pk }}'),
                     json_extract_string(e, '$.before.{{ pk }}'))             as pk_val
        from ev
        qualify row_number() over (
            partition by pk_val
            order by lsn desc nulls last, kafka_offset desc nulls last, filename desc) = 1
    )
    select a, event_ts, lsn, kafka_offset from ranked where op <> 'd' and pk_val is not null
)
{%- endmacro %}
