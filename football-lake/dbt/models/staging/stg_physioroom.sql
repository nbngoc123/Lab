{# tables_raw.json.gz = mảng gồm 4 bảng (theo thứ tự p16.parse_tables):
   0 tổng chấn thương/CLB | 1 danh sách cầu thủ | 2 loại chấn thương/CLB | 3 tần suất loại chấn thương.
   Chưa biết tên cột thật của từng bảng nên trả dạng long (row_json). Khi có mẫu, viết model typed cho từng table_name.
   LƯU Ý: nếu file chứa NaN (pandas.to_dict) thì JSON không hợp lệ -> sửa ở ingest (xem CHANGES.md).
   Giữ mọi snapshot (mỗi ngày là 1 ảnh chụp bảng chấn thương). #}

with raw as (
    select filename, "json" as doc
    from read_json_objects({{ lake_path('raw/physioroom/injury_table/**/tables_raw.json.gz') }},
                           format='unstructured', filename=true)
),

idx as (
    select filename, doc, unnest(range(0, json_array_length(doc)::int)) as table_idx
    from raw
),

tbls as (
    select filename, table_idx, json_extract(doc, '$[' || table_idx || ']') as tbl
    from idx
),

rws as (
    select filename, table_idx, unnest(from_json(tbl, '["JSON"]')) as row_json
    from tbls
)

select
    {{ path_date() }} as snapshot_date,
    table_idx,
    case table_idx
        when 0 then 'club_injury_totals'
        when 1 then 'player_injuries'
        when 2 then 'injury_types_by_club'
        when 3 then 'injury_type_frequency'
    end as table_name,
    row_json
from rws
