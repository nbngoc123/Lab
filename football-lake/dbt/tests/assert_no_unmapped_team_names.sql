{{ config(severity='warn') }}
{# CẢNH BÁO (không làm fail build): còn tên đội chưa có trong seed/team_alias.csv.
   Mở model audit_unmapped_team_names để xem danh sách rồi bổ sung seed. #}
select * from {{ ref('audit_unmapped_team_names') }}
