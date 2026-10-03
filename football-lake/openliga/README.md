# openliga — Football Source DB (mirror OpenLigaDB, mùa 2024 → nay)

```
OpenLigaDB API ──poll──▶ openliga.sync ──INSERT/UPDATE/DELETE──▶ Postgres football_source ──WAL──▶ Debezium ──▶ Kafka
```

## Chạy nhanh
```bash
# 1. Hạ tầng (postgres-cdc đã bật wal_level=logical, max_wal_senders, max_replication_slots)
docker compose -f docker-compose-debezium.yml up -d postgres-cdc

# 2. Tạo DB + schema + publication (idempotent). Từ máy host: Postgres ở localhost:5433
python -m openliga.cli init

# 3. Backfill 2024 -> nay (resume được; ~470 request ≈ 10 phút do giới hạn 60 req/phút)
python -m openliga.cli backfill                # tất cả giải
python -m openliga.cli backfill --only bl1     # thử nhanh Bundesliga trước
python -m openliga.cli stats
python -m openliga.cli verify bl1 2026         # đối chiếu số trận/bàn thắng + bảng xếp hạng với API

# 4. Chạy worker đồng bộ liên tục
python -m openliga.cli sync                    # hoặc: docker compose ... --profile sync up -d openliga-sync

# 5. CHỈ SAU KHI backfill xong và verify OK: bật Kafka/Debezium rồi đăng ký connector
docker compose -f docker-compose-debezium.yml up -d
python -m openliga.cli register                # topic: football.public.<bảng>
```
Biến môi trường chính: `FOOTBALL_PG_HOST/PORT/USER/PASSWORD/DB`, `OPENLIGA_START_SEASON` (mặc định 2024),
`OPENLIGA_END_SEASON`, `OPENLIGA_MAX_REQ_PER_MIN` (mặc định 50), `OPENLIGA_RAW_LOG=1` (lưu payload thô vào `raw_api_log`).

## Dữ liệu API → bảng
| Endpoint | Bảng |
|---|---|
| `getavailablesports` | `sports` |
| `getresulttypes` | `result_types` |
| `getavailableleagues/{season}` | `leagues` (mỗi mùa 2024..nay) |
| `getresultinfos/{leagueId}` | `league_result_infos` |
| `getmatchdata/{shortcut}/{season}` (1 request/giải-mùa) | `matches`, `match_results`, `goals`, `teams`, `locations`, `groups` |
| `getlastchangedate`, `getmatchdata/.../{group}` | dùng cho sync incremental, trạng thái ở `sync_state` |

**Không mirror** (suy ra từ matches, nên làm ở Silver/Gold): `getgoalgetters`, `getavailableteams`,
`getavailablegroups`, `getbltable`, `getgrouptable` (đang 404). Có sẵn view `v_match_scores`, `v_league_table`
để đối chiếu với `getbltable`.

## Đảm bảo cho WAL/CDC
- PK = ID của API → upsert idempotent, Debezium có key ổn định.
- Upsert `... WHERE (cũ) IS DISTINCT FROM (mới)`: dữ liệu không đổi thì **0 UPDATE → WAL im lặng** (có test).
  `lastUpdateDateTime` được lưu nhưng không dùng để so sánh, vì chỉ API "chạm" timestamp cũng không sinh event.
- Mỗi payload (một giải-mùa / một spieltag) = **một transaction**: match + results + goals commit cùng lúc.
- API không có tombstone → với mỗi trận, goal/result không còn trong payload bị `DELETE` (đây là nguồn event `op=d`).
  Trận biến mất chỉ bị xoá khi reconcile cả mùa và payload không rỗng.
- `REPLICA IDENTITY FULL` cho `matches`, `match_results`, `goals`; publication `football_pub` không chứa `sync_state`/`raw_api_log`.
- Không tạo replication slot thủ công: Debezium tự tạo `football_slot` (slot mồ côi sẽ giữ WAL đầy đĩa).

## Chiến lược poll (tôn trọng 60 req/phút/IP)
Mỗi 30s worker lập kế hoạch theo thứ tự ưu tiên, tối đa 40 task/tick:
1. **Nhóm live** (có trận kickoff trong [-3h, +10p]): `getlastchangedate` mỗi 60s.
2. **Nhóm hot** (trận trong [-3 ngày, +2 ngày] hoặc chưa kết thúc trong 14 ngày): mỗi 5 phút.
3. **Reconcile** mỗi 24h: tải lại cả giải-mùa (1 request/giải) + refresh danh sách league để bắt giải mới.
Chỉ khi `getlastchangedate` khác giá trị đã lưu mới tải dữ liệu nhóm. Retry/backoff khi 429/5xx, tôn trọng `Retry-After`.
Các mùa đã kết thúc không bị poll lại (`OPENLIGA_LIVE_SEASONS`, mặc định 2 mùa gần nhất).

## Test
```bash
TEST_PG_DSN="host=localhost port=5433 user=postgres password=postgres dbname=postgres" pytest openliga/tests -q
```
Test chạy trên Postgres thật, gồm kiểm tra WAL bằng `test_decoding` (cần `wal_level=logical`).
