# 11 — Generator sự kiện trận đấu giả lập (Streaming thật, tự tạo) → MinIO

**Kiểu ingest:** Dữ liệu **tự sinh**, mô phỏng streaming thời gian thực
**Tần suất:** liên tục, mỗi vài giây trong "thời gian trận đấu" giả lập
**Độ khó:** ★★★☆☆ — không khó về code, khó ở **thiết kế đúng mô hình streaming**

---

## 1. Vì sao cần nguồn này

Tất cả 10 nguồn trước là **batch** — dù có nguồn "sống" như file 09/10, chúng vẫn là request-rồi-nhận-toàn-bộ-kết-quả. Không nguồn miễn phí nào cho bạn **luồng sự kiện thật sự đến từng phần nhỏ theo thời gian**, vì API-Football free tier (file 02) quá hiếm quota để poll liên tục.

Giải pháp: **tự viết generator** phát ra sự kiện trận đấu giả lập (bàn thắng, thẻ, thay người, sút bóng...) theo nhịp thời gian thực, ghi từng batch nhỏ vào MinIO. Đây không phải "giả dữ liệu cho vui" — nó mô phỏng chính xác **pattern kiến trúc** mà một hệ thống ingest streaming thật (Kafka → Lake) phải xử lý: file nhỏ, đến liên tục, cần compact định kỳ.

Giá trị dạy học lớn nhất: bạn thực hành được vấn đề kinh điển của streaming-to-lake là **"small file problem"** và cách giải quyết bằng compaction job.

## 2. Thiết kế mô phỏng

- Một trận giả lập chạy trong **90 "phút ảo"**, mỗi phút ảo = 2 giây thực (cả trận ~3 phút thực tế) — đủ nhanh để demo, đủ chậm để thấy rõ luồng.
- Xác suất sự kiện dựa trên tần suất thật của bóng đá: ~2.7 bàn/trận, ~25 sút/trận, ~3.5 thẻ vàng/trận.
- Mỗi sự kiện ghi thành **1 object nhỏ** trong MinIO ngay khi phát sinh — mô phỏng đúng hành vi consumer đọc từ Kafka topic và ghi micro-batch.

## 3. Layout trong lake

```
bronze/live_sim/events/match_id=SIM001/minute=07/evt_a91f2c.json
bronze/live_sim/events/match_id=SIM001/minute=07/evt_b04d31.json
bronze/live_sim/events/match_id=SIM001/minute=23/evt_c7712a.json
...
bronze/live_sim/checkpoints/match_id=SIM001/state.json      ← trạng thái trận đang chạy

silver/events/sim_match_events/match_id=SIM001/part-0.parquet   ← sau khi compact
gold/live/sim_match_score/match_id=SIM001/current.json           ← điểm số real-time
```

**Nguyên tắc quan trọng:** bronze ở đây có hàng trăm file nhỏ theo thiết kế (đúng bản chất streaming). Việc gộp chúng lại thành ít file Parquet lớn ở silver là **compaction job** — chạy riêng, không phải lúc ingest.

## 4. Script sinh sự kiện — `pipelines/p11_live_simulator.py`

```python
"""
Generator streaming giả lập: phát sự kiện trận đấu theo thời gian thực vào MinIO.
Mô phỏng đúng pattern Kafka-consumer-ghi-lake, không cần hạ tầng Kafka thật.
"""
import json
import random
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

from lake.minio_io import S3, BUCKET, summary

SRC = "live-match-simulator"
SECONDS_PER_MINUTE = 2.0        # tốc độ giả lập: 1 phút bóng đá = 2 giây thực

PLAYERS_HOME = ["Saka", "Odegaard", "Rice", "Havertz", "Trossard", "Gabriel"]
PLAYERS_AWAY = ["Salah", "Nunez", "Szoboszlai", "Mac Allister", "Diaz", "Van Dijk"]

EVENT_WEIGHTS = {          # xác suất tương đối mỗi phút
    "Shot": 0.28, "Shot on Target": 0.12, "Corner": 0.10,
    "Foul": 0.14, "Yellow Card": 0.04, "Substitution": 0.02,
    "Goal": 0.03, "Offside": 0.05, "Red Card": 0.002,
}


@dataclass
class MatchState:
    match_id: str
    home_team: str
    away_team: str
    minute: int = 0
    home_score: int = 0
    away_score: int = 0
    status: str = "SCHEDULED"


def put_event(match_id: str, minute: int, event: dict):
    """Ghi 1 sự kiện = 1 object nhỏ, ngay khi nó xảy ra. Đây chính là điểm mô phỏng streaming."""
    eid = uuid.uuid4().hex[:8]
    key = f"bronze/live_sim/events/match_id={match_id}/minute={minute:02d}/evt_{eid}.json"
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=key, Body=body,
                  ContentType="application/json",
                  Metadata={"source": SRC, "match_id": match_id,
                           "minute": str(minute)})
    print(f"    [{minute:02d}'] {event['type']:<16} {event.get('team','')} "
          f"{event.get('player','')}")


def save_checkpoint(state: MatchState):
    S3.put_object(
        Bucket=BUCKET,
        Key=f"bronze/live_sim/checkpoints/match_id={state.match_id}/state.json",
        Body=json.dumps(asdict(state)).encode(),
        ContentType="application/json")


def write_gold_score(state: MatchState):
    """Bảng gold cập nhật liên tục — nơi dashboard/app đọc điểm số real-time."""
    S3.put_object(
        Bucket=BUCKET,
        Key=f"gold/live/sim_match_score/match_id={state.match_id}/current.json",
        Body=json.dumps({
            "match_id": state.match_id,
            "score": f"{state.home_team} {state.home_score} - "
                     f"{state.away_score} {state.away_team}",
            "minute": state.minute, "status": state.status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).encode(),
        ContentType="application/json")


def maybe_generate_events(state: MatchState):
    for etype, prob in EVENT_WEIGHTS.items():
        if random.random() > prob:
            continue
        team, players = random.choice(
            [(state.home_team, PLAYERS_HOME), (state.away_team, PLAYERS_AWAY)])
        player = random.choice(players)

        event = {
            "match_id": state.match_id, "minute": state.minute,
            "type": etype, "team": team, "player": player,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if etype == "Substitution":
            event["player_out"] = random.choice(players)
            event["player_in"] = f"Sub_{random.randint(1,9)}"
        if etype == "Goal":
            if team == state.home_team:
                state.home_score += 1
            else:
                state.away_score += 1
            event["home_score"] = state.home_score
            event["away_score"] = state.away_score

        put_event(state.match_id, state.minute, event)


def run_match(match_id: str, home: str, away: str):
    state = MatchState(match_id=match_id, home_team=home, away_team=away,
                       status="LIVE")
    print(f"\n▶ Kick-off: {home} vs {away}  (match_id={match_id})")
    save_checkpoint(state)

    for minute in range(1, 91):
        state.minute = minute
        maybe_generate_events(state)
        write_gold_score(state)
        save_checkpoint(state)
        time.sleep(SECONDS_PER_MINUTE)

        if minute == 45:
            print("    -- Hết hiệp 1, nghỉ giải lao (mô phỏng: chờ 3s) --")
            time.sleep(3)

    state.status = "FINISHED"
    save_checkpoint(state)
    write_gold_score(state)
    print(f"⏹ Kết thúc: {home} {state.home_score} - {state.away_score} {away}")
    return state


if __name__ == "__main__":
    run_match("SIM001", "Arsenal", "Liverpool")
    summary("bronze/live_sim/")
    summary("gold/live/")
```

Chạy:

```bash
python -m pipelines.p11_live_simulator
```

Trận chạy khoảng **3-4 phút thực tế** (90 phút ảo × 2s + nghỉ giữa hiệp), phát ra thường **60-100 sự kiện** rải thành 60-100 object riêng biệt trong MinIO.

## 5. Compaction job — `pipelines/p11b_compact.py`

Đây là phần bổ sung bắt buộc phải có khi làm streaming: gộp hàng trăm file JSON nhỏ thành 1 Parquet lớn ở silver.

```python
"""Compaction: gộp file nhỏ streaming thành Parquet lớn ở silver."""
import json
import pandas as pd
from lake.minio_io import list_keys, read_bytes, put_parquet, S3, BUCKET

SRC = "live-match-simulator"


def compact_match(match_id: str):
    prefix = f"bronze/live_sim/events/match_id={match_id}/"
    rows = []
    small_files = []
    for key, size in list_keys(prefix):
        small_files.append(key)
        rows.append(json.loads(read_bytes(key)))

    if not rows:
        print(f"  ! không có event nào cho {match_id}")
        return

    df = pd.DataFrame(rows).sort_values("minute")
    put_parquet(f"silver/events/sim_match_events/match_id={match_id}/part-0.parquet",
                df, SRC, meta={"match_id": match_id, "events": len(df)})

    print(f"  ✓ compact {match_id}: {len(small_files)} file nhỏ "
          f"-> 1 Parquet ({len(df)} dòng)")

    # tùy chọn: dọn file nhỏ sau khi đã compact an toàn
    # for key in small_files:
    #     S3.delete_object(Bucket=BUCKET, Key=key)
    # print(f"  ✓ đã dọn {len(small_files)} file nhỏ")


if __name__ == "__main__":
    compact_match("SIM001")
```

**Cố ý comment sẵn phần xóa file nhỏ** — trong lake thật bạn thường giữ bronze để có thể replay, chỉ xóa khi chắc chắn đã archive nơi khác.

## 6. Kết quả mong đợi

```
▶ Kick-off: Arsenal vs Liverpool  (match_id=SIM001)
    [01'] Foul             Arsenal Rice
    [03'] Shot             Liverpool Salah
    [07'] Corner           Arsenal Saka
    [12'] Yellow Card      Liverpool Van Dijk
    [23'] Goal             Arsenal Trossard
    -- Hết hiệp 1, nghỉ giải lao (mô phỏng: chờ 3s) --
    [51'] Shot on Target   Liverpool Nunez
    [67'] Goal             Liverpool Salah
    [78'] Substitution     Arsenal Havertz
    ...
⏹ Kết thúc: Arsenal 2 - 1 Liverpool

[summary] bronze/live_sim/: 87 objects, 0.14 MB
[summary] gold/live/: 1 objects, 0.31 KB
```

Sau compaction:

```
  ✓ compact SIM001: 86 file nhỏ -> 1 Parquet (86 dòng)

[summary] silver/events/sim_match_events/: 1 objects, 8.2 KB
```

87 object nhỏ (≈1.6 KB mỗi cái) gộp thành 1 file 8.2 KB — minh chứng trực quan cho lý do vì sao streaming-to-lake luôn cần compaction: đọc 1 file Parquet nhanh hơn rất nhiều so với mở 87 kết nối S3 riêng lẻ.

## 7. Truy vấn kiểm chứng

```sql
-- Đọc điểm số real-time (trong lúc trận đang "chạy")
SELECT * FROM read_json_auto(
  's3://football-lake/gold/live/sim_match_score/match_id=SIM001/current.json');

-- Sau compaction: timeline đầy đủ trận đấu
SELECT minute, type, team, player
FROM read_parquet('s3://football-lake/silver/events/sim_match_events/match_id=SIM001/*.parquet')
ORDER BY minute;

-- Thống kê: loại sự kiện nào nhiều nhất?
SELECT type, COUNT(*) AS so_lan
FROM read_parquet('s3://football-lake/silver/events/sim_match_events/**/*.parquet')
GROUP BY type ORDER BY so_lan DESC;
```

## 8. Chạy nhiều trận song song (mô phỏng nhiều "producer")

```python
import concurrent.futures as cf
from pipelines.p11_live_simulator import run_match

matches = [
    ("SIM001", "Arsenal", "Liverpool"),
    ("SIM002", "Manchester City", "Chelsea"),
    ("SIM003", "Tottenham", "Manchester United"),
]
with cf.ThreadPoolExecutor(max_workers=3) as ex:
    list(ex.map(lambda m: run_match(*m), matches))
```

Mỗi trận ghi vào `match_id` riêng nên không đụng nhau — đúng mô hình nhiều partition Kafka chạy song song đổ vào cùng 1 lake.

## 9. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Quá nhiều request PUT trong thời gian ngắn | MinIO local thường chịu được tốt, nhưng S3 thật có thể throttle | Gộp vài sự kiện cùng phút thành 1 object thay vì 1-sự-kiện-1-object nếu cần tối ưu |
| Compaction đọc thiếu event | Job compact chạy khi trận chưa kết thúc | Chỉ compact sau khi checkpoint có `status: FINISHED` |
| Điểm số ở gold không khớp event ở bronze | Race condition khi ghi đồng thời | Với demo 1-luồng không xảy ra; multi-producer cần thêm lock hoặc dùng append-log thay vì overwrite |

## 10. Mở rộng

- Thêm consumer riêng đọc `gold/live/sim_match_score/` mỗi giây và hiển thị lên terminal/dashboard — mô phỏng đầy đủ pipeline producer → lake → consumer.
- Nối `sim_match_events` với `fdo_players` (file 09) qua tên cầu thủ để có "trận giả lập nhưng cầu thủ thật".
- Thay tốc độ `SECONDS_PER_MINUTE` bằng biến điều khiển được qua CLI argument để demo nhanh (0.1s/phút) hay thật (60s/phút) tùy nhu cầu trình bày.
- Đây là nơi tự nhiên nhất để giới thiệu Kafka thật nếu muốn nâng cấp: thay `put_event` bằng `producer.send(topic, event)`, dùng Kafka Connect S3 Sink để tự động đổ vào MinIO.
