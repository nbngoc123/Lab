"""Ingest p26 - OpenLigaDB CDC: Kafka (Debezium) -> MinIO Bronze.

Luồng:  Postgres football_source --WAL--> Debezium --> Kafka (football.public.<bảng>)
                                                          |
                                         (module này)  <--+
                                                          v
                          s3://football-lake/raw/openliga/<bảng>/ingest_date=YYYY-MM-DD/*.json.gz

Bronze là NHẬT KÝ SỰ KIỆN append-only (kể cả op='d'): dbt dựng lại trạng thái hiện tại ở staging
(macro cdc_current). Đảm bảo at-least-once: chỉ commit offset Kafka SAU KHI upload MinIO thành công;
nếu crash giữa chừng thì sự kiện được đọc lại và dbt khử trùng theo (khóa, lsn, offset).
"""
import base64
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

SRC = "openliga-cdc"
TOPIC_PREFIX = os.getenv("OPENLIGA_TOPIC_PREFIX", "football.public.")
TABLES = ["sports", "result_types", "leagues", "league_result_infos", "teams", "locations",
          "groups", "matches", "match_results", "goals"]
BOOTSTRAP = os.getenv("OPENLIGA_KAFKA_BOOTSTRAP", "kafka-cdc:29092")
GROUP_ID = os.getenv("OPENLIGA_KAFKA_GROUP", "football_lake_openliga_ingest")

ROUND_MAX_EVENTS = int(os.getenv("OPENLIGA_INGEST_ROUND_EVENTS", "50000"))   # trần RAM / 1 vòng
MAX_SECONDS = int(os.getenv("OPENLIGA_INGEST_MAX_SECONDS", "600"))           # trần thời gian / 1 task
FIRST_IDLE = float(os.getenv("OPENLIGA_INGEST_FIRST_IDLE", "30"))            # chờ rebalance lần đầu
IDLE = float(os.getenv("OPENLIGA_INGEST_IDLE", "10"))                        # hết tin => coi như bắt kịp


def topics():
    return [TOPIC_PREFIX + t for t in TABLES]


def table_of(topic: str) -> str:
    return topic[len(TOPIC_PREFIX):] if topic.startswith(TOPIC_PREFIX) else topic


def parse_event(value, topic: str, partition: int, offset: int):
    """Debezium JSON -> dict phẳng. Trả None cho tombstone (value rỗng).
    Chịu được cả dạng có envelope {'schema','payload'} lẫn payload trần. Lỗi cú pháp => ValueError."""
    if not value:
        return None
    doc = json.loads(value.decode("utf-8"))
    p = doc.get("payload", doc) if isinstance(doc, dict) else None
    if not isinstance(p, dict) or "op" not in p:
        raise ValueError("không phải sự kiện Debezium (thiếu 'op')")
    src = p.get("source") or {}
    return {
        "op": p["op"],                      # c / u / d / r (snapshot)
        "before": p.get("before"),
        "after": p.get("after"),
        "ts_ms": p.get("ts_ms"),
        "lsn": src.get("lsn"),
        "tx_id": src.get("txId"),
        "snapshot": src.get("snapshot"),
        "kafka_partition": partition,
        "kafka_offset": offset,
    }


def default_writer(key: str, obj: dict, meta: dict):
    from lake.minio_io import put_json_gz        # import muộn: không cần MinIO khi test
    put_json_gz(key, obj, SRC, meta=meta)


def _flush(buffers, writer, ingest_date, stats):
    """Ghi mỗi (bảng, partition) một file; tên file gắn dải offset nên chạy lại ghi đè đúng file cũ."""
    for (table, part), events in buffers.items():
        if not events:
            continue
        first, last = events[0]["kafka_offset"], events[-1]["kafka_offset"]
        key = (f"raw/openliga/{table}/ingest_date={ingest_date}/"
               f"p{part}_{first:012d}-{last:012d}.json.gz")
        obj = {"table": table, "topic": TOPIC_PREFIX + table, "partition": part,
               "first_offset": first, "last_offset": last, "n_events": len(events),
               "ingested_at": datetime.now(timezone.utc).isoformat(), "events": events}
        writer(key, obj, {"table": table, "events": len(events), "first_offset": first, "last_offset": last})
        stats["files"] += 1
        stats["by_table"][table] += len(events)
    buffers.clear()


def consume_and_store(consumer, writer=default_writer, max_seconds=None, round_max=None,
                      first_idle=None, idle=None, clock=time.monotonic):
    """Đọc Kafka theo vòng; mỗi vòng: gom -> ghi MinIO -> commit offset. Trả về thống kê."""
    max_seconds = MAX_SECONDS if max_seconds is None else max_seconds
    round_max = ROUND_MAX_EVENTS if round_max is None else round_max
    first_idle = FIRST_IDLE if first_idle is None else first_idle
    idle = IDLE if idle is None else idle

    stats = {"events": 0, "files": 0, "bad": 0, "by_table": defaultdict(int)}
    ingest_date = datetime.now(timezone.utc).date().isoformat()
    t0 = clock()
    last_msg = t0
    got_any = False
    buffers, n_round, dead = defaultdict(list), 0, []

    def end_round():
        nonlocal n_round
        if dead:                                           # message hỏng: giữ lại để điều tra, không mất
            writer(f"raw/openliga/_dead_letter/ingest_date={ingest_date}/{int(time.time()*1000)}.json.gz",
                   {"n": len(dead), "messages": list(dead)}, {"dead_letters": len(dead)})
            dead.clear()
        _flush(buffers, writer, ingest_date, stats)
        if n_round:
            consumer.commit(asynchronous=False)            # chỉ commit SAU khi đã ghi MinIO
        n_round = 0

    try:
        while clock() - t0 < max_seconds:
            msg = consumer.poll(1.0)
            now = clock()
            if msg is None:
                if now - last_msg >= (idle if got_any else first_idle):
                    break
                continue
            if msg.error():
                from confluent_kafka import KafkaError
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue                               # topic chưa có (Debezium chưa snapshot)
                raise RuntimeError(f"Kafka lỗi: {msg.error()}")
            last_msg, got_any = now, True
            n_round += 1
            try:
                ev = parse_event(msg.value(), msg.topic(), msg.partition(), msg.offset())
            except (ValueError, UnicodeDecodeError) as e:
                stats["bad"] += 1
                raw = msg.value() or b""
                dead.append({"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                             "error": str(e), "value_b64": base64.b64encode(raw[:100_000]).decode()})
                continue
            if ev is None:                                 # tombstone: bỏ qua nhưng offset vẫn tiến
                continue
            buffers[(table_of(msg.topic()), msg.partition())].append(ev)
            stats["events"] += 1
            if n_round >= round_max:
                end_round()
        end_round()
    finally:
        consumer.close()
    stats["by_table"] = dict(stats["by_table"])
    return stats


def make_consumer():
    from confluent_kafka import Consumer
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",      # lần đầu: lấy cả snapshot Debezium
        "enable.auto.commit": False,          # commit thủ công sau khi ghi MinIO
        "enable.partition.eof": False,
        "session.timeout.ms": 30000,
        "max.poll.interval.ms": 600000,
    })
    c.subscribe(topics())
    return c


def ingest() -> dict:
    """Entry point cho Airflow."""
    stats = consume_and_store(make_consumer())
    print(f"[p26] {stats['events']} sự kiện, {stats['files']} file, {stats['bad']} lỗi parse -> {stats['by_table']}")
    return stats
