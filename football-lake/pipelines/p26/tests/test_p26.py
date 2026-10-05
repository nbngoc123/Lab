"""pytest pipelines/p26/tests -q   (không cần Kafka/MinIO: dùng consumer + writer giả)"""
import gzip
import json

from pipelines.p26 import main as p26


class Msg:
    def __init__(self, topic, off, value, part=0, err=None):
        self._t, self._o, self._v, self._p, self._e = topic, off, value, part, err
    def topic(self): return self._t
    def offset(self): return self._o
    def partition(self): return self._p
    def value(self): return self._v
    def error(self): return self._e


class FakeConsumer:
    def __init__(self, msgs): self.msgs, self.commits, self.closed = list(msgs), 0, False
    def poll(self, t): return self.msgs.pop(0) if self.msgs else None
    def commit(self, asynchronous=False): self.commits += 1
    def close(self): self.closed = True


def ev(op, after=None, before=None, lsn=100, envelope=True):
    p = {"op": op, "before": before, "after": after, "ts_ms": 1, "source": {"lsn": lsn, "txId": 7, "snapshot": "false"}}
    return json.dumps({"schema": {}, "payload": p} if envelope else p).encode()


class Clock:                      # đồng hồ giả: mỗi lần gọi tăng 1s => poll rỗng nhanh chóng 'hết idle'
    def __init__(self): self.t = 0
    def __call__(self): self.t += 1; return self.t


def run(msgs, **kw):
    out = []
    c = FakeConsumer(msgs)
    st = p26.consume_and_store(c, writer=lambda k, o, m: out.append((k, o, m)), clock=Clock(),
                               first_idle=3, idle=3, max_seconds=10_000, **kw)
    return st, out, c


def test_parse_both_envelopes_and_tombstone():
    assert p26.parse_event(ev("c", {"match_id": 1}), "t", 0, 5)["after"] == {"match_id": 1}
    assert p26.parse_event(ev("d", before={"goal_id": 9}, envelope=False), "t", 0, 6)["op"] == "d"
    assert p26.parse_event(None, "t", 0, 7) is None
    assert p26.parse_event(ev("u", {"a": 1}, lsn=55), "t", 2, 8)["lsn"] == 55


def test_writes_one_file_per_table_with_offset_range_and_commits_after():
    T = p26.TOPIC_PREFIX
    msgs = [Msg(T + "matches", 0, ev("r", {"match_id": 1})), Msg(T + "matches", 1, ev("u", {"match_id": 1})),
            Msg(T + "goals", 0, ev("c", {"goal_id": 5, "match_id": 1})), Msg(T + "goals", 1, None)]  # tombstone
    st, out, c = run(msgs)
    assert st["events"] == 3 and st["files"] == 2 and st["by_table"] == {"matches": 2, "goals": 1}
    keys = sorted(k for k, _, _ in out)
    assert keys[0].startswith("raw/openliga/goals/ingest_date=") and keys[0].endswith("p0_000000000000-000000000000.json.gz")
    assert keys[1].endswith("p0_000000000000-000000000001.json.gz")
    assert c.commits == 1 and c.closed


def test_round_limit_flushes_and_commits_each_round():
    T = p26.TOPIC_PREFIX
    msgs = [Msg(T + "goals", i, ev("c", {"goal_id": i})) for i in range(5)]
    st, out, c = run(msgs, round_max=2)
    assert st["events"] == 5 and st["files"] == 3 and c.commits == 3


def test_bad_message_goes_to_dead_letter_and_does_not_block():
    T = p26.TOPIC_PREFIX
    st, out, c = run([Msg(T + "teams", 0, b"{khong phai json"), Msg(T + "teams", 1, ev("c", {"team_id": 3}))])
    assert st["bad"] == 1 and st["events"] == 1
    assert any("_dead_letter" in k for k, _, _ in out) and c.commits == 1


def test_no_messages_means_no_files_and_no_commit():
    st, out, c = run([])
    assert st["events"] == 0 and out == [] and c.commits == 0
