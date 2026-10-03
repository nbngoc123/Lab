"""Chạy:  TEST_PG_DSN="host=... user=postgres password=... dbname=postgres" pytest -q
Cần Postgres có wal_level=logical (để test 'WAL im lặng'); nếu không sẽ skip test đó."""
import copy
import os
import pytest
import psycopg2

from openliga import config, db, loader, sync
from openliga.transform import parse_dt
from .fixtures import FakeClient, match_future, season_payload

DSN = os.getenv("TEST_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="cần TEST_PG_DSN")
TABLES = ["goals", "match_results", "matches", "groups", "locations", "teams",
          "league_result_infos", "leagues", "result_types", "sports", "sync_state", "raw_api_log"]


@pytest.fixture()
def conn(monkeypatch):
    admin = psycopg2.connect(DSN); admin.autocommit = True
    with admin.cursor() as c:
        c.execute("DROP DATABASE IF EXISTS football_test"); c.execute("CREATE DATABASE football_test")
    admin.close()
    kv = dict(p.split("=", 1) for p in DSN.split())
    monkeypatch.setattr(config, "PG_HOST", kv.get("host", "localhost"))
    monkeypatch.setattr(config, "PG_PORT", int(kv.get("port", 5432)))
    monkeypatch.setattr(config, "PG_USER", kv["user"])
    monkeypatch.setattr(config, "PG_PASSWORD", kv.get("password", ""))
    monkeypatch.setattr(config, "PG_DB", "football_test")
    monkeypatch.setattr(config, "SEASONS", [2026]); monkeypatch.setattr(config, "LIVE_SEASONS", [2026])
    c = db.connect(); db.apply_schema(c)
    yield c
    c.close()


def count(conn, t):
    with conn.cursor() as c:
        c.execute(f"SELECT count(*) FROM {t}"); return c.fetchone()[0]


def full_backfill(conn, client=None):
    return sync.backfill(conn, client or FakeClient(), seasons=[2026])


def test_parse_dt():
    assert parse_dt("2026-09-10T17:23:11.1").microsecond == 100000
    assert parse_dt("2026-08-28T18:30:00Z").utcoffset().total_seconds() == 0
    assert parse_dt(None) is None and parse_dt("rác") is None


def test_backfill_rows_and_idempotency(conn):
    stats, fails = full_backfill(conn)
    assert not fails
    assert (count(conn, "matches"), count(conn, "goals"), count(conn, "match_results")) == (3, 8, 4)
    assert count(conn, "leagues") == 2 and count(conn, "teams") == 5 and count(conn, "groups") == 2
    assert count(conn, "locations") == 1 and count(conn, "league_result_infos") == 1
    # chạy lại với force: KHÔNG được có bất kỳ insert/update/delete nào
    stats2 = sync.backfill(conn, FakeClient(), seasons=[2026], force=True)[0]
    assert stats2.total_changes() == 0, str(stats2)


def test_goal_removed_by_var_is_deleted(conn):
    full_backfill(conn)
    cl = FakeClient(); cl.season[0]["goals"].pop(3)               # bàn phản lưới bị huỷ
    st = db.Stats(); loader.load_matches(conn, cl.season, st); conn.commit()
    assert st.d["goals"]["del"] == 1 and st.total_changes() == 1
    assert count(conn, "goals") == 7


def test_score_change_updates_only_result(conn):
    full_backfill(conn)
    cl = FakeClient(); cl.season[0]["matchResults"][1]["pointsTeam1"] = 6
    cl.season[0]["lastUpdateDateTime"] = "2026-09-03T10:00:00.5"  # api_updated_at đổi nhưng không tính
    st = db.Stats(); loader.load_matches(conn, cl.season, st); conn.commit()
    assert st.d["match_results"]["upd"] == 1 and "matches" not in st.d


def test_only_api_timestamp_change_is_noise_free(conn):
    full_backfill(conn)
    cl = FakeClient(); cl.season[1]["lastUpdateDateTime"] = "2027-01-01T00:00:00"
    st = db.Stats(); loader.load_matches(conn, cl.season, st); conn.commit()
    assert st.total_changes() == 0


def test_missing_match_deleted_only_on_full_season(conn):
    full_backfill(conn)
    cl = FakeClient(); cl.season.pop()                              # trận tương lai biến mất
    st = db.Stats(); loader.load_matches(conn, cl.season, st, full_season=False); conn.commit()
    assert count(conn, "matches") == 3                              # group/partial không xoá trận
    st = db.Stats(); loader.load_matches(conn, cl.season, st, full_season=True); conn.commit()
    assert st.d["matches"]["del"] == 1 and count(conn, "matches") == 2
    # payload rỗng (API lỗi) không được xoá sạch
    st = db.Stats(); loader.load_matches(conn, [], st, full_season=True); conn.commit()
    assert count(conn, "matches") == 2


def test_views_league_table(conn):
    full_backfill(conn)
    with conn.cursor() as c:
        c.execute("SELECT team_id, points, goals, opponent_goals, goal_diff FROM v_league_table ORDER BY team_id")
        rows = {r[0]: r[1:] for r in c.fetchall()}
    assert rows[40] == (3, 5, 1, 4) and rows[16] == (0, 1, 5, -4)
    assert rows[7] == (3, 2, 0, 2) and 134 not in rows            # trận chưa đá không tính


def _live_client():
    """Client mà trận 83192 đang đá (kickoff 30 phút trước) => group 5 là nhóm 'live'."""
    from datetime import datetime, timedelta, timezone
    cl = FakeClient()
    ko = datetime.now(timezone.utc) - timedelta(minutes=30)
    cl.season[2]["matchDateTimeUTC"] = ko.strftime("%Y-%m-%dT%H:%M:%SZ")
    return cl


def _expire_checks(conn):
    with conn.cursor() as c:
        c.execute("UPDATE sync_state SET last_checked_at = now() - interval '10 minutes'")
    conn.commit()


def test_tick_polls_cheaply_and_syncs_on_change(conn):
    cl = _live_client()
    full_backfill(conn, cl)                                         # backfill: có sync_state(-1) cho bl1/2026
    cl.requests_made = 0

    sync.tick(conn, cl)                                             # group 5 chưa có state -> last_change + fetch
    assert cl.requests_made == 2                                    # 1 last_change + 1 group_matches

    cl.requests_made = 0
    sync.tick(conn, cl)                                             # vừa check xong -> chưa tới hạn -> 0 request
    assert cl.requests_made == 0

    _expire_checks(conn); cl.requests_made = 0
    st = sync.tick(conn, cl)                                        # tới hạn nhưng lastchange KHÔNG đổi
    assert cl.requests_made == 1 and st.total_changes() == 0       # chỉ 1 request rẻ, không tải dữ liệu

    _expire_checks(conn)                                            # lastchange đổi + trận kết thúc 1-1
    cl.last_change_value = "2026-10-09T21:00:00.1"
    cl.season[2]["matchIsFinished"] = True
    cl.season[2]["matchResults"] = [{"resultID": 999001, "resultName": "Endergebnis", "pointsTeam1": 1,
                                     "pointsTeam2": 1, "resultOrderID": 2, "resultTypeID": 2,
                                     "resultTypeKind": "After90Minutes", "resultDescription": "d"}]
    st = sync.tick(conn, cl)
    assert st.d["matches"]["upd"] == 1 and st.d["match_results"]["ins"] == 1
    assert st.total_changes() == 2


def test_wal_silent_when_nothing_changes(conn):
    """Bằng chứng cho mục tiêu CDC: poll không đổi => 0 event trong WAL."""
    with conn.cursor() as c:
        c.execute("SHOW wal_level"); wl = c.fetchone()[0]
    if wl != "logical":
        pytest.skip("wal_level != logical")
    full_backfill(conn)
    with conn.cursor() as c:
        c.execute("SELECT pg_create_logical_replication_slot('t_slot','test_decoding')")
    conn.commit()
    try:
        sync.backfill(conn, FakeClient(), seasons=[2026], force=True)       # lần 2, dữ liệu y hệt
        with conn.cursor() as c:
            c.execute("""SELECT data FROM pg_logical_slot_get_changes('t_slot', NULL, NULL)
                         WHERE data NOT LIKE 'BEGIN%%' AND data NOT LIKE 'COMMIT%%'
                           AND data NOT LIKE '%%sync_state%%'""")
            ev = [r[0] for r in c.fetchall()]
        assert ev == [], f"WAL không im lặng: {ev[:3]}"
        # đổi 1 điểm số => đúng 1 event UPDATE trên match_results, có đủ 'old-key' nhờ REPLICA IDENTITY FULL
        cl = FakeClient(); cl.season[0]["matchResults"][1]["pointsTeam1"] = 6
        st = db.Stats(); loader.load_matches(conn, cl.season, st); conn.commit()
        with conn.cursor() as c:
            c.execute("""SELECT data FROM pg_logical_slot_get_changes('t_slot', NULL, NULL)
                         WHERE data LIKE 'table%%'""")
            ev = [r[0] for r in c.fetchall()]
        assert len(ev) == 1 and "match_results" in ev[0] and "UPDATE" in ev[0] and "old-key" in ev[0], ev
    finally:
        with conn.cursor() as c:
            c.execute("SELECT pg_drop_replication_slot('t_slot')")
        conn.commit()
