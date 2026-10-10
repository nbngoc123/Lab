"""Dữ liệu giả lập (xác định, seed cố định) cho kiểm thử tầng mart ML, ghi vào 1 thư mục 'lake' cục bộ:
   - OpenLigaDB CDC: 1 giải 6 đội, 10 vòng đã đá (30 trận, bàn thắng khớp tỉ số) + 2 vòng sắp đá (6 trận)
   - Google News / football_news / YouTube (video+bình luận) / Wikipedia / Wikimedia pageviews / Understat shots+players
   python -m ml.tests.mart.fixture /tmp/lake"""
import gzip, json, os, random, sys
from datetime import date, datetime, timedelta, timezone

from pipelines.p26 import main as p26
from pipelines.p26.tests.test_p26 import FakeConsumer, Clock, Msg

T = p26.TOPIC_PREFIX
_off = {}


def micros(s):                       # 'YYYY-MM-DDTHH:MM:SS' (giờ địa phương) -> micro-giây (Debezium MicroTimestamp)
    return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()) * 1_000_000


def event(table, op, after=None, before=None, lsn=1000):
    p = {"before": before, "after": after, "op": op, "ts_ms": 1_790_000_000_000,
         "source": {"lsn": lsn, "txId": 1, "snapshot": "true" if op == "r" else "false"}}
    o = _off.setdefault(table, 0); _off[table] = o + 1
    return Msg(T + table, o, json.dumps({"schema": {}, "payload": p}).encode())


def match_row(mid, home, away, gid, utc, local, fin, loc=None, viewers=None, league=4937):
    return {"match_id": mid, "league_id": league, "group_id": gid, "team1_id": home, "team2_id": away,
            "team1_group_name": None, "team2_group_name": None, "location_id": loc, "match_time_utc": utc,
            "match_time_local": micros(local), "time_zone_id": None, "is_finished": fin, "viewers": viewers,
            "api_updated_at": micros("2026-01-01T00:00:00")}


def res(rid, mid, order, tid, kind, p1, p2):
    return {"result_id": rid, "match_id": mid, "result_name": kind, "result_order": order, "result_type_id": tid,
            "result_type_kind": kind, "description": "d", "points_team1": p1, "points_team2": p2}


def goal(gid, mid, s1, s2, minute, who, name, team, own=False, ot=False):
    return {"goal_id": gid, "match_id": mid, "minute": minute, "scorer_id": who, "scorer_name": name,
            "scoring_team_id": team, "score_team1": s1, "score_team2": s2, "is_penalty": False,
            "is_own_goal": own, "is_overtime": ot, "comment": None}

TEAMS = {40: "FC Bayern München", 7: "Borussia Dortmund", 23: "RB Leipzig",
         9: "Bayer 04 Leverkusen", 91: "Eintracht Frankfurt", 16: "VfB Stuttgart"}
START = date(2025, 8, 23)
LEAGUE = 4937
JUNK_MATCH = 99001


def schedule():
    """Vòng tròn 6 đội -> 5 vòng lượt đi + 5 vòng lượt về (đảo sân) + 2 vòng tương lai."""
    ids = list(TEAMS)
    rounds = []
    arr = ids[:]
    for _ in range(5):
        rounds.append([(arr[i], arr[-1 - i]) for i in range(3)])
        arr = [arr[0]] + [arr[-1]] + arr[1:-1]
    rounds = rounds + [[(b, a) for a, b in r] for r in rounds]
    rounds = rounds + [[(b, a) for a, b in r] for r in rounds[:2]]       # vòng 11-12 (tương lai)
    return rounds


def build_matches():
    rng = random.Random(7)
    out, mid, gid = [], 90000, 800000
    for ri, rnd in enumerate(schedule()):
        future = ri >= 10
        d = date(2027, 2, 6) + timedelta(days=7 * (ri - 10)) if future else START + timedelta(days=7 * ri)
        for home, away in rnd:
            mid += 1
            hg, ag = (None, None) if future else (rng.choice([0, 0, 1, 1, 1, 2, 2, 3]), rng.choice([0, 0, 1, 1, 2, 3]))
            goals = []
            if not future:
                sides = ["h"] * hg + ["a"] * ag
                rng.shuffle(sides)
                minutes = sorted(rng.sample(range(1, 90), len(sides)))
                sh = sa = 0
                for s, mn in zip(sides, minutes):
                    gid += 1
                    sh += s == "h"; sa += s == "a"
                    goals.append(goal(gid, mid, sh, sa, mn, gid, f"Player {gid}", home if s == "h" else away))
            out.append(dict(mid=mid, home=home, away=away, round=ri + 1, d=d, future=future, hg=hg, ag=ag, goals=goals,
                            ht_h=0 if future else rng.randint(0, hg), ht_a=0 if future else rng.randint(0, ag)))
    return out


def openliga_messages(matches):
    r = lambda t, row: event(t, "r", row, lsn=500)
    msgs = [r("sports", {"sport_id": 1, "sport_name": "Fußball"}),
            r("leagues", {"league_id": LEAGUE, "shortcut": "bl1", "season": 2025, "name": "1. Bundesliga 2025/2026", "sport_id": 1})]
    # giải rác thật trên OpenLigaDB ("BLCLAUDE"): giờ đá placeholder 1970-01-01, chưa đá -> phải bị loại khỏi core và mart
    msgs += [r("leagues", {"league_id": 9999, "shortcut": "blclaude", "season": 2025, "name": "Junk league", "sport_id": 1}),
             r("groups", {"group_id": 69999, "league_id": 9999, "group_order": 1, "group_name": "1. Spieltag"}),
             r("matches", match_row(JUNK_MATCH, 40, 7, 69999, "1970-01-01T00:00:00.000000Z", "1970-01-01T00:00:00", False, league=9999))]
    msgs += [r("teams", {"team_id": i, "team_name": n, "short_name": n.split()[-1], "icon_url": f"u{i}"}) for i, n in TEAMS.items()]
    for ri in range(12):
        msgs.append(r("groups", {"group_id": 60000 + ri, "league_id": LEAGUE, "group_order": ri + 1, "group_name": f"{ri + 1}. Spieltag"}))
    rid = 700000
    for m in matches:
        utc = f"{m['d']}T15:30:00.000000Z"
        msgs.append(r("matches", match_row(m["mid"], m["home"], m["away"], 60000 + m["round"] - 1, utc, f"{m['d']}T17:30:00",
                                           not m["future"], league=LEAGUE)))
        if not m["future"]:
            rid += 1; msgs.append(r("match_results", res(rid, m["mid"], 1, 1, "HalfTime", m["ht_h"], m["ht_a"])))
            rid += 1; msgs.append(r("match_results", res(rid, m["mid"], 2, 2, "After90Minutes", m["hg"], m["ag"])))
            msgs += [r("goals", g) for g in m["goals"]]
    return msgs


def gz(path, obj=None, lines=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as f:
        if lines is not None:
            f.write("\n".join(json.dumps(x, ensure_ascii=False) for x in lines) + "\n")
        else:
            json.dump(obj, f, ensure_ascii=False)


def pageviews_value(lang, day):
    """Lượt xem xác định theo ngày, để test tính được kết quả kỳ vọng bằng tay."""
    idx = (day - date(2025, 8, 1)).days
    return 1000 + 10 * idx + (500 if lang == "de" else 0)


HEADS = ["Bayern Munich cruise past rivals", "Borussia Dortmund eye title push", "Bayern Munich vs Borussia Dortmund preview",
         "Premier League weekend roundup", "Como sign new striker", "Nice weather expected for the derby",
         "RB Leipzig injury worry", "Bundesliga matchday report"]
# đội KỲ VỌNG được nhắc (theo tiêu đề) - dùng team_key chuẩn của seed
HEAD_TEAMS = {HEADS[0]: {"Bayern Munich"}, HEADS[1]: {"Borussia Dortmund"}, HEADS[2]: {"Bayern Munich", "Borussia Dortmund"},
              HEADS[3]: set(), HEADS[4]: set(), HEADS[5]: set(), HEADS[6]: {"RB Leipzig"}, HEADS[7]: set()}


def google_news_records():
    rfc = lambda d: datetime(d.year, d.month, d.day, 10, 0, tzinfo=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    rng = random.Random(3)
    out = []
    for i in range(60):
        d = date(2025, 9, 20) + timedelta(days=i * 2)
        out.append({"guid": f"g{i}", "title": rng.choice(HEADS), "summary": "summary text " + str(i), "link": f"http://x/{i}",
                    "source": "Pub", "published": rfc(d), "query": "Bayern", "lang": "en", "feed": "rss", "_date": d})
    return out


def video_records():
    return [{"video_id": f"v{i}", "query": "Bundesliga highlights", "published_at": f"2025-10-{i + 1:02d}T08:00:00Z",
             "channel_id": "c1", "title": "Highlights: Bayern Munich vs Dortmund" if i % 2 == 0 else "Tin tức bóng đá hôm nay",
             "description": "desc", "channel_title": "Chan"} for i in range(8)]


def comment_records():
    return [{"comment_id": f"c{i}", "video_id": f"v{i % 8}", "author": f"u{i % 5}", "text": "great", "like_count": i % 7,
             "published_at": f"2025-10-{(i % 8) + 1:02d}T12:00:00Z"} for i in range(40)]


def write_text_sources(root):
    gn = [{k: v for k, v in r.items() if k != "_date"} for r in google_news_records()]
    gz(f"{root}/raw/rss/google_news/query=Bayern/lang=en/ingest_date=2026-01-05/entries.jsonl.gz", lines=gn)
    gz(f"{root}/raw/football_news/query=premier_league/ingest_date=2026-01-05/news.json.gz",
       {"articles": [{"title": "Borussia Dortmund sign winger", "url": "http://n/1", "publishedAt": "2025-10-01T09:00:00Z",
                      "source": {"name": "NewsX"}}]})
    gz(f"{root}/raw/youtube/videos/query=bundesliga/ingest_date=2026-01-05/videos.jsonl.gz", lines=video_records())
    gz(f"{root}/raw/youtube/comments/query=bundesliga/ingest_date=2026-01-05/comments.jsonl.gz", lines=comment_records())
    gz(f"{root}/raw/wikipedia_articles/entity=team/lang=en/fetched_date=2026-01-05/bayern.json.gz",
       {"page_id": "100", "title": "FC Bayern Munich", "entity_type": "team", "lang": "en",
        "extract": "Bayern Munich is a German club.", "word_count": 6})


def write_pageviews(root, article_names=None):
    base = f"{root}/raw/wikimedia_pageviews/per_article/entity=team"
    days = [date(2025, 8, 1) + timedelta(days=i) for i in range(184)]       # 2025-08-01 .. 2026-01-31
    for art in ["FC_Bayern_Munich", "Borussia_Dortmund"]:
        for lang in ["en", "de"]:
            items = [{"timestamp": d.strftime("%Y%m%d00"), "views": pageviews_value(lang, d)} for d in days]
            gz(f"{base}/lang={lang}/article={art}/ingest_date=2026-02-01/daily.json.gz", {"items": items})
    for art in article_names or []:                                          # chỉ để kiểm tra map tên bài -> team_key
        if art in ("FC_Bayern_Munich", "Borussia_Dortmund"):
            continue
        gz(f"{base}/lang=en/article={art}/ingest_date=2026-02-01/daily.json.gz",
           {"items": [{"timestamp": "2025101000", "views": 5}]})


def write_understat(root):
    shot = lambda i, mn, side, x, y, res_, sit, st, player, pid, ass: {
        "id": str(i), "match_id": "555", "minute": str(mn), "X": str(x), "Y": str(y), "xG": "0.1", "result": res_,
        "player": player, "player_id": str(pid), "player_assisted": ass, "lastAction": "Pass", "situation": sit,
        "shotType": st, "h_team": "Arsenal", "a_team": "Chelsea", "h_goals": "1", "a_goals": "1"}
    h = [shot(1, 10, "h", 0.90, 0.50, "Goal", "OpenPlay", "RightFoot", "A Player", 11, "B Player"),
         shot(2, 30, "h", 0.80, 0.40, "SavedShot", "OpenPlay", "Head", "A Player", 11, None),
         shot(3, 70, "h", 0.88, 0.50, "MissedShots", "SetPiece", "LeftFoot", "C Player", 12, None)]
    a = [shot(4, 10, "a", 0.85, 0.30, "SavedShot", "OpenPlay", "RightFoot", "D Player", 21, None),
         shot(5, 55, "a", 0.88, 0.50, "Goal", "Penalty", "RightFoot", "D Player", 21, None),
         shot(6, 80, "a", 0.75, 0.60, "BlockedShot", "OpenPlay", "RightFoot", "E Player", 22, "D Player")]
    gz(f"{root}/raw/understat/shots/league=EPL/season=2025/match_id=555/shots.json.gz", {"h": h, "a": a})
    # cầu thủ 3 mùa (2023, 2024, 2025)
    def pl(pid, name, season, minutes, goals, xg, assists, xa, shots_, kp, team="Arsenal", pos="F S"):
        return {"id": str(pid), "player_name": name, "games": "30", "time": str(minutes), "goals": str(goals), "xG": str(xg),
                "assists": str(assists), "xA": str(xa), "shots": str(shots_), "key_passes": str(kp), "yellow_cards": "2",
                "red_cards": "0", "position": pos, "team_title": team, "npg": str(goals), "npxG": str(xg),
                "xGChain": "9.0", "xGBuildup": "3.0"}
    data = {2023: [pl(11, "Alpha One", 2023, 2700, 20, 18.0, 5, 4.0, 90, 40), pl(12, "Beta Two", 2023, 300, 1, 1.0, 0, 0.2, 10, 5)],
            2024: [pl(11, "Alpha One", 2024, 2400, 12, 14.0, 6, 5.0, 80, 45), pl(12, "Beta Two", 2024, 1800, 4, 3.5, 2, 1.5, 40, 20)],
            2025: [pl(11, "Alpha One", 2025, 1000, 8, 6.0, 1, 1.0, 35, 15), pl(12, "Beta Two", 2025, 2200, 6, 5.0, 3, 2.5, 55, 25)]}
    for s, rows in data.items():
        gz(f"{root}/raw/understat/players/league=EPL/season={s}/ingest_date=2026-01-05/players.json.gz", rows)


def p10_team_articles():
    """Tên bài đội thật mà p10 thu (để kiểm tra map sang team_key); bỏ các trang giải đấu."""
    import ast
    src = open(os.path.join(os.path.dirname(__file__), "..", "..", "..", "pipelines", "p10", "main.py"), encoding="utf-8").read()
    teams = next(ast.literal_eval(n.value) for n in ast.walk(ast.parse(src))
                 if isinstance(n, ast.Assign) and any(getattr(t, "id", "") == "TEAMS" for t in n.targets))
    comps = {"Premier_League", "La_Liga", "Serie_A", "Bundesliga", "Ligue_1", "UEFA_Champions_League",
             "FIFA_World_Cup", "UEFA_European_Championship"}
    return [a for a in teams if a not in comps]


def main(root, matches=None):
    matches = matches or build_matches()

    def writer(key, obj, meta):
        gz(os.path.join(root, key), obj)
    p26.consume_and_store(FakeConsumer(openliga_messages(matches)), writer=writer, clock=Clock(),
                          first_idle=3, idle=3, max_seconds=10_000)
    write_text_sources(root)
    write_pageviews(root, article_names=p10_team_articles())
    write_understat(root)
    return matches


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "/tmp/lake")
