"""Kiểm thử tầng mart ML trên dữ liệu giả lập (xem fixture.py). Mọi giá trị kỳ vọng được TÍNH ĐỘC LẬP bằng Python thuần,
không dùng lại SQL của dbt.

Chạy:  cd football-lake && pytest ml/tests/mart -q
Cần: dbt-duckdb, duckdb, pandas. Nếu không có internet cho `dbt deps`, đặt MART_TEST_DBT_PACKAGES=<thư mục dbt_packages>.
"""
import copy
import math
import shutil
import tempfile
from datetime import date, datetime, timedelta

import pytest

duckdb = pytest.importorskip("duckdb")
pd = pytest.importorskip("pandas")
pytest.importorskip("dbt.version")

from ml.tests.mart import fixture as fx          # noqa: E402
from ml.tests.mart.harness import build           # noqa: E402


# --------------------------------------------------------------------------------------------------------------
# dựng 2 pipeline: gốc, và bản đổi TỈ SỐ 1 trận giữa mùa (để kiểm tra rò rỉ)
# --------------------------------------------------------------------------------------------------------------
PERTURB_IDX = 13          # trận thứ 2 của vòng 5


@pytest.fixture(scope="module")
def runs():
    tmp = tempfile.mkdtemp(prefix="mart_test_")
    base_matches = fx.build_matches()
    alt_matches = copy.deepcopy(base_matches)
    alt_matches[PERTURB_IDX]["hg"] += 2
    out = {}
    for name, ms in (("base", base_matches), ("alt", alt_matches)):
        lake, dwh = f"{tmp}/{name}_lake", f"{tmp}/{name}_dwh"
        fx.main(lake, matches=ms)
        build(lake, dwh, workdir=f"{tmp}/{name}_work")
        out[name] = dict(dwh=dwh, matches=ms, con=duckdb.connect())
    out["perturbed"] = base_matches[PERTURB_IDX]
    yield out
    shutil.rmtree(tmp, ignore_errors=True)


def df(run, sql):
    return run["con"].execute(sql.replace("{dwh}", run["dwh"])).fetchdf()


def feats(run, name="mart_ml_match_features"):
    return df(run, f"select * from '{{dwh}}/{name}.parquet'")


def approx_eq(a, b):
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None or (isinstance(b, float) and math.isnan(b))
    if b is None or (isinstance(b, float) and math.isnan(b)):
        return False
    return a == pytest.approx(b, rel=1e-9, abs=1e-9)


# --------------------------------------------------------------------------------------------------------------
# cấu trúc & nhãn
# --------------------------------------------------------------------------------------------------------------
def test_structure_and_contract(runs):
    f = feats(runs["base"])
    assert len(f) == 36 and f.match_key.is_unique
    nonprefixed = [c for c in f.columns if not c.startswith(("f_", "y_"))]
    # mọi cột không-tiền-tố phải là khoá/metadata/cờ, KHÔNG có số liệu sau trận lọt vào
    assert not any(x in nonprefixed for x in ("home_goals", "away_goals", "result", "home_xg", "total_goals"))
    assert {"split", "is_warm", "is_labeled"} <= set(nonprefixed)
    assert f[f.split == "predict"].y_result.isna().all() and (f.split == "predict").sum() == 6


def test_labels_match_fixture(runs):
    r = runs["base"]
    f = feats(r).set_index("openliga_match_id")
    for m in r["matches"]:
        row = f.loc[str(m["mid"])]
        if m["future"]:
            assert not row.is_labeled and pd.isna(row.y_home_goals)
            continue
        hg, ag = m["hg"], m["ag"]
        assert (row.y_home_goals, row.y_away_goals, row.y_total_goals) == (hg, ag, hg + ag)
        assert row.y_result == ("H" if hg > ag else "D" if hg == ag else "A")
        assert row.y_result_code == (2 if hg > ag else 1 if hg == ag else 0)
        assert (row.y_btts, row.y_over_2_5, row.y_over_1_5, row.y_over_3_5) == (
            int(hg > 0 and ag > 0), int(hg + ag > 2), int(hg + ag > 1), int(hg + ag > 3))


def test_split_is_chronological(runs):
    f = feats(runs["base"])
    lab = f[f.is_labeled]
    d = lambda s: lab[lab.split == s].match_date
    assert d("train").max() <= d("valid").min() and d("valid").max() <= d("test").min()
    assert set(lab.split) == {"train", "valid", "test"}
    n = len(lab)
    assert (lab.split == "train").sum() == pytest.approx(0.70 * n, abs=1.5)


# --------------------------------------------------------------------------------------------------------------
# đặc trưng phong độ: so với cài đặt độc lập
# --------------------------------------------------------------------------------------------------------------
def season_of(d):
    return d.year if d.month >= 7 else d.year - 1


def expected_team_features(matches, key_of):
    rows = []
    for m in matches:
        hk, ak = key_of[m["mid"]]
        ts = datetime(m["d"].year, m["d"].month, m["d"].day, 15, 30)
        for side, team, opp, gf, ga in (("h", hk, ak, m["hg"], m["ag"]), ("a", ak, hk, m["ag"], m["hg"])):
            played = not m["future"]
            pts = (3 if gf > ga else 1 if gf == ga else 0) if played else None
            rows.append(dict(mid=m["mid"], side=side, team=team, opp=opp, ts=ts, played=played, gf=gf, ga=ga, pts=pts,
                             season=season_of(m["d"])))
    out = {}
    mean = lambda xs: sum(xs) / len(xs)
    for r in rows:
        prior = sorted((p for p in rows if p["team"] == r["team"] and p["played"]
                        and p["ts"] + timedelta(hours=3) < r["ts"]), key=lambda p: p["ts"])
        f = {"n_prev": len(prior)}

        def win(n, key=lambda p: p, src=None):
            src = prior if src is None else src
            return [key(p) for p in src[-n:]] if len(src) >= n else None

        for n in (3, 5, 10, 20):
            for nm, fn in (("pts", lambda p: p["pts"]), ("gf", lambda p: p["gf"]), ("ga", lambda p: p["ga"]),
                           ("gd", lambda p: p["gf"] - p["ga"]), ("win_rate", lambda p: int(p["pts"] == 3)),
                           ("cs_rate", lambda p: int(p["ga"] == 0)),
                           ("btts_rate", lambda p: int(p["gf"] > 0 and p["ga"] > 0)),
                           ("over25_rate", lambda p: int(p["gf"] + p["ga"] > 2))):
                v = win(n, fn)
                f[f"{nm}_l{n}"] = mean(v) if v else None
        side_prior = [p for p in prior if p["side"] == r["side"]]
        for nm, k in (("pts", "pts"), ("gf", "gf"), ("ga", "ga")):
            v = win(5, lambda p, k=k: p[k], side_prior)
            f[f"side_{nm}_l5"] = mean(v) if v else None
        sp = [p for p in prior if p["season"] == r["season"]]
        f["s_played"] = len(sp)
        f["s_ppg"] = mean([p["pts"] for p in sp]) if sp else None
        f["s_gf_pg"] = mean([p["gf"] for p in sp]) if sp else None
        f["s_ga_pg"] = mean([p["ga"] for p in sp]) if sp else None
        f["s_gd_pg"] = mean([p["gf"] - p["ga"] for p in sp]) if sp else None
        h2 = [p for p in prior if p["opp"] == r["opp"]][-5:]
        f["h2h_n"] = len(h2)
        f["h2h_pts"] = mean([p["pts"] for p in h2]) if h2 else None
        f["h2h_gf"] = mean([p["gf"] for p in h2]) if h2 else None
        f["h2h_ga"] = mean([p["ga"] for p in h2]) if h2 else None
        f["rest_days"] = (r["ts"] - prior[-1]["ts"]).total_seconds() / 86400 if prior else None
        for nd in (7, 14, 30):
            f[f"n_{nd}d"] = sum(1 for p in prior if p["ts"] >= r["ts"] - timedelta(days=nd))
        form = win(5, lambda p: "W" if p["pts"] == 3 else "D" if p["pts"] == 1 else "L")
        f["form5"] = "".join(form) if form else None
        out[(r["mid"], r["side"])] = f
    return out


def test_rolling_features_match_independent_implementation(runs):
    r = runs["base"]
    f = feats(r).set_index("openliga_match_id")
    key_of = {m["mid"]: (f.loc[str(m["mid"])].home_key, f.loc[str(m["mid"])].away_key) for m in r["matches"]}
    exp = expected_team_features(r["matches"], key_of)
    bad, compared = [], 0
    for (mid, side), feat in exp.items():
        prefix = "f_home_" if side == "h" else "f_away_"
        row = f.loc[str(mid)]
        for name, v in feat.items():
            col = "f_h2h_n" if name == "h2h_n" else prefix + name        # số lần đối đầu là chung cho 2 đội
            if col not in row.index:                      # bộ kỳ vọng tính dư vài cửa sổ mà mart không công bố
                continue
            compared += 1
            got = row[col]
            if name == "form5":
                ok = (v is None and (got is None or pd.isna(got))) or got == v
            else:
                ok = approx_eq(v if v is not None else None, None if got is None or pd.isna(got) else float(got))
            if not ok:
                bad.append((mid, side, name, v, got))
    assert not bad, f"{len(bad)} giá trị lệch, vd: {bad[:5]}"
    assert compared >= 36 * 2 * 35, f"chỉ so {compared} giá trị: bộ kiểm tra quá lỏng"


def test_features_absent_in_fixture_are_null_not_zero(runs):
    f = feats(runs["base"])
    for c in ("f_home_xgf_l5", "f_home_ppda_l5", "f_mkt_p_home", "f_weather_temp_c", "f_ref_avg_cards"):
        assert f[c].isna().all(), c
    assert not f.has_odds.any() and not f.has_xg.any()
    # hồi quy từ dữ liệu thật: forecast Understat tính từ xG của chính trận => KHÔNG được là đặc trưng (f_)
    assert not [c for c in f.columns if c.startswith("f_us_")] and {"y_us_xg_p_home", "y_us_xg_p_draw", "y_us_xg_p_away"} <= set(f.columns)
    assert (f.f_ref_n_prev == 0).all()


# --------------------------------------------------------------------------------------------------------------
# KHÔNG RÒ RỈ: đổi tỉ số 1 trận -> mọi đặc trưng của trận đó và các trận TRƯỚC nó phải y nguyên
# --------------------------------------------------------------------------------------------------------------
def test_no_leakage_when_a_result_changes(runs):
    base, alt, pm = feats(runs["base"]), feats(runs["alt"]), runs["perturbed"]
    base, alt = base.set_index("match_key").sort_index(), alt.set_index("match_key").sort_index()
    fcols = [c for c in base.columns if c.startswith("f_")]
    pdate = pd.Timestamp(pm["d"])
    upto = base.match_date <= pdate                         # gồm CHÍNH trận bị đổi và mọi trận trước/cùng ngày
    assert upto.sum() >= 13
    mism = {c for c in fcols for k in base.index[upto]
            if not approx_eq(None if pd.isna(base.at[k, c]) else base.at[k, c],
                             None if pd.isna(alt.at[k, c]) else alt.at[k, c])}
    assert not mism, f"RÒ RỈ: đặc trưng thay đổi dù chỉ đổi kết quả trận hiện tại/sau: {sorted(mism)[:8]}"
    # độ nhạy: nhãn của trận đó đổi, và đặc trưng của các trận SAU của 2 đội liên quan có đổi
    k = base.index[base.openliga_match_id == str(pm["mid"])][0]
    assert base.at[k, "y_home_goals"] != alt.at[k, "y_home_goals"]
    involved = {base.at[k, "home_key"], base.at[k, "away_key"]}
    later = base[(base.match_date > pdate) & (base.home_key.isin(involved) | base.away_key.isin(involved))]
    changed = [kk for kk in later.index for c in fcols
               if not approx_eq(None if pd.isna(base.at[kk, c]) else base.at[kk, c],
                                None if pd.isna(alt.at[kk, c]) else alt.at[kk, c])]
    assert changed, "harness không nhạy: đổi kết quả mà trận sau không đổi gì"


# --------------------------------------------------------------------------------------------------------------
# mức độ quan tâm Wikipedia
# --------------------------------------------------------------------------------------------------------------
def test_attention_side_table_exact(runs):
    r = runs["base"]
    f = feats(r).set_index("openliga_match_id")
    att = df(r, "select * from '{dwh}/mart_ml_match_attention.parquet'").set_index("match_key")
    assert att.index.is_unique and set(att.index) <= set(f.match_key)
    row = next(f.loc[str(m["mid"])] for m in r["matches"] if not m["future"] and f.loc[str(m["mid"])].home_key == "Bayern Munich")
    d = row.match_date.date()
    views = lambda day: fx.pageviews_value("en", day) + fx.pageviews_value("de", day)
    w7 = [views(d - timedelta(days=i)) for i in range(1, 8)]
    w28 = [views(d - timedelta(days=i)) for i in range(1, 29) if d - timedelta(days=i) >= date(2025, 8, 1)]
    a = att.loc[row.match_key]
    assert a.f_home_att_7d == pytest.approx(sum(w7) / 7)
    assert a.f_home_att_28d == pytest.approx(sum(w28) / len(w28))
    assert a.f_home_att_ratio == pytest.approx((sum(w7) / 7) / (sum(w28) / len(w28)))
    # chỉ có dòng cho trận mà ít nhất 1 đội có >= 14 ngày dữ liệu; đội không có dữ liệu -> NULL (không phải 0)
    covered = {"Bayern Munich", "Borussia Dortmund"}
    # trận tương lai (2027) nằm ngoài dữ liệu lượt xem (hết 2026-01-31) -> cửa sổ trống -> KHÔNG có dòng (không suy từ dữ liệu cũ)
    in_range = f.match_date < pd.Timestamp("2026-03-01")
    expected_rows = f[in_range & (f.home_key.isin(covered) | f.away_key.isin(covered))].match_key
    assert not set(f[~in_range].match_key) & set(att.index)
    assert set(att.index) == set(expected_rows)
    one_side = att.join(f.set_index("match_key")[["home_key", "away_key"]])
    only_home = one_side[one_side.home_key.isin(covered) & ~one_side.away_key.isin(covered)]
    assert len(only_home) > 0 and only_home.f_away_att_7d.isna().all() and only_home.f_att_log_ratio_7d.isna().all()
    both = one_side[one_side.home_key.isin(covered) & one_side.away_key.isin(covered)]
    assert len(both) > 0 and both.f_att_log_ratio_7d.notna().all()


def test_team_attention_maps_p10_article_names(runs):
    a = df(runs["base"], "select count(distinct team_key) n from '{dwh}/mart_ml_team_attention_daily.parquet'")
    assert a.n[0] >= 41, f"chỉ map được {a.n[0]}/42 tên bài p10"     # 42 đội; Atlético có thể cần biến thể riêng
    keys = set(df(runs["base"], "select distinct team_key from '{dwh}/mart_ml_team_attention_daily.parquet'").team_key)
    assert {"Bayern Munich", "Borussia Dortmund", "Arsenal"} <= keys


# --------------------------------------------------------------------------------------------------------------
# văn bản
# --------------------------------------------------------------------------------------------------------------
def test_text_tables_are_separate_per_source(runs):
    r = runs["base"]
    t = lambda n: df(r, f"select * from '{{dwh}}/mart_ml_text_{n}.parquet'")
    gn, fn, yv, yc, wk = t("google_news"), t("football_news"), t("youtube_videos"), t("youtube_comments"), t("wikipedia")
    assert (len(gn), len(fn), len(yv), len(yc), len(wk)) == (60, 1, 8, 40, 1)
    assert gn.news_id.is_unique and yv.video_id.is_unique and yc.comment_id.is_unique
    # mỗi nguồn giữ cột gốc riêng, không bị ép khung chung, và không có liên kết đội/trận
    assert {"publisher", "source_type", "search_queries", "title_hash"} <= set(gn.columns)
    assert {"image_url", "source_name"} <= set(fn.columns)
    assert {"channel_id", "channel_title", "description", "n_comments_collected"} <= set(yv.columns)
    assert {"author", "author_hash", "like_count", "video_title", "days_after_video"} <= set(yc.columns)
    for d in (gn, fn, yv, yc, wk):
        assert not [c for c in d.columns if "team" in c or "mention" in c]
    # quan hệ chỉ theo ID: 40 bình luận chia đều 8 video, tổng like khớp
    assert (yv.n_comments_collected == 5).all()
    assert yv.comment_likes_sum.sum() == yc.like_count.sum()
    assert yc.video_title.notna().all() and (yc.days_after_video >= 0).all()
    assert not [c for c in feats(r).columns if "_txt_" in c or "_att_" in c]      # mart chính không chứa văn bản/lượt xem
    import os
    assert not os.path.exists(f"{r['dwh']}/mart_ml_text_docs.parquet")             # bảng gộp cũ đã bỏ


def test_placeholder_date_matches_are_excluded(runs):
    """Hồi quy từ dữ liệu thật: giải rác BLCLAUDE có 11 trận ngày 1970-01-01 lọt vào fact_match."""
    for name in ("fact_match", "mart_ml_match_features", "mart_ml_team_match_features"):
        d = df(runs["base"], f"select min(match_date) mn from '{{dwh}}/{name}.parquet'")
        assert d.mn[0] >= pd.Timestamp("2000-01-01"), name
    fm = df(runs["base"], "select count(*) n from '{dwh}/fact_match.parquet' where openliga_match_id = '%d'" % fx.JUNK_MATCH)
    assert fm.n[0] == 0


# --------------------------------------------------------------------------------------------------------------
# cú sút / cầu thủ / bàn thắng
# --------------------------------------------------------------------------------------------------------------
def test_shots_geometry_and_game_state(runs):
    s = df(runs["base"], "select * from '{dwh}/mart_ml_shots.parquet'").set_index("shot_id")
    assert len(s) == 6
    dx, dy = (1 - 0.90) * 105, 0.0
    assert s.loc["1", "f_distance_m"] == pytest.approx(math.hypot(dx, dy))
    assert s.loc["1", "f_angle_rad"] == pytest.approx(math.atan2(7.32 * dx, dx * dx - 3.66 ** 2))
    assert s.loc["1", "f_is_assisted"] == 1 and s.loc["2", "f_is_header"] == 1 and s.loc["5", "f_is_penalty"] == 1
    assert s.y_is_goal.to_dict() == {"1": 1, "2": 0, "3": 0, "4": 0, "5": 1, "6": 0}
    # tỉ số TRƯỚC cú sút (chỉ bàn ở phút trước): sút 2 (nhà, p30) -> nhà dẫn 1; sút 5 (khách, p55) -> khách kém 1;
    # sút 3 (nhà, p70) -> 1-1 => 0; sút 1/4 cùng phút 10 -> 0
    assert s.f_score_diff_before.to_dict() == {"1": 0, "2": 1, "3": 0, "4": 0, "5": -1, "6": 0}
    assert (s.benchmark_xg == 0.1).all() and not any(c == "xg" for c in s.columns)


def test_player_season_features_use_only_prior_seasons(runs):
    p = df(runs["base"], "select * from '{dwh}/mart_ml_player_season_features.parquet'")
    a = p[(p.y_minutes == 1000)].iloc[0]                      # Alpha One, mùa 2025
    assert a.f_prev1_minutes == 2400 and a.f_prev1_goals_p90 == pytest.approx(12 * 90 / 2400)
    assert a.f_prev2_goals_p90 == pytest.approx(20 * 90 / 2700) and a.f_has_prev1 == 1
    first = p[(p.season == 2023) & (p.y_minutes == 2700)].iloc[0]       # mùa đầu tiên: chưa có lịch sử
    assert pd.isna(first.f_prev1_goals_p90) and first.f_has_prev1 == 0
    assert bool(first.is_reliable_target) and not bool(p[(p.season == 2023) & (p.y_minutes == 300)].iloc[0].is_reliable_target)
    beta24 = p[(p.season == 2024) & (p.y_minutes == 1800)].iloc[0]       # mùa trước chỉ 300 phút (<450) => p90 NULL
    assert pd.isna(beta24.f_prev1_goals_p90) and beta24.f_prev1_minutes == 300 and beta24.f_has_prev1 == 1


def test_goal_events_game_state(runs):
    r = runs["base"]
    g = df(r, "select * from '{dwh}/mart_ml_goals.parquet'").set_index("goal_id")
    exp = {}
    for m in r["matches"]:
        for gl in m["goals"]:
            home = gl["scoring_team_id"] == m["home"]
            sh, sa = gl["score_team1"], gl["score_team2"]
            bh, ba = (sh - 1, sa) if home else (sh, sa - 1)
            diff = (bh - ba) if home else (ba - bh)
            exp[gl["goal_id"]] = (bh, ba, diff, "trailing" if diff < 0 else "tied" if diff == 0 else "leading")
    assert len(g) == len(exp)
    for gid, (bh, ba, diff, state) in exp.items():
        row = g.loc[gid]
        assert (row.f_home_goals_before, row.f_away_goals_before, row.f_scorer_diff_before, row.y_state_before_goal) == (bh, ba, diff, state)
        assert row.y_is_equalizer == int(diff == -1) and row.y_is_go_ahead == int(diff == 0)
