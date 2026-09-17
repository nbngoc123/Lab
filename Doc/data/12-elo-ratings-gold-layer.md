# 12 — Bảng ELO tự tính (Gold layer tự sinh) → MinIO

**Kiểu ingest:** Không thu thập từ bên ngoài — **tự sinh dữ liệu mới** bằng cách tính toán trên silver layer đã có
**Tần suất:** sau mỗi lần silver cập nhật (hàng ngày/hàng tuần)
**Độ khó:** ★★★☆☆ — khó ở thuật toán, không phải ở ingest

---

## 1. Vì sao cần nguồn này

Mười một nguồn trước đều **đưa dữ liệu từ ngoài vào** lake. Nguồn này khác hẳn về bản chất: nó **không lấy gì từ bên ngoài** — nó đọc dữ liệu đã có sẵn ở silver (từ file 03, 05, 09), chạy một thuật toán, và ghi ra một bảng hoàn toàn mới mà trước đó không tồn tại ở bất kỳ đâu.

Đây chính là định nghĩa của **gold layer**: dữ liệu được tạo ra bởi chính pipeline của bạn, không phải copy từ nguồn nào. Một data lake thật luôn có tầng này — nếu thiếu, lake của bạn chỉ là "kho chứa file" chứ chưa phải "nền tảng phân tích".

Chọn ELO rating vì ba lý do: thuật toán đơn giản (\<50 dòng code), kết quả *diễn giải được* (ai cũng hiểu "điểm cao hơn = mạnh hơn"), và **kiểm chứng được** bằng cách so với bảng xếp hạng thật.

## 2. Thuật toán ELO cho bóng đá

Công thức gốc (dùng cho cờ vua) cần điều chỉnh 2 chỗ cho bóng đá:

- **Có kết quả hòa** → điểm thực tế (S) = 1 (thắng), 0.5 (hòa), 0 (thua)
- **Chênh lệch bàn thắng** ảnh hưởng tới mức độ điều chỉnh K (thắng đậm tăng điểm nhiều hơn thắng sát nút)

```
E_home = 1 / (1 + 10^((R_away - R_home - home_advantage) / 400))
K_dieu_chinh = K_co_ban × goal_diff_multiplier(chenh_lech_ban)

R_home_moi = R_home + K_dieu_chinh × (S_home - E_home)
R_away_moi = R_away + K_dieu_chinh × (S_away - E_away)
```

Tham số chuẩn dùng trong các mô hình ELO bóng đá công khai (ví dụ FiveThirtyEight SPI):
- `home_advantage ≈ 65` điểm ELO
- `K_co_ban = 20`
- `R_khoi_dau = 1500` cho đội chưa có lịch sử

## 3. Layout trong lake

```
gold/analytics/elo_ratings/history/part-0.parquet      ← ELO sau MỖI trận (time series)
gold/analytics/elo_ratings/current/part-0.parquet       ← ELO mới nhất mỗi đội (snapshot)
gold/analytics/elo_ratings/predictions/season=2425/part-0.parquet   ← xác suất thắng/hòa/thua dự đoán trước mỗi trận
_meta/elo/run_log/ingest_date=2026-09-16.json           ← nhật ký lần chạy: bao nhiêu trận, độ chính xác
```

Nguồn đầu vào: `silver/matches/fd_matches/` (file 03) — nhiều mùa nhất, phù hợp để ELO "khởi động" và ổn định trước khi dùng cho mùa hiện tại.

## 4. Script tính ELO — `pipelines/p12_elo_gold.py`

```python
"""
Gold layer tự sinh: tính ELO rating từ lịch sử trận đấu ở silver.
Không gọi API/tải file nào — input 100% lấy từ lake, output ghi ngược vào lake.
"""
import json
from datetime import datetime, timezone

import duckdb
import pandas as pd
from lake.minio_io import put_parquet, S3, BUCKET, today, summary

SRC = "elo-model"           # "nguồn" ở đây là chính pipeline này
D = today()

K_BASE = 20
HOME_ADV = 65
START_RATING = 1500


def load_matches() -> pd.DataFrame:
    """Đọc toàn bộ trận từ silver (file 03) qua DuckDB, không cần tải về máy."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
      SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false;
      SET s3_url_style='path';
      SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin123';
    """)
    df = con.sql("""
        SELECT match_date, home_team, away_team, home_goals, away_goals, result
        FROM read_parquet('s3://football-lake/silver/matches/fd_matches/division=E0/**/*.parquet')
        WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
        ORDER BY match_date
    """).df()
    df["match_date"] = pd.to_datetime(df["match_date"])
    return df


def goal_diff_multiplier(diff: int) -> float:
    """Thắng đậm -> điều chỉnh ELO mạnh hơn. Công thức tham khảo từ mô hình 538 SPI."""
    diff = abs(diff)
    if diff <= 1:
        return 1.0
    elif diff == 2:
        return 1.5
    else:
        return (11 + diff) / 8


def expected_score(r_a: float, r_b: float, home_adv: float = 0) -> float:
    return 1 / (1 + 10 ** ((r_b - r_a - home_adv) / 400))


def run_elo(matches: pd.DataFrame):
    """
    Chạy tuần tự qua từng trận theo thời gian, cập nhật ELO ngay sau mỗi trận.
    Đây là phần thuật toán cốt lõi — cố tình viết rõ ràng thay vì tối ưu tốc độ.
    """
    ratings: dict[str, float] = {}
    history_rows = []
    correct_predictions = 0
    total_predictable = 0

    for _, m in matches.iterrows():
        home, away = m.home_team, m.away_team
        r_home = ratings.get(home, START_RATING)
        r_away = ratings.get(away, START_RATING)

        e_home = expected_score(r_home, r_away, HOME_ADV)
        e_away = 1 - e_home

        # đánh giá dự đoán TRƯỚC KHI cập nhật (tránh nhìn trước tương lai)
        predicted_home_win = e_home > 0.5
        actual_home_win = m.result == "H"
        if m.result in ("H", "A"):     # bỏ qua trận hòa khi tính accuracy nhị phân
            total_predictable += 1
            if predicted_home_win == actual_home_win:
                correct_predictions += 1

        if m.result == "H":
            s_home, s_away = 1.0, 0.0
        elif m.result == "A":
            s_home, s_away = 0.0, 1.0
        else:
            s_home, s_away = 0.5, 0.5

        diff = m.home_goals - m.away_goals
        k = K_BASE * goal_diff_multiplier(diff)

        r_home_new = r_home + k * (s_home - e_home)
        r_away_new = r_away + k * (s_away - e_away)

        ratings[home] = r_home_new
        ratings[away] = r_away_new

        history_rows.append({
            "match_date": m.match_date, "home_team": home, "away_team": away,
            "home_goals": m.home_goals, "away_goals": m.away_goals,
            "result": m.result,
            "elo_home_before": round(r_home, 1), "elo_away_before": round(r_away, 1),
            "elo_home_after": round(r_home_new, 1), "elo_away_after": round(r_away_new, 1),
            "predicted_prob_home_win": round(e_home, 3),
            "prediction_correct": (predicted_home_win == actual_home_win)
                                   if m.result in ("H", "A") else None,
        })

    accuracy = correct_predictions / total_predictable if total_predictable else 0
    print(f"  ✓ xử lý {len(history_rows):,} trận, "
          f"{len(ratings)} đội, độ chính xác dự đoán thắng/thua: {accuracy:.1%}")

    return pd.DataFrame(history_rows), ratings, accuracy


def build_current_snapshot(ratings: dict) -> pd.DataFrame:
    df = pd.DataFrame(
        [{"team": t, "elo": round(r, 1)} for t, r in ratings.items()]
    ).sort_values("elo", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df


def build_predictions_for_upcoming(ratings: dict, upcoming: list[tuple[str, str]]):
    """
    Dự đoán cho các trận sắp tới (chưa có kết quả).
    upcoming: list các cặp (home_team, away_team) lấy từ fixtures chưa đá (file 01/09).
    """
    rows = []
    for home, away in upcoming:
        r_home = ratings.get(home, START_RATING)
        r_away = ratings.get(away, START_RATING)
        e_home = expected_score(r_home, r_away, HOME_ADV)
        # xấp xỉ xác suất hòa dựa trên khoảng cách ELO (mô hình đơn giản hoá)
        gap = abs(r_home - r_away)
        p_draw = max(0.18, 0.30 - gap / 1500)
        p_home = e_home * (1 - p_draw)
        p_away = (1 - e_home) * (1 - p_draw)
        rows.append({
            "home_team": home, "away_team": away,
            "elo_home": round(r_home, 1), "elo_away": round(r_away, 1),
            "prob_home_win": round(p_home, 3), "prob_draw": round(p_draw, 3),
            "prob_away_win": round(p_away, 3),
        })
    return pd.DataFrame(rows)


def log_run(n_matches: int, n_teams: int, accuracy: float):
    S3.put_object(
        Bucket=BUCKET, Key=f"_meta/elo/run_log/ingest_date={D}.json",
        Body=json.dumps({
            "run_at": datetime.now(timezone.utc).isoformat(),
            "matches_processed": n_matches, "teams": n_teams,
            "prediction_accuracy": round(accuracy, 4),
            "params": {"k_base": K_BASE, "home_advantage": HOME_ADV,
                      "start_rating": START_RATING},
        }).encode(), ContentType="application/json")


if __name__ == "__main__":
    print("[1/4] đọc dữ liệu từ silver (không tải gì từ ngoài)")
    matches = load_matches()
    print(f"  · {len(matches):,} trận từ {matches.match_date.min().date()} "
          f"đến {matches.match_date.max().date()}")

    print("[2/4] chạy thuật toán ELO tuần tự")
    history, ratings, accuracy = run_elo(matches)

    print("[3/4] ghi gold layer")
    put_parquet("gold/analytics/elo_ratings/history/part-0.parquet",
                history, SRC, meta={"matches": len(history)})

    current = build_current_snapshot(ratings)
    put_parquet("gold/analytics/elo_ratings/current/part-0.parquet",
                current, SRC, meta={"teams": len(current)})

    upcoming_example = [("Arsenal", "Chelsea"), ("Liverpool", "Manchester City")]
    preds = build_predictions_for_upcoming(ratings, upcoming_example)
    put_parquet("gold/analytics/elo_ratings/predictions/season=2425/part-0.parquet",
                preds, SRC)

    print("[4/4] ghi log")
    log_run(len(history), len(ratings), accuracy)

    print("\nTop 10 ELO hiện tại:")
    print(current.head(10).to_string(index=False))

    summary("gold/analytics/elo_ratings/")
```

## 5. Kết quả mong đợi

```
[1/4] đọc dữ liệu từ silver (không tải gì từ ngoài)
  · 3,800 trận từ 2015-08-08 đến 2024-05-19
[2/4] chạy thuật toán ELO tuần tự
  ✓ xử lý 3,800 trận, 33 đội, độ chính xác dự đoán thắng/thua: 62.4%
[3/4] ghi gold layer
  ✓ s3://football-lake/gold/analytics/elo_ratings/history/part-0.parquet  (198,441 B, 3800 rows)
  ✓ s3://football-lake/gold/analytics/elo_ratings/current/part-0.parquet  (1,204 B, 33 rows)
  ✓ s3://football-lake/gold/analytics/elo_ratings/predictions/season=2425/part-0.parquet  (441 B, 2 rows)
[4/4] ghi log

Top 10 ELO hiện tại:
          team     elo  rank
 Manchester City  1782.3     1
      Liverpool   1701.5     2
        Arsenal   1688.9     3
        Chelsea   1612.4     4
Tottenham Hotspur 1598.1     5
...

[summary] gold/analytics/elo_ratings/: 3 objects, 0.19 MB
```

**Độ chính xác 62.4%** là con số kiểm chứng quan trọng nhất của cả file này: một mô hình ELO cơ bản cho bóng đá Anh thường đạt 55-65% độ chính xác dự đoán thắng/thua (baseline "luôn đoán đội mạnh hơn thắng" đã ở mức đó). Nếu số của bạn ra quá thấp (\<50%, tệ hơn tung đồng xu) hoặc quá cao (\>75%, đáng ngờ vì có nhìn trước tương lai) thì cần kiểm tra lại thuật toán.

## 6. Truy vấn kiểm chứng

```sql
-- Top 10 đội mạnh nhất theo ELO hiện tại
SELECT rank, team, elo
FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/current/*.parquet')
LIMIT 10;

-- ELO của 1 đội thay đổi qua thời gian (vẽ được biểu đồ line chart)
SELECT match_date,
       CASE WHEN home_team = 'Arsenal' THEN elo_home_after ELSE elo_away_after END AS elo
FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/history/*.parquet')
WHERE home_team = 'Arsenal' OR away_team = 'Arsenal'
ORDER BY match_date;

-- So ELO dự đoán với bảng xếp hạng thật (file 09) — kiểm chứng mô hình có hợp lý không
SELECT c.team, c.elo, c.rank AS elo_rank,
       s.rank AS bang_xh_that, s.points
FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/current/*.parquet') c
JOIN read_parquet('s3://football-lake/silver/standings/fdo_standings/competition=PL/**/*.parquet') s
  ON s.team LIKE '%' || c.team || '%'
ORDER BY c.elo DESC;

-- Trận nào là cú sốc lớn nhất? (đội yếu hơn nhiều thắng đội mạnh)
SELECT match_date, home_team, away_team, home_goals, away_goals,
       ROUND(predicted_prob_home_win, 3) AS xac_suat_du_doan_nha_thang,
       result
FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/history/*.parquet')
WHERE (result = 'A' AND predicted_prob_home_win > 0.75)
   OR (result = 'H' AND predicted_prob_home_win < 0.25)
ORDER BY match_date DESC LIMIT 15;
```

Truy vấn thứ ba là kiểm chứng chéo mạnh nhất: nếu ELO tự tính từ 9 mùa dữ liệu lịch sử **xếp hạng gần giống** bảng xếp hạng PL thật hiện tại (file 09), đó là bằng chứng thuyết phục rằng mô hình gold layer của bạn phản ánh đúng thực tế.

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Độ chính xác ~50% (như tung đồng xu) | `HOME_ADV` hoặc `K_BASE` chưa hợp lý, hoặc dữ liệu bị xáo trộn thứ tự | Đảm bảo `ORDER BY match_date` khi load; thử tăng `HOME_ADV` lên 80-100 |
| Độ chính xác >80% | Rất có thể đang tính expected_score **sau khi** đã update rating (nhìn trước tương lai — data leakage) | Kiểm tra lại: đánh giá dự đoán phải dùng rating *trước* trận, code mẫu đã tách đúng thứ tự |
| ELO đội mới lên hạng rất thấp | Đúng ý đồ — họ khởi động ở 1500 và cần vài trận để điều chỉnh | Có thể thêm "giai đoạn khởi động" 5 trận đầu mùa để rating cũ hạng dưới ảnh hưởng |
| `read_parquet` từ DuckDB lỗi kết nối MinIO | Sai config S3 endpoint | Đối chiếu với `.env`, dùng đúng `s3_endpoint` không có `http://` |
| Trận đấu trùng lặp do nhiều mùa ghi đè | Đọc `**/*.parquet` gộp cả nhiều partition có thể trùng | Group by `(match_date, home_team, away_team)` trước khi chạy ELO nếu nghi ngờ trùng |

## 8. Mở rộng

- Mở rộng ELO sang đa giải: dùng thêm `silver/matches/esd_matches` (file 05) và `fdo_matches` (file 09), gộp theo `TEAM_ALIAS` (đã tạo ở file 03) để có ELO xuyên 11 giải châu Âu.
- Thêm "ELO điều chỉnh xG": thay `home_goals`/`away_goals` bằng `shot_statsbomb_xg` tổng hợp từ file 04 khi có sẵn — phản ánh phong độ thật tốt hơn tỷ số (vốn nhiễu vì may rủi).
- Backtest: chia dữ liệu train/test theo thời gian (train 2015-2022, test 2022-2024), so log-loss của xác suất dự đoán với xác suất ngầm từ odds (`fd_odds`, file 03) — xem mô hình tự viết của bạn so với thị trường cá cược thế nào.
- Lên lịch chạy file này **sau mỗi vòng đấu** (phụ thuộc file 03 hoặc 09 cập nhật xong) → ELO luôn phản ánh phong độ mới nhất, biến gold layer thành thứ "sống" chứ không chỉ tính một lần.
