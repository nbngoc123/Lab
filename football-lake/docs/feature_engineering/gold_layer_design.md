# Thiết kế Gold Layer Data Warehouse
## Từ Silver → OBT / Feature / Agg / Mart

---

## KIẾN TRÚC TỔNG QUAN

```
SILVER LAYER                          GOLD LAYER
─────────────────────────────────────────────────────────────────────
silver/matches/fdo_matches        ─┐
silver/standings/fdo_standings    ─┤
silver/players/understat_player   ─┼──► OBT_match_master.parquet
silver/teams/understat_team_xg    ─┤
silver/weather/weather_at_match   ─┤
silver/betting/                   ─┘

OBT_match_master                  ─┬──► Feature_match_ml.parquet
silver/text/wm_pageview_spikes    ─┤    (dùng window functions)
silver/text/google_news_articles  ─┘

Feature_match_ml                  ──── ► Agg_team_season.parquet
                                         Agg_league_weekly.parquet

Agg_*                             ─┬──► Mart_betting_analysis
Feature_*                         ─┤    Mart_match_prediction
                                   └──► Mart_team_performance
```

---

## 1. OBT – One Big Table (Bảng gốc phẳng)
> **Mỗi dòng = 1 trận đấu. Gom tất cả Silver vào.**

**File:** `gold/obt/obt_match_master.parquet`

| Cột | Nguồn Silver | Mô tả |
|---|---|---|
| `match_id` | fdo_matches | ID trận đấu |
| `match_date` | fdo_matches | Ngày thi đấu |
| `competition` | fdo_matches | Giải đấu (PL, PD, BL1...) |
| `season` | fdo_matches | Mùa giải |
| `matchday` | fdo_matches | Vòng đấu |
| `home_team_id` | fdo_matches | ID đội nhà |
| `home_team_name` | fdo_matches | Tên đội nhà |
| `away_team_id` | fdo_matches | ID đội khách |
| `away_team_name` | fdo_matches | Tên đội khách |
| `home_goals` | fdo_matches | Bàn thắng đội nhà |
| `away_goals` | fdo_matches | Bàn thắng đội khách |
| `result` | fdo_matches | H/D/A (home/draw/away) |
| `home_xg` | understat_match_xg | xG đội nhà |
| `away_xg` | understat_match_xg | xG đội khách |
| `home_rank` | fdo_standings | Thứ hạng đội nhà trước trận |
| `away_rank` | fdo_standings | Thứ hạng đội khách trước trận |
| `home_points` | fdo_standings | Điểm đội nhà trước trận |
| `away_points` | fdo_standings | Điểm đội khách trước trận |
| `temperature` | weather_at_match | Nhiệt độ lúc đá (°C) |
| `precipitation` | weather_at_match | Lượng mưa (mm) |
| `wind_speed` | weather_at_match | Tốc độ gió (km/h) |
| `odds_home_win` | silver/betting | Tỷ lệ cược đội nhà thắng |
| `odds_draw` | silver/betting | Tỷ lệ cược hòa |
| `odds_away_win` | silver/betting | Tỷ lệ cược đội khách thắng |

---

## 2. FEATURE TABLE – Bảng đặc trưng ML
> **Tính toán từ OBT + Window Functions. Dùng để train model.**

**File:** `gold/features/feature_match_ml.parquet`

| Feature | Công thức | Ý nghĩa |
|---|---|---|
| `home_win_rate_5` | `AVG(is_home_win) OVER last 5 matches` | Tỷ lệ thắng 5 trận gần nhất (đội nhà) |
| `away_win_rate_5` | `AVG(is_away_win) OVER last 5 matches` | Tỷ lệ thắng 5 trận gần nhất (đội khách) |
| `home_goals_avg_5` | `AVG(home_goals) OVER last 5` | Trung bình bàn thắng 5 trận gần nhất |
| `away_goals_avg_5` | `AVG(away_goals) OVER last 5` | Trung bình bàn thắng 5 trận gần nhất |
| `home_xg_avg_5` | `AVG(home_xg) OVER last 5` | xG trung bình 5 trận (đội nhà) |
| `away_xg_avg_5` | `AVG(away_xg) OVER last 5` | xG trung bình 5 trận (đội khách) |
| `home_xg_overperform` | `home_goals - home_xg` | Đang "may mắn" hay thực lực? |
| `rank_diff` | `away_rank - home_rank` | Chênh lệch thứ hạng |
| `points_diff` | `home_points - away_points` | Chênh lệch điểm số |
| `is_home_top6` | `home_rank <= 6` | Đội nhà thuộc top 6? |
| `odds_implied_home` | `1 / odds_home_win` | Xác suất ngầm định đội nhà thắng |
| `odds_margin` | `sum(1/odds) - 1` | Lợi nhuận nhà cái (market efficiency) |
| `home_pageview_spike` | wm_pageview_spikes | Đội nhà đang hot trên Wikipedia? |
| `away_pageview_spike` | wm_pageview_spikes | Đội khách đang hot? |
| `home_news_sentiment` | google_news_articles | Tâm lý báo chí đội nhà (NLP) |
| `is_raining` | precipitation > 2mm | Trời mưa khi đá? |
| `days_since_last_match_home` | `match_date - prev_match_date` | Số ngày nghỉ của đội nhà |
| `days_since_last_match_away` | `match_date - prev_match_date` | Số ngày nghỉ của đội khách |
| `target` | `result` | **LABEL: H/D/A** (cột cần dự đoán) |

---

## 3. AGG TABLE – Bảng tổng hợp sẵn
> **Pre-aggregate để Dashboard load tức thì.**

### 3a. Tổng kết đội bóng theo mùa
**File:** `gold/agg/agg_team_season.parquet`

| Cột | Mô tả |
|---|---|
| `team_name`, `competition`, `season` | Khóa nhóm |
| `total_matches`, `wins`, `draws`, `losses` | Tổng kết |
| `goals_scored`, `goals_conceded`, `goal_diff` | Tấn công/thủ |
| `avg_xg_for`, `avg_xg_against` | xG trung bình |
| `points`, `final_rank` | Kết quả mùa giải |

### 3b. Tổng kết theo tuần (cho dashboard real-time)
**File:** `gold/agg/agg_league_weekly.parquet`

| Cột | Mô tả |
|---|---|
| `competition`, `year_week` | Khóa nhóm |
| `total_matches`, `avg_goals_per_match` | Thống kê vòng đấu |
| `most_goals_team`, `avg_odds_home` | Đội ghi nhiều nhất |

---

## 4. DATA MART – Chợ dữ liệu theo mục đích

### Mart A: Dự đoán kết quả trận đấu (ML)
**File:** `gold/mart/mart_match_prediction.parquet`

Lấy `feature_match_ml` → Lọc chỉ những trận **đã có kết quả** (FINISHED).
Input cho model: Random Forest, XGBoost, LightGBM.

### Mart B: Phân tích cá cược (Betting Analytics)
**File:** `gold/mart/mart_betting_analysis.parquet`

Lấy OBT → Chỉ lấy các cột Odds + Result + xG.
Phục vụ: Phân tích value bet, so sánh odds với xác suất thực tế.

### Mart C: Hiệu suất đội bóng (Team Performance)
**File:** `gold/mart/mart_team_performance.parquet`

Lấy `agg_team_season` → Chỉ export các cột phân tích phong độ.
Phục vụ: Power BI dashboard "Tổng quan đội bóng".

---

## LỘ TRÌNH TRIỂN KHAI

```
Bước 1: build_obt.py         → Tạo OBT từ Silver (JOIN)
Bước 2: build_features.py    → Tạo Feature Table từ OBT (Window Functions)
Bước 3: build_agg.py         → Tạo Agg Tables từ OBT (GROUP BY)
Bước 4: build_marts.py       → Tạo Data Marts từ Feature + Agg (Filter/Select)
```
