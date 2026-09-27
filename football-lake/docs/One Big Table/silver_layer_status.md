# 📊 Hiện Trạng Dữ Liệu Lớp Silver (Silver Layer Inventory)

Dưới đây là thống kê toàn bộ các bảng Parquet hiện đang có trong lớp **Silver** trên MinIO. Dữ liệu rất phong phú và đã sẵn sàng để Join tạo bảng Gold (OBT - One Big Table).

## ⚽ 1. Trận đấu & Kèo cược (Matches & Odds)

| Dataset | Files | Size (MB) | Ước tính số dòng | Schema (Cột nổi bật) |
|---|---|---|---|---|
| `matches/fd_matches` | 36 | 0.92 | ~ 11,016 | `Div, match_date, home_team, away_team, home_goals, result, HTHG, HS, AS, HST...` (28 cột) |
| `odds/fd_odds` | 36 | 1.08 | ~ 88,128 | `match_id, bookmaker, odds_home, odds_draw, odds_away, implied_margin` (6 cột) |
| `matches/fd_matches_weather` | 4 | 0.36 | ~ 3,040 | Dữ liệu trận đấu kèm thời tiết (34 cột) |
| `matches/understat_match_xg` | 11 | 0.27 | ~ 3,366 | `id, h, a, goals, xG, datetime, forecast...` (10 cột) |
| `events/understat_shots` | 10 | 2.26 | ~ 79,440 | `minute, result, X, Y, xG, player, h_a, shotType...` (Toạ độ cú sút - 22 cột) |
| `matches/fdo_matches` | 6 | 0.08 | ~ 1,836 | API Football-Data (15 cột) |
| `matches/af_fixtures` | 3 | 0.05 | ~ 1,140 | API Football Lịch thi đấu |
| `matches/af_match_events` | 1 | 0.03 | ~ 1,496 | Thẻ phạt, thay người, kiến tạo... |
| `matches/af_match_stats` | 1 | 0.02 | ~ 188 | `shots_on_goal, ball_possession, corner_kicks, yellow_cards...` |
| `matches/af_lineups` | 1 | 0.03 | ~ 3,758 | Đội hình xuất phát (`formation, role, grid...`) |
| `standings/af_standings` | 3 | 0.03 | ~ 60 | Bảng xếp hạng, `points, goal_diff, form...` |

## 🏃 2. Cầu thủ & Đội bóng (Players & Teams)

| Dataset | Files | Size (MB) | Ước tính số dòng | Schema (Cột nổi bật) |
|---|---|---|---|---|
| `teams/understat_team_xg` | 11 | 0.46 | ~ 6,732 | `xG, xGA, npxG, ppda, scored, missed, wins...` (18 cột) |
| `players/understat_player_xg`| 11 | 0.74 | ~ 5,291 | `goals, xG, assists, xA, key_passes, position...` (25 cột) |
| `players/af_injuries` | 3 | 0.09 | ~ 9,504 | `player, type, reason, fixture_date...` |
| `players/pr_player_injuries` | 3 | 0.01 | ~ 186 | Physioroom injuries |
| `players/af_players` | 4 | 0.08 | ~ 80 | `age, height, weight, appearances, minutes, goals...` |
| `players/af_top_scorers` | 3 | 0.02 | ~ 60 | Top ghi bàn |
| `dim/wd_clubs` | 1 | 0.01 | ~ 50 | Thông tin tĩnh từ Wikidata (`capacity, inception...`) |
| `dim/wd_stadiums` | 1 | 0.01 | ~ 20 | Sân vận động (`lon, lat, opened, city...`) |
| `dim/wd_players` | 1 | 0.01 | ~ 36 | Thông tin tĩnh cầu thủ |

## 📰 3. Văn bản & Truyền thông (Text & Media) - Đo độ HOT

| Dataset | Files | Size (MB) | Ước tính số dòng | Schema (Cột nổi bật) |
|---|---|---|---|---|
| `text/wm_pageviews_multilang`| 2 | 17.82 | ~ 532,258 | Lượt xem Wikipedia nhiều ngôn ngữ (18 cột) |
| `text/wm_pageviews` | 2 | 5.41 | ~ 110,842 | `views, views_7d_avg, views_28d_std, zscore...` |
| `text/wm_pageview_spikes` | 1 | 1.64 | ~ 38,323 | Các biến động bất thường của views |
| `text/youtube_comments` | 5 | 4.10 | ~ 75,910 | Bình luận MXH (`text, like_count, published_at...`) |
| `text/youtube_videos` | 5 | 0.14 | ~ 500 | `title, description, channel_title...` |
| `text/google_news_articles` | 3 | 1.33 | ~ 2,016 | Bài báo (`title, summary, source, published...`) |
| `text/wiki_articles` | 3 | 0.61 | ~ 6 | Chiết xuất nội dung Wiki (`extract, word_count...`) |

---
**Nhận xét:**
- Lớp Silver của bạn đang **cực kỳ hoàn thiện**. Nó kết nối đầy đủ các chiều thông tin: Dữ liệu chuyên môn (số liệu sút, kiểm soát bóng, xG, xA), Kèo cược, Sự kiện chấn thương và quan trọng nhất là **Sức nóng Truyền thông** (Views, Comments, News).
- Việc tiếp theo là thiết kế bảng **Gold OBT (One Big Table)** để gom những thông tin này thành bộ feature cho các thuật toán học máy phân tích.
