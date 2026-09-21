# Báo cáo Toàn diện: Hiện trạng Data Lake (Bronze & Silver)

> Báo cáo này được tổng hợp từ việc đọc toàn bộ 13 Airflow DAGs + kiểm tra trực tiếp cây thư mục trên MinIO.  
> Mục đích: Làm cơ sở thiết kế Feature Engineering và Gold Layer.

---

## TÓM TẮT TỔNG QUAN

| Nguồn | DAG | Schedule | Layer | Loại dữ liệu |
|---|---|---|---|---|
| Football-Data.org | p09 | @daily | Bronze + Silver | Matches, Teams, Standings, Competitions |
| API-Football | p24 | @daily | Bronze + Silver | Teams, Standings, Fixtures, Events, Lineups, Top Scorers |
| Understat | p19 | @weekly | Bronze + Silver | xG/xA Player, Team, Match |
| Wikidata (SPARQL) | p06 | @monthly | Bronze + Silver | Clubs, Players, Stadiums (Dim tables) |
| TheSportsDB | p08 | @monthly | Bronze + Silver | Metadata đội bóng, ảnh, sân vận động |
| Wikimedia Pageviews | p10 | @daily | Bronze + Silver | Lượt xem Wikipedia cầu thủ/đội (đa ngôn ngữ) |
| Open-Meteo | p13 | @daily | Bronze + Silver | Thời tiết lịch sử + dự báo tại sân vận động |
| Physioroom | p16 | @daily | Bronze + Silver | Chấn thương cầu thủ, thống kê câu lạc bộ |
| Google News RSS | p20 | @daily | Silver | Tin tức bóng đá (text) |
| Wikipedia Text | p20 | @weekly | Silver | Nội dung bài viết Wikipedia (entity: team, tactic, history) |
| YouTube | p21 | @daily | Bronze + Silver | Video highlights + Comments (đa ngôn ngữ) |
| Odds API | p22 | **@hourly** | Bronze + Silver | Tỷ lệ cược từ nhiều nhà cái |
| Football News (RapidAPI) | p25 | @daily | Silver | Bài báo chuyên sâu về bóng đá |

---

## BRONZE LAYER - Chi tiết từng nguồn

### 1. `bronze/football_data_org/` (p09)
| Dataset | Path Pattern | Nội dung |
|---|---|---|
| Competitions | `.../competitions/ingest_date=*/competitions.json.gz` | Danh sách giải đấu |
| Matches | `.../matches/competition={PL,PD,BL1,SA,FL1,CL}/season=*/ingest_date=*/` | Lịch sử + lịch thi đấu |
| Teams | `.../teams/competition=*/ingest_date=*/` | Thông tin đội bóng + squad |
| Standings | `.../standings/competition=*/ingest_date=*/` | Bảng xếp hạng hiện tại |

### 2. `bronze/api_football/` (p24)
| Dataset | Path Pattern | Nội dung |
|---|---|---|
| Teams | `.../teams/season=2024/teams.json.gz` | Chi tiết đội bóng |
| Standings | `.../standings/ingest_date=*/standings.json.gz` | Bảng xếp hạng |
| Top Scorers | `.../top_scorers/season=*/ingest_date=*/` | Vua phá lưới |
| Fixtures, Events, Lineups | *(đang thu thập)* | Đội hình, sự kiện trận đấu |

### 3. `bronze/understat/` (p19)
Dữ liệu xG/xA theo giải đấu: EPL, La_Liga, Bundesliga, Serie_A, Ligue_1.
| Dataset | Nội dung |
|---|---|
| Players | xG, xA, minutes, shots của từng cầu thủ |
| Teams | xG, xGA tổng hợp theo đội |
| Matches | xG from, xG against từng trận |

### 4. `bronze/wikimedia_pageviews/` (p10)
- Path: `bronze/wikimedia_pageviews/per_article/entity={player|team}/lang={en,es,fr,pt,de,...}/article={tên}/ingest_date=*/daily.json.gz`
- Nội dung: Lượt xem Wikipedia theo ngày, theo ngôn ngữ của từng cầu thủ/đội bóng.

### 5. `bronze/youtube/` (p21)
| Dataset | Path Pattern |
|---|---|
| Videos | `bronze/youtube/videos/query={Arsenal_highlight, Real_Madrid_highlight,...}` |
| Comments | `bronze/youtube/comments/video_id={id}/` |

### 6. Physioroom, Open-Meteo, Wikidata, TheSportsDB
- Physioroom: HTML thô của trang web chấn thương, được parse sang Bronze.
- Open-Meteo: JSON thời tiết theo tọa độ sân vận động (nhiệt độ, độ ẩm, gió, mưa).
- Wikidata: SPARQL query kết quả: clubs, players, stadiums dạng Linked Data.
- TheSportsDB: Binary/media metadata của đội bóng.

---

## SILVER LAYER - Chi tiết từng dataset

### Nhóm: Dim (Dimension - Bảng danh mục)
| Dataset | Path | Nội dung |
|---|---|---|
| fdo_teams | `silver/dim/fdo_teams/competition=*/ingest_date=*/` | Đội bóng chuẩn hóa |
| fdo_players | `silver/dim/fdo_players/` | Cầu thủ chuẩn hóa |
| fdo_competitions | `silver/dim/fdo_competitions/` | Giải đấu |
| wikidata_clubs | `silver/dim/wikidata_clubs/` | Clubs từ Wikidata SPARQL |
| wikidata_players | `silver/dim/wikidata_players/` | Players từ Wikidata SPARQL |
| wikidata_stadiums | `silver/dim/wikidata_stadiums/` | Stadiums với tọa độ GPS |

### Nhóm: Matches (Bảng Sự kiện Trận đấu)
| Dataset | Path | Nội dung |
|---|---|---|
| fdo_matches | `silver/matches/fdo_matches/competition=*/` | Kết quả trận đấu (p09) |
| understat_match_xg | `silver/matches/understat_match_xg/league=*/season=*/` | xG từng trận (p19) |
| weather_at_match | `silver/weather/weather_at_match/` | Thời tiết join với trận đấu (p13) |

### Nhóm: Players (Thống kê Cầu thủ)
| Dataset | Path | Nội dung |
|---|---|---|
| understat_player_xg | `silver/players/understat_player_xg/league=*/season=*/` | xG, xA, goals, assists, minutes |

### Nhóm: Teams (Thống kê Đội bóng)
| Dataset | Path | Nội dung |
|---|---|---|
| understat_team_xg | `silver/teams/understat_team_xg/league={EPL,La_Liga,Bundesliga,Serie_A,Ligue_1}/season=2025/` | xG for/against, tỷ lệ thắng |
| fdo_standings | `silver/standings/fdo_standings/competition=*/ingest_date=*/` | Bảng xếp hạng lịch sử |

### Nhóm: Text / NLP (Văn bản)
| Dataset | Path | Nội dung |
|---|---|---|
| google_news_articles | `silver/text/google_news_articles/ingest_date=*/part-0.parquet` | Tiêu đề + nội dung tin tức Google News |
| wiki_articles | `silver/text/wiki_articles/entity={team,tactic,history}/fetched_date=*/` | Bài viết Wikipedia |
| wm_pageviews | `silver/text/wm_pageviews/entity={player,team}/part-0.parquet` | Tổng hợp lượt xem Wikimedia |
| wm_pageviews_multilang | `silver/text/wm_pageviews_multilang/entity={player,team}/` | Lượt xem theo ngôn ngữ |
| wm_pageview_spikes | `silver/text/wm_pageview_spikes/part-0.parquet` | Phát hiện các đợt tăng đột biến lượt xem |
| youtube_videos | `silver/text/youtube_videos/ingest_date=*/part-0.parquet` | Metadata video highlight |
| youtube_comments | `silver/text/youtube_comments/ingest_date=*/part-0.parquet` | Bình luận YouTube (raw) |

---

## GỢI Ý FEATURE ENGINEERING - Kế hoạch Join

Dựa trên toàn bộ hiện trạng trên, đây là lộ trình join để tạo ra bảng Feature Table phục vụ Gold Layer:

```
                       ┌─────────────────────────────────────────┐
                       │       FACT: fdo_matches (trận đấu)      │
                       │  match_id | home | away | date | result  │
                       └────────────────┬────────────────────────┘
                                        │
         ┌──────────────────────────────┼─────────────────────────────┐
         │                              │                             │
         ▼                              ▼                             ▼
  understat_match_xg           weather_at_match              fdo_standings
  (xG for/against)             (nhiệt độ, mưa, gió)         (điểm số trước trận)
         │
         ├── understat_team_xg          ├── wm_pageview_spikes        ├── understat_player_xg
         │   (phong độ xG đội)          │   (độ hot cầu thủ)          │   (form cá nhân)
         │
         └── google_news_articles       └── physioroom                └── odds (p22)
             (sentiment score)              (chấn thương đội)             (niềm tin nhà cái)
```

**Output Gold Table:** `obt_match_features.parquet`  
**Mỗi dòng = 1 trận đấu**, với đầy đủ các cột đặc trưng từ tất cả nguồn trên.
