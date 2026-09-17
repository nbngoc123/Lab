Dưới đây là shortlist những nguồn **thực sự làm được** (miễn phí hoặc free tier đủ dùng, không cần thủ tục phức tạp), mỗi loại 2–3 cái.

## 1. API — chọn 3

**Fantasy Premier League API** (khuyên dùng nhất)
- Endpoint: `https://fantasy.premierleague.com/api/bootstrap-static/`, `/fixtures/`, `/element-summary/{id}/`
- Không cần API key, không rate limit khắt khe, JSON sạch
- Có: toàn bộ cầu thủ PL, giá, điểm, phút thi đấu, goals/assists, clean sheet, fixtures cả mùa

**TheSportsDB**
- Free tier với test key, JSON REST
- Có: teams, players, leagues, logo, lịch/kết quả nhiều giải (không chỉ PL)
- Dùng tốt để làm **dimension table** (đội, sân, cầu thủ) bổ sung cho FPL

**API-Football** (bạn đã có trong danh sách)
- Free plan ~100 request/ngày — đủ để chạy job ingest 1 lần/ngày
- Mạnh nhất về events, lineups, statistics chi tiết từng trận

## 2. Flat file — chọn 2

**StatsBomb Open Data** (GitHub: `statsbomb/open-data`)
- JSON event-level: từng đường chuyền, cú sút, tọa độ x/y, xG
- Clone repo về là có luôn vài GB dữ liệu, không cần key
- Hạn chế: chỉ một số giải/mùa được mở (World Cup, La Liga Messi, FA WSL...) — nhưng cực tốt để demo xử lý nested JSON

**Football-Data.co.uk** (bạn đã có)
- CSV theo mùa, link cố định dạng `https://www.football-data.co.uk/mmz4281/2425/E0.csv`
- Dễ viết loop tải 20+ mùa → nguồn lý tưởng cho batch ingest lịch sử

## 3. Database — chọn 2

**European Soccer Database (Kaggle)**
- File SQLite ~300MB, 25k trận, 11 giải châu Âu 2008–2016
- Có sẵn quan hệ bảng → minh họa tốt việc ingest từ RDBMS vào lake

**Wikidata SPARQL endpoint**
- `https://query.wikidata.org/sparql`, trả JSON/CSV
- Query được: cầu thủ PL, quốc tịch, ngày sinh, CLB, sân vận động, tọa độ địa lý
- Dùng làm **enrichment layer** cho dimension cầu thủ/CLB

## 4. Text / Unstructured — chọn 2

**Reddit API** (r/soccer, r/PremierLeague)
- Free tier vẫn dùng được cho khối lượng nhỏ (cần đăng ký app, OAuth)
- Lấy post + comment → phân tích sentiment theo trận/cầu thủ

**Wikipedia**
- API `https://en.wikipedia.org/w/api.php` hoặc dump XML
- Text tiểu sử cầu thủ, lịch sử CLB → dữ liệu text dài, hợp để demo NLP

> Lưu ý: **Twitter/X API giờ đã thu phí**, free tier gần như không đọc được tweet. Nên thay bằng Reddit nếu bạn cần social data.

## 5. Streaming — chọn 1 (thực tế nhất)

**Giả lập stream từ API polling**
- Không dùng WebSocket không chính thức (dễ bị chặn, rủi ro ToS). Thay vào đó:
- Dùng API-Football endpoint `fixtures?live=all` poll mỗi 30–60 giây, đẩy vào Kafka/Kinesis
- Hiệu quả tương đương near-real-time và hoàn toàn hợp lệ

## 6. Multimedia — chọn 1

**Logo/ảnh từ TheSportsDB**
- Mỗi team/player record có sẵn URL ảnh (`strTeamBadge`, `strThumb`)
- Tải về lưu object storage → minh họa lake chứa binary, kèm metadata JSON

---

## Bộ tối thiểu mình đề xuất nếu bạn muốn gọn

| Loại | Nguồn | Vai trò trong lake |
|---|---|---|
| API (no-key) | FPL API | Fact: player performance theo gameweek |
| API (có key) | API-Football | Fact: match events, lineups, live |
| Flat file CSV | Football-Data.co.uk | Fact: lịch sử 20+ mùa + odds |
| Flat file JSON nested | StatsBomb Open Data | Fact: event-level chi tiết |
| Database | European Soccer DB (SQLite) | Nguồn RDBMS, cross-league |
| Semantic DB | Wikidata SPARQL | Dimension enrichment |
| Text | Reddit + RSS | Sentiment / news |
| Binary | TheSportsDB images | Object storage demo |

Bộ này phủ đủ **8 kiểu ingest khác nhau** (REST no-auth, REST có key, CSV batch, nested JSON, SQLite, SPARQL, OAuth API, RSS, binary) — rất mạnh khi trình bày kiến trúc Data Lake.

Bạn muốn mình viết luôn **script ingest mẫu** cho vài nguồn trong bảng này không?
