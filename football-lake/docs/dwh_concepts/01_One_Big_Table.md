# Hướng dẫn triển khai One Big Table (OBT)

## 1. Khái niệm

**One Big Table (OBT)** là phương pháp tổ chức dữ liệu phân tích bằng cách **denormalize** nhiều nguồn dữ liệu có liên quan thành một bảng dữ liệu rộng.

Thay vì yêu cầu người dùng phải JOIN nhiều bảng khi phân tích, OBT cung cấp các bảng đã được tích hợp sẵn những thông tin cần thiết cho từng đối tượng phân tích.

Trong dự án Football Data Lake, OBT được xây dựng tại **tầng Gold** với hai góc nhìn chính:

* **Match 360**: bảng phân tích tổng hợp xoay quanh một trận đấu.
* **Player 360**: bảng phân tích tổng hợp xoay quanh một cầu thủ.

Do hai loại OBT có Grain khác nhau nên được triển khai thành **hai bảng riêng biệt**.

---

### 1.1. Match 360

`obt_match_360` được thiết kế với:

> **1 dòng = 1 trận đấu**

Do đó, mỗi `match_id` tương ứng với một bản ghi trong OBT.

Ví dụ:

```text
obt_match_360

1 dòng
│
├── Thông tin trận đấu
├── Thông tin giải đấu
├── Thông tin đội nhà
├── Thông tin đội khách
├── Kết quả trận đấu
├── Thông tin Odds
├── Thông tin thời tiết
├── Team xG
└── Các thông tin liên quan khác
```

Match 360 tập trung vào việc trả lời:

> **"Một trận đấu có những thông tin gì?"**

---

### 1.2. Player 360

`obt_player_360` được thiết kế với:

> **1 dòng = 1 cầu thủ trong một mùa giải / phạm vi phân tích**

Ví dụ:

```text
obt_player_360

1 dòng
│
├── Thông tin cầu thủ
├── Thông tin đội bóng
├── Thông tin giải đấu
├── Mùa giải
├── Số trận thi đấu
├── Số phút thi đấu
├── Bàn thắng
├── Kiến tạo
├── Các chỉ số tấn công
├── Các chỉ số phòng ngự
└── Các chỉ số hiệu suất khác
```

Player 360 tập trung vào việc trả lời:

> **"Một cầu thủ có những thông tin và thành tích gì trong phạm vi phân tích?"**

Player 360 không phải là dữ liệu tracking vị trí theo thời gian thực. Nó là bảng tổng hợp thông tin và thống kê cầu thủ từ các nguồn dữ liệu có sẵn.

---

## 2. Vai trò của OBT trong kiến trúc

OBT là một trong những sản phẩm của tầng **Gold**.

```text
                         SILVER
                            │
             ┌──────────────┼──────────────┐
             │              │              │
             ▼              ▼              ▼
            OBT          FEATURE       DATA MART
             │              │              │
       ┌─────┴─────┐        │       ┌──────┼──────────────┐
       │           │        │       ▼      ▼       ▼      ▼
       ▼           ▼        │     Match   Team   League  Market
  Match 360    Player 360   │
       │           │        │
       └─────┬─────┘        │
             │              │
             ▼              ▼
        Analytics / BI       ML
```

Các thành phần có mục đích khác nhau:

| Thành phần    | Mục đích                                                     |
| ------------- | ------------------------------------------------------------ |
| **OBT**       | Bảng rộng, tích hợp thông tin xoay quanh đối tượng phân tích |
| **Feature**   | Tạo biến đặc trưng phục vụ Machine Learning                  |
| **Data Mart** | Dữ liệu chuyên biệt phục vụ BI và từng chủ đề phân tích      |

OBT gồm hai bảng chính:

| Bảng             | Grain                    | Đối tượng |
| ---------------- | ------------------------ | --------- |
| `obt_match_360`  | 1 dòng / match           | Trận đấu  |
| `obt_player_360` | 1 dòng / player / season | Cầu thủ   |

Vì vậy, OBT **không phải là Feature** và cũng **không thay thế Data Mart**.

---

## 3. Dữ liệu đầu vào

OBT được xây dựng từ các dữ liệu đã được chuẩn hóa tại tầng **Silver**.

Các nguồn có thể sử dụng gồm:

```text
Silver

├── Matches
├── Teams
├── Players
├── Odds
├── Understat / Team xG
├── Player Statistics
├── Weather
└── Text / Attention
       │
       ├──────────────► OBT Match 360
       │
       └──────────────► OBT Player 360
```

Không nên lấy trực tiếp dữ liệu chưa xử lý từ Bronze/Raw để xây OBT.

Silver đóng vai trò là lớp dữ liệu chuẩn hóa chung cho các sản phẩm Gold.

Đối với `obt_player_360`, chỉ sử dụng các nguồn Player Statistics thực sự tồn tại trong Silver. Nếu Silver chưa có dữ liệu cầu thủ tương ứng thì không tự tạo các trường thống kê chưa có nguồn dữ liệu.

---

# 4. Các nhóm thông tin trong OBT

## 4.1. `obt_match_360`

### 4.1.1. Thông tin trận đấu

Bao gồm:

* `match_id`
* Ngày thi đấu
* Mùa giải
* Giải đấu
* Đội chủ nhà
* Đội khách
* Sân vận động nếu có dữ liệu

---

### 4.1.2. Kết quả trận đấu

Bao gồm:

* Bàn thắng đội nhà
* Bàn thắng đội khách
* Kết quả: thắng / hòa / thua
* Tổng số bàn thắng

Nhóm này phục vụ phân tích kết quả thực tế của trận đấu.

---

### 4.1.3. Thông tin đội bóng

Có thể tích hợp các thông tin mô tả liên quan đến hai đội:

```text
Home Team

├── team_id
├── team_name
├── league
└── các thông tin mô tả có sẵn

Away Team

├── team_id
├── team_name
├── league
└── các thông tin mô tả có sẵn
```

Các thông tin được đưa vào phải dựa trên dữ liệu thực sự có trong Silver.

---

### 4.1.4. Thông tin thị trường

Có thể tích hợp các thông tin Odds liên quan đến trận đấu:

* Nhà cái
* Odds đội nhà
* Odds hòa
* Odds đội khách

Nếu một trận đấu có nhiều nhà cái, cần xác định rõ cách xử lý để vẫn đảm bảo Grain của OBT là:

> **1 dòng = 1 trận đấu**

Ví dụ có thể lựa chọn một nhà cái chuẩn hoặc thực hiện bước tổng hợp Odds trước khi đưa vào OBT.

Không nên JOIN trực tiếp dữ liệu có nhiều bản ghi Odds vào OBT nếu việc JOIN làm một `match_id` xuất hiện nhiều dòng.

---

### 4.1.5. Thông tin bổ sung

Tùy dữ liệu Silver, OBT có thể bổ sung:

* Thời tiết
* Nhiệt độ
* Lượng mưa
* Gió
* Team xG
* Chỉ số Attention

Tuy nhiên, chỉ nên đưa những thông tin **có ý nghĩa ở cấp độ trận đấu**.

---

# 5. `obt_player_360`

## 5.1. Grain

Đây là bước quan trọng nhất khi xây dựng Player 360.

Trong phạm vi project:

> **1 dòng = 1 cầu thủ / mùa giải**

Ví dụ:

```text
player_id = P001
season    = 2025/26
```

chỉ xuất hiện một bản ghi trong `obt_player_360`.

Nếu dữ liệu nguồn có nhiều bản ghi cho cùng một cầu thủ, cần tổng hợp trước khi đưa vào OBT.

---

## 5.2. Thông tin cầu thủ

Có thể bao gồm:

* `player_id`
* Tên cầu thủ
* Vị trí
* Quốc tịch nếu có dữ liệu
* Tuổi nếu có dữ liệu

Các thông tin này phụ thuộc vào dữ liệu Player được cung cấp tại Silver.

---

## 5.3. Thông tin đội bóng và giải đấu

Có thể bao gồm:

* `team_id`
* Tên đội bóng
* `league`
* `season`

Ví dụ:

```text
Player

├── player_id
├── player_name
├── position
│
├── Team
│   ├── team_id
│   └── team_name
│
├── League
│   └── league
│
└── Season
    └── season
```

Nếu một cầu thủ thi đấu cho nhiều đội trong cùng mùa giải, cần xác định quy tắc tổng hợp hoặc phân tách dữ liệu trước khi xây dựng OBT.

---

## 5.4. Thống kê thi đấu

Tùy dữ liệu có trong Silver, có thể tích hợp:

* Số lần ra sân
* Số lần đá chính
* Số phút thi đấu
* Bàn thắng
* Kiến tạo
* Số cú sút
* xG
* xA
* Các chỉ số chuyền bóng
* Các chỉ số phòng ngự
* Thẻ vàng
* Thẻ đỏ

Không bắt buộc phải có toàn bộ các trường trên.

Chỉ đưa những trường có nguồn dữ liệu thực tế trong Silver.

---

## 5.5. Ví dụ Player 360

Ở mức khái niệm:

```text
obt_player_360

player_id
player_name
position
team_id
team_name
league
season
appearances
starts
minutes
goals
assists
shots
xg
xa
yellow_cards
red_cards
...
```

Bảng này giúp phân tích:

* Hiệu suất cầu thủ
* So sánh cầu thủ
* Cầu thủ theo đội
* Cầu thủ theo mùa giải
* Thống kê cầu thủ theo giải đấu

---

# 6. Nguyên tắc xây dựng OBT

## Bước 1 — Xác định Grain

Mỗi OBT phải có Grain rõ ràng.

```text
obt_match_360

→ 1 dòng = 1 match_id
```

và:

```text
obt_player_360

→ 1 dòng = 1 player_id / season
```

Mọi dữ liệu được đưa vào OBT phải được xử lý sao cho không phá vỡ Grain tương ứng.

---

## Bước 2 — Xác định các nguồn dữ liệu

Xác định những bảng Silver cần thiết.

Đối với Match 360:

```text
Matches
Teams
Odds
Weather
Team xG
...
```

Đối với Player 360:

```text
Players
Teams
Player Statistics
League
Season
...
```

Sau đó xác định khóa liên kết giữa chúng:

```text
match_id
player_id
team_id
season
league
```

---

## Bước 3 — Chuẩn hóa dữ liệu trước khi JOIN

Các nguồn Silver có thể có Grain khác nhau.

Ví dụ:

```text
Matches

→ 1 dòng / trận

Odds

→ nhiều dòng / trận / nhà cái

Team

→ 1 dòng / đội

Player Statistics

→ nhiều dòng / cầu thủ / trận

Weather

→ có thể 1 dòng / trận
```

Do đó cần xử lý các bảng có nhiều dòng trước khi tích hợp vào OBT.

Ví dụ đối với Odds:

```text
Odds

1001 → Bookmaker A
1001 → Bookmaker B
1001 → Bookmaker C

        ↓

Tổng hợp / lựa chọn

        ↓

OBT

1001 → 1 dòng
```

Tương tự, nếu Player Statistics ở cấp độ trận đấu:

```text
Player Statistics

P001 → Match 1001
P001 → Match 1002
P001 → Match 1003
...

        ↓

Tổng hợp theo player + season

        ↓

obt_player_360

P001 → Season 2025/26 → 1 dòng
```

Mục tiêu cuối cùng vẫn là đảm bảo Grain của từng OBT.

---

# 7. JOIN các nguồn dữ liệu

## 7.1. Match 360

Sau khi xác định Grain và xử lý dữ liệu có khả năng gây duplicate, các nguồn Silver được kết hợp thành bảng OBT.

```text
Matches

   │
   ├──────── Teams (Home)
   │
   ├──────── Teams (Away)
   │
   ├──────── Odds
   │
   ├──────── Weather
   │
   └──────── Team xG
              │
              ▼
        obt_match_360
```

---

## 7.2. Player 360

Player 360 có logic riêng:

```text
Players

   │
   ├──────── Teams
   │
   ├──────── League
   │
   ├──────── Season
   │
   └──────── Player Statistics
              │
              ▼
        Tổng hợp theo
        player + season
              │
              ▼
        obt_player_360
```

OBT lúc này trở thành các bảng dữ liệu rộng, đã tích hợp những thông tin cần thiết cho từng đối tượng phân tích.

---

# 8. OBT không nên chứa toàn bộ Feature Engineering

Đây là điểm cần phân biệt rõ trong project.

Không nên đưa tất cả các biến được tạo ra trong quá trình Feature Engineering vào OBT chỉ để làm bảng lớn hơn.

Đối với Feature dành cho Match/ML, các biến như:

* `pts_l5`
* `xg_l5`
* `elo_diff`
* `pv_ratio_7_28`
* Các biến rolling window
* Các biến phục vụ dự đoán
* `target_*`
* `train / valid / test`

nên thuộc **Feature layer**.

Có thể hiểu đơn giản:

```text
OBT Match 360

→ "Trận đấu này có những thông tin gì?"

OBT Player 360

→ "Cầu thủ này có những thông tin và thành tích gì?"

Feature

→ "Từ dữ liệu lịch sử, có thể tạo ra những biến nào
   để Machine Learning sử dụng?"
```

Đặc biệt, các biến rolling hoặc biến dự đoán có thể phụ thuộc vào thời điểm của trận đấu và quy tắc chống data leakage. Vì vậy không nên mặc định đưa chúng vào OBT.

---

# 9. Cấu trúc OBT trong tầng Gold

Có thể tổ chức như sau:

```text
gold/

│
├── obt/
│   ├── obt_match_360.parquet
│   └── obt_player_360.parquet
│
├── features/
│   ├── feature_team_match.parquet
│   ├── feature_league_position.parquet
│   ├── feature_elo.parquet
│   ├── feature_market.parquet
│   ├── feature_team_xg.parquet
│   ├── feature_team_attention.parquet
│   ├── feature_match_context.parquet
│   └── feature_match_ml.parquet
│
├── data_mart/
│   ├── mart_match.parquet
│   ├── mart_team.parquet
│   ├── mart_player.parquet
│   ├── mart_league.parquet
│   └── mart_market.parquet
│
└── aggregate/
    ├── agg_team_season.parquet
    ├── agg_team_month.parquet
    ├── agg_player_season.parquet
    ├── agg_league_season.parquet
    └── agg_home_advantage.parquet
```

Trong đó:

* `obt_match_360.parquet` là OBT chính ở cấp độ trận đấu.
* `obt_player_360.parquet` là OBT chính ở cấp độ cầu thủ.
* `feature_*` phục vụ Feature Engineering và Machine Learning.
* `mart_*` phục vụ BI và phân tích theo chủ đề.
* `agg_*` là các bảng tổng hợp được tính trước để phục vụ các truy vấn phân tích thường xuyên.

---

# 10. OBT và Data Mart

OBT và Data Mart có mối quan hệ gần nhau nhưng mục đích khác nhau.

## OBT

```text
obt_match_360
obt_player_360
```

Là các bảng rộng, tích hợp nhiều thông tin xoay quanh một đối tượng phân tích.

* Match 360 → xoay quanh trận đấu.
* Player 360 → xoay quanh cầu thủ.

## Data Mart

```text
mart_match
mart_team
mart_player
mart_league
mart_market
```

Được chia theo **chủ đề phân tích**.

Có thể hình dung:

```text
                     Silver
                        │
            ┌───────────┴───────────┐
            │                       │
            ▼                       ▼
      obt_match_360          obt_player_360
            │                       │
            └───────────┬───────────┘
                        │
             ┌──────────┼──────────┐
             ▼          ▼          ▼
        Match Mart  Player Mart  Team Mart
```

Tuy nhiên, Data Mart **không bắt buộc phải được xây trực tiếp từ OBT**.

Nó có thể được xây từ:

* Silver
* OBT
* Các bảng Gold khác
* Hoặc kết hợp nhiều nguồn

tùy theo mục đích của Data Mart.

---

# 11. OBT và Aggregated Tables

OBT và Aggregated Tables cũng có mục đích khác nhau.

### OBT

```text
obt_match_360
obt_player_360
```

Cung cấp dữ liệu tương đối chi tiết ở cấp độ đối tượng.

### Aggregated Tables

```text
agg_team_season
agg_team_month
agg_player_season
agg_league_season
agg_home_advantage
```

Cung cấp dữ liệu đã được tổng hợp trước theo một Grain cụ thể.

Ví dụ:

```text
Player Statistics

P001 → Match 1
P001 → Match 2
P001 → Match 3
...

        ↓

Aggregation

        ↓

agg_player_season

P001 → Season 2025/26
```

Do đó:

> **OBT trả lời "đối tượng này có những thông tin gì?"**

Trong khi:

> **Aggregate trả lời "các chỉ số đã được tổng hợp ở Grain nào để truy vấn nhanh?"**

---

# 12. Kết quả

Sau khi triển khai OBT, hệ thống có hai bảng tổng hợp chính:

```text
obt_match_360
obt_player_360
```

### `obt_match_360`

Đặc điểm:

* **1 dòng = 1 trận đấu**
* Đã tích hợp dữ liệu từ nhiều nguồn Silver.
* Có cấu trúc rộng và dễ truy vấn.
* Giảm nhu cầu JOIN cho người dùng cuối.
* Có thể sử dụng cho phân tích tổng hợp và BI.

Ví dụ ở mức khái niệm:

```text
obt_match_360

match_id
match_date
season
league
home_team
away_team
home_goals
away_goals
result
home_xg
away_xg
home_odds
draw_odds
away_odds
temperature
precipitation
wind
...
```

### `obt_player_360`

Đặc điểm:

* **1 dòng = 1 cầu thủ / mùa giải**
* Tích hợp thông tin cầu thủ, đội bóng, giải đấu và thống kê thi đấu.
* Có cấu trúc rộng và dễ truy vấn.
* Có thể sử dụng cho phân tích hiệu suất và so sánh cầu thủ.
* Không chứa toàn bộ Feature Engineering dành riêng cho Machine Learning.

Ví dụ ở mức khái niệm:

```text
obt_player_360

player_id
player_name
position
team_id
team_name
league
season
appearances
starts
minutes
goals
assists
shots
xg
xa
yellow_cards
red_cards
...
```

OBT vì vậy đóng vai trò là **các bảng tổng hợp trung tâm ở tầng Gold**, cung cấp góc nhìn **Match 360** và **Player 360**.

Trong khi đó:

* **Feature** phục vụ Machine Learning.
* **Data Mart** phục vụ BI và phân tích theo chủ đề.
* **Aggregated Tables** phục vụ các truy vấn tổng hợp thường xuyên và tối ưu hiệu năng.
