# Hướng dẫn triển khai One Big Table (OBT)

## 1. Khái niệm

**One Big Table (OBT)** là phương pháp tổ chức dữ liệu phân tích bằng cách **denormalize** nhiều nguồn dữ liệu có liên quan thành một bảng dữ liệu rộng.

Thay vì yêu cầu người dùng phải JOIN nhiều bảng khi phân tích, OBT cung cấp một bảng đã được tích hợp sẵn các thông tin cần thiết.

Trong dự án Football Data Lake, OBT được xây dựng tại **tầng Gold** với mục tiêu tạo ra một **bảng phân tích tổng hợp về trận đấu**.

### Grain của OBT

OBT được thiết kế với:

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
└── Các thông tin liên quan khác
```

OBT tập trung vào việc trả lời:

> **"Một trận đấu có những thông tin gì?"**

---

## 2. Vai trò của OBT trong kiến trúc

OBT là một trong những sản phẩm của tầng **Gold**.

```text
                    SILVER
                       │
          ┌────────────┼────────────┐
          │            │            │
          ▼            ▼            ▼
        OBT         FEATURE      DATA MART
          │            │            │
          │            │       ┌────┼────┬────┐
          │            │       ▼    ▼    ▼    ▼
          │            │     Match Team League Market
          │            │
          ▼            ▼
     Analytics / BI      ML
```

Các thành phần có mục đích khác nhau:

| Thành phần    | Mục đích                                                |
| ------------- | ------------------------------------------------------- |
| **OBT**       | Bảng tổng hợp thông tin về trận đấu                     |
| **Feature**   | Tạo biến đặc trưng phục vụ Machine Learning             |
| **Data Mart** | Dữ liệu chuyên biệt phục vụ BI và từng chủ đề phân tích |

Vì vậy, OBT **không phải là Feature** và cũng **không thay thế Data Mart**.

---

## 3. Dữ liệu đầu vào

OBT được xây dựng từ các dữ liệu đã được chuẩn hóa tại tầng **Silver**.

Các nguồn có thể sử dụng gồm:

```text
Silver
│
├── Matches
├── Teams
├── Odds
├── Understat / Team xG
├── Weather
└── Text / Attention
       │
       ▼
   OBT Match 360
```

Không nên lấy trực tiếp dữ liệu chưa xử lý từ Bronze/Raw để xây OBT.

Silver đóng vai trò là lớp dữ liệu chuẩn hóa chung cho các sản phẩm Gold.

---

## 4. Các nhóm thông tin trong OBT

OBT có thể được chia thành các nhóm cột theo chủ đề.

### 4.1. Thông tin trận đấu

Bao gồm:

* `match_id`
* Ngày thi đấu
* Mùa giải
* Giải đấu
* Đội chủ nhà
* Đội khách
* Sân vận động nếu có dữ liệu

---

### 4.2. Kết quả trận đấu

Bao gồm:

* Bàn thắng đội nhà
* Bàn thắng đội khách
* Kết quả: thắng / hòa / thua
* Tổng số bàn thắng

Nhóm này phục vụ phân tích kết quả thực tế của trận đấu.

---

### 4.3. Thông tin đội bóng

Có thể tích hợp các thông tin mô tả liên quan đến hai đội:

```text
Home Team
├── team_name
├── league
└── các thông tin mô tả có sẵn

Away Team
├── team_name
├── league
└── các thông tin mô tả có sẵn
```

Các thông tin được đưa vào phải dựa trên dữ liệu thực sự có trong Silver.

---

### 4.4. Thông tin thị trường

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

### 4.5. Thông tin bổ sung

Tùy dữ liệu Silver, OBT có thể bổ sung:

* Thời tiết.
* Nhiệt độ.
* Lượng mưa.
* Gió.
* Team xG.
* Chỉ số Attention.

Tuy nhiên, chỉ nên đưa những thông tin **có ý nghĩa ở cấp độ trận đấu**.

---

## 5. Nguyên tắc xây dựng OBT

### Bước 1 — Xác định Grain

Đây là bước quan trọng nhất.

```text
OBT
→ 1 dòng = 1 match_id
```

Mọi dữ liệu được đưa vào OBT phải được xử lý sao cho không phá vỡ Grain này.

---

### Bước 2 — Xác định các nguồn dữ liệu

Xác định những bảng Silver cần thiết:

```text
Matches
Teams
Odds
Weather
Team xG
...
```

Sau đó xác định khóa liên kết giữa chúng:

```text
match_id
team_id
season
league
```

---

### Bước 3 — Chuẩn hóa dữ liệu trước khi JOIN

Các nguồn Silver có thể có Grain khác nhau.

Ví dụ:

```text
Matches
→ 1 dòng / trận

Odds
→ nhiều dòng / trận / nhà cái

Team
→ 1 dòng / đội

Weather
→ có thể 1 dòng / trận
```

Do đó cần xử lý các bảng có nhiều dòng trước khi tích hợp vào OBT.

Ví dụ:

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

Mục tiêu cuối cùng vẫn là:

> **1 match_id → 1 dòng OBT**

---

### Bước 4 — JOIN các nguồn dữ liệu

Sau khi xác định Grain và xử lý dữ liệu có khả năng gây duplicate, các nguồn Silver được kết hợp thành bảng OBT.

Ví dụ về logic:

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

OBT lúc này trở thành một bảng dữ liệu rộng, đã tích hợp các thông tin cần thiết cho việc phân tích trận đấu.

---

## 6. OBT không nên chứa toàn bộ Feature Engineering

Đây là điểm cần phân biệt rõ trong project.

Không nên đưa tất cả các biến được tạo ra trong quá trình Feature Engineering vào OBT chỉ để làm bảng lớn hơn.

Ví dụ các biến như:

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
OBT
→ "Trận đấu này có những thông tin gì?"

Feature
→ "Từ dữ liệu lịch sử, có thể tạo ra những biến nào
   để Machine Learning sử dụng?"
```

---

## 7. Cấu trúc OBT trong tầng Gold

Có thể tổ chức như sau:

```text
gold/
│
├── obt/
│   └── obt_match_360.parquet
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
└── data_mart/
    ├── mart_match.parquet
    ├── mart_team.parquet
    ├── mart_league.parquet
    └── mart_market.parquet
```

Trong đó:

**`obt_match_360.parquet`** là bảng OBT chính của hệ thống.

---

## 8. OBT và Data Mart

OBT và Data Mart có mối quan hệ gần nhau nhưng mục đích khác nhau.

### OBT

```text
obt_match_360
```

Là bảng rộng, tích hợp nhiều thông tin xoay quanh **một trận đấu**.

### Data Mart

```text
mart_match
mart_team
mart_league
mart_market
```

Được chia theo **chủ đề phân tích**.

Có thể hình dung:

```text
                    Silver
                       │
                       ▼
                obt_match_360
                       │
             ┌─────────┼─────────┐
             │         │         │
             ▼         ▼         ▼
        Match Mart  Market Mart  ...
```

Tuy nhiên, Data Mart **không bắt buộc phải được xây trực tiếp từ OBT**. Nó có thể được xây từ Silver hoặc kết hợp OBT với các nguồn Gold khác tùy nhu cầu.

---

## 9. Kết quả

Sau khi triển khai OBT, hệ thống có một bảng:

```text
obt_match_360
```

với đặc điểm:

* **1 dòng = 1 trận đấu**
* Đã tích hợp dữ liệu từ nhiều nguồn Silver.
* Có cấu trúc rộng và dễ truy vấn.
* Giảm nhu cầu JOIN cho người dùng cuối.
* Có thể sử dụng cho phân tích tổng hợp và BI.
* Không chứa toàn bộ Feature Engineering dành riêng cho Machine Learning.

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

OBT vì vậy đóng vai trò là **bảng tổng hợp trung tâm ở tầng Gold cho phân tích trận đấu**, trong khi Feature và Data Mart được xây dựng như các sản phẩm dữ liệu chuyên biệt cho Machine Learning và BI.
