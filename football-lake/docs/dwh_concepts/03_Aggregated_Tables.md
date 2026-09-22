# Hướng dẫn triển khai Aggregated Tables (Bảng tổng hợp)

## 1. Khái niệm

**Aggregated Tables** (Bảng tổng hợp) là các bảng dữ liệu đã được **tính toán và gom nhóm trước** theo một hoặc nhiều chiều phân tích.

Thay vì mỗi lần truy vấn phải tính toán lại trên toàn bộ dữ liệu chi tiết, các phép tính như:

* `COUNT`
* `SUM`
* `AVG`
* `MIN`
* `MAX`
* `GROUP BY`

được thực hiện trước và lưu thành bảng tổng hợp.

Trong dự án Football Data Lake, Aggregated Tables được sử dụng chủ yếu để **tăng tốc các truy vấn phân tích và Dashboard**.

Ví dụ:

```text
Dữ liệu trận đấu chi tiết
        │
        │ GROUP BY team + season
        ▼
agg_team_season
```

Thay vì phải xử lý hàng chục nghìn trận đấu mỗi lần mở Dashboard, BI có thể đọc trực tiếp dữ liệu đã được tổng hợp.

---

## 2. Vai trò của Aggregated Tables

Aggregated Tables **không thay thế OBT, Feature hoặc Data Mart**.

Mỗi thành phần có mục đích riêng:

| Thành phần            | Mục đích                                                          |
| --------------------- | ----------------------------------------------------------------- |
| **OBT**               | Tích hợp thông tin chi tiết về trận đấu                           |
| **Feature**           | Tạo các biến đặc trưng phục vụ Machine Learning                   |
| **Data Mart**         | Tổ chức dữ liệu theo chủ đề phân tích                             |
| **Aggregated Tables** | Tổng hợp trước dữ liệu để phục vụ truy vấn và Dashboard nhanh hơn |

Có thể hình dung:

```text
Silver
  │
  ▼
Gold
  │
  ├── OBT
  │
  ├── Feature
  │
  └── Data Mart
          │
          ▼
    Aggregated Tables
          │
          ▼
      BI / Dashboard
```

Aggregated Tables có thể được xây dựng từ **OBT hoặc Data Mart**, tùy theo nhu cầu phân tích.

---

## 3. Khi nào cần sử dụng Aggregated Tables?

Không phải dữ liệu nào cũng cần tạo bảng tổng hợp.

Aggregated Tables phù hợp khi:

* Dashboard thường xuyên sử dụng cùng một phép tính.
* Dữ liệu chi tiết có kích thước lớn.
* Các truy vấn `GROUP BY` được thực hiện nhiều lần.
* Muốn giảm thời gian xử lý khi người dùng lọc dữ liệu.
* Có các chỉ số thống kê được sử dụng lặp lại.

Ví dụ:

```text
Dashboard Team Performance
        │
        ▼
Cần liên tục tính:
- số trận
- thắng
- hòa
- thua
- bàn thắng
- điểm
        │
        ▼
Tạo trước:
agg_team_season
```

---

## 4. Xác định Grain của Aggregated Table

Trước khi xây dựng bảng tổng hợp cần xác định rõ **một dòng dữ liệu đại diện cho điều gì**.

Ví dụ:

### `agg_team_season`

```text
1 dòng = 1 đội + 1 mùa giải
```

### `agg_team_month`

```text
1 dòng = 1 đội + 1 tháng
```

### `agg_league_season`

```text
1 dòng = 1 giải đấu + 1 mùa giải
```

### `agg_home_advantage`

```text
1 dòng = 1 giải đấu + 1 mùa giải
```

Việc xác định Grain giúp tránh việc tính toán hoặc JOIN sai dữ liệu.

---

## 5. Các Aggregated Tables đề xuất

Đối với project Football Data Lake, không cần tạo quá nhiều bảng tổng hợp.

Có thể triển khai một số bảng chính sau:

### 5.1. Team Season Aggregate

**Tên:**

```text
agg_team_season
```

**Grain:**

```text
1 dòng = 1 đội + 1 mùa giải
```

Các chỉ số:

* Số trận.
* Số trận thắng.
* Số trận hòa.
* Số trận thua.
* Bàn thắng.
* Bàn thua.
* Hiệu số.
* Điểm.
* Tỷ lệ thắng.
* Điểm trung bình mỗi trận.

Ví dụ:

```text
team     season    matches    wins    draws    losses    points
Arsenal  2024      38         28      5        5         89
```

Bảng này phù hợp cho Dashboard **Team Performance**.

---

### 5.2. Team Monthly Aggregate

**Tên:**

```text
agg_team_month
```

**Grain:**

```text
1 dòng = 1 đội + 1 tháng
```

Các chỉ số:

* Số trận trong tháng.
* Số trận thắng.
* Số trận hòa.
* Số trận thua.
* Bàn thắng.
* Bàn thua.
* Điểm.
* Tỷ lệ thắng.

Ví dụ:

```text
team      year    month    matches    goals    points
Arsenal   2024    12       5          12       12
```

Bảng này phục vụ phân tích **xu hướng phong độ theo thời gian**.

---

### 5.3. League Season Aggregate

**Tên:**

```text
agg_league_season
```

**Grain:**

```text
1 dòng = 1 giải đấu + 1 mùa giải
```

Các chỉ số:

* Tổng số trận.
* Tổng số bàn thắng.
* Số bàn thắng trung bình mỗi trận.
* Số trận thắng sân nhà.
* Số trận hòa.
* Số trận thắng sân khách.
* Tỷ lệ thắng sân nhà.
* Tỷ lệ hòa.
* Tỷ lệ thắng sân khách.

Ví dụ:

```text
league           season    matches    total_goals    avg_goals
Premier League   2024      380        1150           3.03
```

Bảng này phục vụ Dashboard **League Overview**.

---

### 5.4. Home Advantage Aggregate

**Tên:**

```text
agg_home_advantage
```

**Grain:**

```text
1 dòng = 1 giải đấu + 1 mùa giải
```

Các chỉ số:

* Tỷ lệ thắng sân nhà.
* Tỷ lệ hòa.
* Tỷ lệ thắng sân khách.
* Bàn thắng trung bình đội nhà.
* Bàn thắng trung bình đội khách.

Ví dụ:

```text
league           season    home_win_rate    draw_rate    away_win_rate
Premier League   2024      0.47             0.25         0.28
```

Bảng này phục vụ phân tích **Home Advantage**.

---

## 6. Quy trình xây dựng Aggregated Tables

### Bước 1 — Xác định nhu cầu phân tích

Xác định Dashboard hoặc báo cáo nào cần dữ liệu tổng hợp.

Ví dụ:

```text
Team Performance
        ↓
agg_team_season
```

hoặc:

```text
League Overview
        ↓
agg_league_season
```

---

### Bước 2 — Xác định nguồn dữ liệu

Có thể sử dụng:

```text
OBT
 │
 ├── Match information
 ├── Team information
 ├── Result
 ├── Odds
 └── Weather
```

hoặc sử dụng Data Mart:

```text
Data Mart
 │
 ├── mart_match
 ├── mart_team
 ├── mart_league
 └── mart_market
```

Tùy trường hợp, không nhất thiết phải xây Aggregate trực tiếp từ Silver.

---

### Bước 3 — Xác định Grain

Ví dụ:

```text
agg_team_season
→ team + season

agg_team_month
→ team + year + month

agg_league_season
→ league + season
```

Đây là bước quan trọng để đảm bảo kết quả tổng hợp chính xác.

---

### Bước 4 — Thực hiện phép tổng hợp

Dữ liệu được tính toán bằng các phép:

```text
GROUP BY
COUNT
SUM
AVG
MIN
MAX
```

Ví dụ về logic:

```text
OBT
100.000 dòng
     │
     │ GROUP BY team + season
     ▼
agg_team_season
2.000 dòng
```

Kết quả được lưu lại để có thể sử dụng nhiều lần.

---

### Bước 5 — Lưu Aggregated Tables

Các bảng tổng hợp có thể được lưu dưới dạng:

* Parquet trong Data Lake.
* Table trong DuckDB.
* Table trong Data Warehouse.

Trong kiến trúc hiện tại, có thể tổ chức:

```text
gold/
│
├── obt/
│   └── obt_match_360.parquet
│
├── features/
│   └── ...
│
├── data_mart/
│   ├── mart_match.parquet
│   ├── mart_team.parquet
│   ├── mart_league.parquet
│   └── mart_market.parquet
│
└── aggregate/
    ├── agg_team_season.parquet
    ├── agg_team_month.parquet
    ├── agg_league_season.parquet
    └── agg_home_advantage.parquet
```

---

## 7. Aggregated Tables và Data Mart

Hai khái niệm này có thể có sự giao nhau nhưng không hoàn toàn giống nhau.

**Data Mart** trả lời:

> "Tôi muốn phân tích chủ đề nào?"

Ví dụ:

```text
mart_team
→ Phân tích đội bóng
```

**Aggregated Table** trả lời:

> "Tôi muốn dữ liệu đã được tổng hợp ở mức nào để truy vấn nhanh?"

Ví dụ:

```text
agg_team_season
→ Đội bóng + mùa giải
```

Do đó:

```text
Data Mart
→ Subject-oriented

Aggregate
→ Pre-aggregated
```

Một Data Mart có thể chứa dữ liệu chi tiết, còn một Data Mart cũng có thể sử dụng các Aggregated Tables để tăng tốc Dashboard.

---

## 8. Kết quả

Sau khi triển khai, hệ thống có thể cung cấp các bảng tổng hợp:

```text
aggregate/
│
├── agg_team_season
├── agg_team_month
├── agg_league_season
└── agg_home_advantage
```

Các bảng này có số lượng bản ghi nhỏ hơn đáng kể so với dữ liệu trận đấu chi tiết và chứa các chỉ số đã được tính toán trước.

Ví dụ:

```text
Dữ liệu chi tiết
100.000 trận
        │
        ▼
GROUP BY team + season
        │
        ▼
agg_team_season
2.000 dòng
        │
        ▼
Power BI
```

Nhờ đó, Dashboard có thể đọc trực tiếp các chỉ số đã được tổng hợp thay vì phải thực hiện lại các phép tính trên toàn bộ dữ liệu chi tiết mỗi lần truy vấn.

---

## 9. Cấu trúc Gold hoàn chỉnh

Cuối cùng, tầng Gold của project có thể được tổ chức như sau:

```text
GOLD
│
├── OBT
│   └── obt_match_360
│
├── FEATURE
│   ├── feature_team_match
│   ├── feature_league_position
│   ├── feature_elo
│   ├── feature_market
│   ├── feature_team_xg
│   ├── feature_team_attention
│   ├── feature_match_context
│   └── feature_match_ml
│
├── DATA MART
│   ├── mart_match
│   ├── mart_team
│   ├── mart_league
│   └── mart_market
│
└── AGGREGATE
    ├── agg_team_season
    ├── agg_team_month
    ├── agg_league_season
    └── agg_home_advantage
```

### Vai trò của từng thành phần

```text
OBT
→ Tổng hợp thông tin về từng trận đấu

FEATURE
→ Tạo đặc trưng cho Machine Learning

DATA MART
→ Phân chia dữ liệu theo chủ đề phân tích

AGGREGATE
→ Tính toán trước các chỉ số để tối ưu BI / Dashboard
```

Như vậy, **Aggregated Tables là lớp tối ưu hóa dữ liệu phân tích**, không phải một phiên bản khác của OBT và cũng không phải thay thế cho Data Mart.
