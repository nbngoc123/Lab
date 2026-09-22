# Hướng dẫn triển khai Data Marts (Chợ dữ liệu)

## 1. Khái niệm

**Data Mart** là một tập dữ liệu được tổ chức theo **một chủ đề hoặc nhu cầu phân tích cụ thể**, phục vụ trực tiếp cho BI, Dashboard và các hoạt động phân tích dữ liệu.

Trong dự án Football Data Lake, Data Mart được xây dựng ở **tầng Gold**, sau khi dữ liệu đã được thu thập, làm sạch và chuẩn hóa ở tầng Silver.

Thay vì đưa toàn bộ dữ liệu bóng đá vào một bảng duy nhất cho người dùng cuối, dữ liệu được chia thành các Data Mart theo từng nhóm nghiệp vụ:

* **Match Data Mart** → phân tích trận đấu.
* **Team Data Mart** → phân tích phong độ đội bóng.
* **League Data Mart** → phân tích giải đấu và bảng xếp hạng.
* **Market Data Mart** → phân tích tỷ lệ cược và thị trường.

Mỗi Data Mart chỉ chứa những dữ liệu cần thiết cho một nhóm phân tích cụ thể.

---

## 2. Vị trí của Data Mart trong kiến trúc

Data Mart thuộc **tầng Gold**:

```text
                    SILVER
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
      OBT           FEATURE       DATA MART
        │              │              │
        │              │       ┌──────┼──────┬──────┐
        │              │       ▼      ▼      ▼      ▼
        │              │    Match   Team   League  Market
        │              │
        ▼              ▼              ▼
                   END USERS
              BI / Dashboard / ML
```

Silver đóng vai trò là nguồn dữ liệu chuẩn hóa chung. Từ Silver có thể xây dựng các sản phẩm Gold phục vụ những mục đích khác nhau.

---

## 3. Các Data Mart của hệ thống

### 3.1. Match Data Mart

**Mục đích:** phục vụ phân tích kết quả và đặc điểm của từng trận đấu.

Một bản ghi tương ứng với một trận đấu.

Các nhóm dữ liệu chính:

* Thông tin trận đấu.
* Ngày thi đấu.
* Mùa giải.
* Giải đấu.
* Đội chủ nhà.
* Đội khách.
* Bàn thắng đội nhà.
* Bàn thắng đội khách.
* Kết quả trận đấu.
* Tổng số bàn thắng.
* Các chỉ số phục vụ phân tích kết quả.

**Các câu hỏi có thể phân tích:**

* Đội nào ghi nhiều bàn nhất?
* Trận đấu có nhiều bàn thắng nhất?
* Tỷ lệ thắng sân nhà là bao nhiêu?
* Phân bố kết quả thắng, hòa, thua như thế nào?
* Xu hướng số bàn thắng theo mùa giải?

---

### 3.2. Team Data Mart

**Mục đích:** phục vụ phân tích phong độ và hiệu suất của các đội bóng.

Dữ liệu được tổng hợp theo **đội bóng và mùa giải**.

Các nhóm dữ liệu chính:

* Tên đội.
* Mùa giải.
* Số trận.
* Số trận thắng.
* Số trận hòa.
* Số trận thua.
* Điểm số.
* Bàn thắng.
* Bàn thua.
* Hiệu số bàn thắng.
* Tỷ lệ thắng.
* Điểm trung bình mỗi trận.

**Các câu hỏi có thể phân tích:**

* Đội nào có phong độ tốt nhất?
* Đội nào ghi nhiều bàn nhất?
* Đội nào có hàng thủ tốt nhất?
* Phong độ của một đội thay đổi như thế nào qua các mùa?
* So sánh hiệu suất giữa các đội.

---

### 3.3. League Data Mart

**Mục đích:** phục vụ phân tích tổng thể theo giải đấu và bảng xếp hạng.

Dữ liệu được tổ chức theo **giải đấu, mùa giải và đội bóng**.

Các nhóm dữ liệu chính:

* Giải đấu.
* Mùa giải.
* Đội bóng.
* Thứ hạng.
* Số trận.
* Thắng.
* Hòa.
* Thua.
* Điểm.
* Bàn thắng.
* Bàn thua.
* Hiệu số.

**Các câu hỏi có thể phân tích:**

* Bảng xếp hạng của giải đấu.
* Đội dẫn đầu theo từng mùa giải.
* Sự thay đổi thứ hạng của các đội.
* So sánh chất lượng giữa các mùa giải.
* Phân tích số bàn thắng và điểm số của toàn giải.

---

### 3.4. Market Data Mart

**Mục đích:** phục vụ phân tích dữ liệu tỷ lệ cược và thị trường.

Data Mart này tập trung vào dữ liệu Odds thay vì toàn bộ dữ liệu trận đấu.

Các nhóm dữ liệu chính:

* Match ID.
* Ngày thi đấu.
* Đội chủ nhà.
* Đội khách.
* Nhà cái.
* Odds đội nhà.
* Odds hòa.
* Odds đội khách.
* Xác suất ngầm định từ Odds.
* Market Margin.

**Các câu hỏi có thể phân tích:**

* Odds thay đổi như thế nào giữa các nhà cái?
* Thị trường đánh giá khả năng thắng của đội nào cao hơn?
* Nhà cái nào có mức Odds khác biệt?
* Market Margin thay đổi như thế nào?
* So sánh Odds với kết quả thực tế.

---

## 4. Cách xây dựng Data Mart

Quá trình xây dựng Data Mart được thực hiện theo các bước:

### Bước 1 — Xác định chủ đề phân tích

Mỗi Data Mart phải có một mục đích rõ ràng.

```text
Match   → Phân tích trận đấu
Team    → Phân tích đội bóng
League  → Phân tích giải đấu
Market  → Phân tích Odds
```

Không tạo Data Mart chỉ để chia nhỏ dữ liệu một cách máy móc.

---

### Bước 2 — Xác định Grain

Xác định **một dòng dữ liệu đại diện cho điều gì**.

Ví dụ:

```text
mart_match
→ 1 dòng = 1 trận đấu

mart_team
→ 1 dòng = 1 đội trong 1 mùa giải

mart_league
→ 1 dòng = 1 đội trong 1 mùa giải của một giải đấu

mart_market
→ 1 dòng = 1 trận đấu / nhà cái
```

Việc xác định Grain giúp tránh dữ liệu bị trùng hoặc tổng hợp sai.

---

### Bước 3 — Xác định dữ liệu đầu vào

Data Mart sử dụng các dữ liệu đã được chuẩn hóa ở tầng **Silver**.

Ví dụ:

```text
Silver Matches
Silver Odds
Silver Teams
Silver Understat
Silver Weather
Silver Text
        │
        ▼
     Data Mart
```

Không nên lấy trực tiếp dữ liệu Raw/Bronze để xây Data Mart.

---

### Bước 4 — Lọc và kết hợp dữ liệu

Chỉ lấy những trường dữ liệu cần thiết cho từng chủ đề.

Ví dụ:

```text
Silver Matches + Silver Odds
              │
              ▼
       Market Data Mart
```

Trong khi:

```text
Silver Matches
      +
Silver Teams
      +
Silver League
      │
      ▼
     Team / League Data Mart
```

Nhờ vậy Data Mart có cấu trúc nhỏ gọn và tập trung hơn so với OBT.

---

### Bước 5 — Tổng hợp dữ liệu

Tùy từng Data Mart, dữ liệu có thể được tổng hợp ở các mức khác nhau.

Ví dụ:

```text
Match Mart
→ cấp độ trận đấu

Team Mart
→ tổng hợp theo đội + mùa giải

League Mart
→ tổng hợp theo giải đấu + mùa giải + đội

Market Mart
→ cấp độ trận đấu + nhà cái
```

---

### Bước 6 — Materialize Data Mart

Data Mart có thể được triển khai dưới dạng:

* **View** nếu cần dữ liệu động và không muốn tạo thêm bản sao dữ liệu.
* **Parquet** nếu muốn lưu thành các dataset riêng trong Data Lake.
* **Table trong Data Warehouse** nếu hệ thống có một kho dữ liệu phục vụ BI.

Trong kiến trúc hiện tại của project, có thể tổ chức Gold như sau:

```text
gold/
│
├── obt/
│   └── obt_match_360
│
├── features/
│   ├── feature_team_match
│   ├── feature_elo
│   ├── feature_market
│   ├── feature_team_xg
│   └── ...
│
└── data_mart/
    ├── mart_match
    ├── mart_team
    ├── mart_league
    └── mart_market
```

---

## 5. Data Mart và OBT khác nhau như thế nào?

Hai khái niệm này không nên xem là giống nhau.

### OBT

**OBT = One Big Table**

Mục đích là tạo một bảng rộng, tích hợp nhiều thông tin liên quan đến một đối tượng phân tích.

Trong project:

```text
obt_match_360
→ 1 dòng = 1 trận đấu
→ chứa nhiều nhóm thông tin liên quan đến trận đấu
```

OBT phù hợp khi cần nhìn một trận đấu từ nhiều khía cạnh.

---

### Data Mart

Data Mart tập trung vào **một chủ đề phân tích cụ thể**.

```text
mart_match
mart_team
mart_league
mart_market
```

Mỗi Mart chỉ giữ những dữ liệu cần thiết cho chủ đề đó.

Do đó:

```text
OBT
→ rộng, tích hợp nhiều thông tin

Data Mart
→ chuyên biệt, phục vụ từng nhu cầu phân tích
```

---

## 6. Kết quả cuối cùng

Sau khi triển khai, tầng Gold của hệ thống gồm ba nhóm sản phẩm chính:

```text
                         GOLD
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
         OBT            FEATURE          DATA MART
          │                │                │
          │                │       ┌────────┼────────┐
          │                │       │        │        │
          │                │       ▼        ▼        ▼
          │                │    Match     Team    League
          │                │
          │                │                 └── Market
          │                │
          ▼                ▼                ▼
     Phân tích         Machine Learning    BI / Dashboard
     tổng hợp          / Prediction        / Analytics
```

### Cấu trúc Data Mart được chốt

| Data Mart     | Grain                   | Mục đích                    |
| ------------- | ----------------------- | --------------------------- |
| `mart_match`  | 1 trận đấu              | Phân tích trận đấu          |
| `mart_team`   | 1 đội / mùa giải        | Phân tích phong độ đội      |
| `mart_league` | 1 đội / giải / mùa giải | Phân tích giải đấu & BXH    |
| `mart_market` | 1 trận / nhà cái        | Phân tích Odds & thị trường |

Như vậy, **4 Data Mart là đủ cho phạm vi project**, mỗi Mart có một chủ đề rõ ràng và có thể kết nối trực tiếp với BI để xây dựng Dashboard.
