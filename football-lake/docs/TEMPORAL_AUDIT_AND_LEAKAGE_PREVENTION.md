# BÁO CÁO KIỂM TOÁN VÀ CƠ CHẾ CHỐNG RÒ RỈ THỜI GIAN (TEMPORAL LEAKAGE PREVENTION)

> **Tài liệu chuẩn hóa kiến trúc dữ liệu và kiểm thử rò rỉ thời gian cho Football Data Warehouse & ML Feature Mart**  
> **Phiên bản:** 1.0.0  
> **Áp dụng cho:** `mart_ml_match_features`, `mart_ml_team_match_features`, `mart_ml_temporal_audit`

---

## 1. TỔNG QUAN VÀ BỐI CẢNH

Trong bài toán dự đoán kết quả bóng đá bằng Học máy (Machine Learning), **Rò rỉ thời gian (Temporal Data Leakage)** là nguyên nhân hàng đầu khiến mô hình đạt điểm số đánh giá "ảo" rất cao trong quá trình phát triển (cross-validation hoặc offline test) nhưng sụp đổ hoàn toàn khi triển khai thực tế (production/live forecasting).

Rò rỉ thời gian xảy ra khi:
1. Đặc trưng đầu vào ($f\_*$) vô tình hấp thụ thông tin sự kiện diễn ra **trong trận** hoặc **sau trận** của chính trận đang dự đoán.
2. Thứ tự thời gian bị đảo lộn: Trận đấu diễn ra sau lại được tính vào lịch sử phong độ của trận đấu diễn ra trước.
3. Các trận cùng ngày thiếu giờ thi đấu chuẩn xác bị ngộ nhận là đã kết thúc trước giờ bóng lăn của các trận khác.
4. Phân chia tập dữ liệu (Train / Validation / Test) cắt ngang vòng đấu trong cùng ngày, dẫn đến dữ liệu cùng ngày vừa xuất hiện ở tập huấn luyện vừa xuất hiện ở tập kiểm thử.

Tài liệu này tổng hợp toàn bộ kết quả audit thực nghiệm trên **31.674 trận đấu**, làm rõ cơ chế kỹ thuật đã được triển khai trong dbt và hệ thống kiểm thử tự động (Tripwires).

---

## 2. KẾT QUẢ KIỂM TOÁN THỰC NGHIỆM VÀ QUYẾT ĐỊNH XỬ LÝ

| Hạng mục kiểm toán | Hiện trạng đo được trên dữ liệu thực tế (31.674 trận) | Rủi ro tiềm ẩn | Quyết định & Giải pháp triển khai | Trạng thái |
| :--- | :--- | :--- | :--- | :--- |
| **Rolling Window & ASOF JOIN** | Cơ chế ASOF `b.ord_ts > r.known_ts` đã loại trừ chính trận hiện tại; mẫu Ipswich Town cho thấy $f\_n\_prev = 0$ ở trận mở màn. | Cần đảm bảo thứ tự tích lũy window hoàn toàn khớp với mốc `known_ts`. | **Giữ nguyên cơ chế Event-Sourcing ASOF JOIN**. Đổi thứ tự window sang `order by known_ts, match_key`. Bổ sung test tự động. | **Đã hoàn thành** |
| **Thiếu `kickoff_utc`** | 992 trận thiếu giờ (chiếm 3.13%), phân bổ ở RFPL (480), Bundesliga (324), La Liga (112), Ligue 1 (75). | Nếu gán `known_ts = 00:00 + 3h = 03:00`, trận thiếu giờ có thể bị coi là kết thúc sớm và rò rỉ cho các trận 17:30 cùng ngày. | **Áp dụng quy ước bảo thủ cuối ngày**: Khi thiếu giờ, gán `known_ts = match_date 23:59:59 UTC`. Trận thiếu giờ tuyệt đối không rò rỉ cho bất kỳ trận nào khác cùng ngày. | **Đã hoàn thành** |
| **Phân chia Split** | Dùng `percent_rank()` làm xé lẻ đúng 2 ngày thi đấu (2025-01-25 có 58 trận; 2025-11-22 có 59 trận) nằm ở 2 tập khác nhau. | Rò rỉ thông tin phong độ và tương quan giữa các trận cùng vòng đấu. | **Chuyển sang Calendar Date Cutoff cố định**: Train ($\le 30/06/2024$), Valid ($01/07/2024 - 30/06/2025$), Test ($> 30/06/2025$). 0 ngày bị phân mảnh. | **Đã hoàn thành** |
| **Nguồn Odds (`odds_src`)** | 14.045 trận có odds (44.34%) đều từ `mkt_avg` (football-data.co.uk). 100% trong số 14.045 trận này đều có `kickoff_utc` đầy đủ. | Nhầm lẫn giữa kèo pre-match (trước trận) và kèo in-play / closing odds sau trận. | **Giữ nguyên `mkt_avg`**. Bản chất dữ liệu co.uk là kèo trước trận. Tất cả trận có odds đều có timestamp chuẩn. | **Đã hoàn thành** |
| **Toàn vẹn `INNER JOIN`** | Đầu vào `fact_match`: 31.674 trận; `fact_team_match`: 63.348 dòng; Mart đầu ra: 31.674 trận. | Trùng khóa hoặc mất bản ghi do quan hệ 1-nhiều hoặc nhiều-nhiều. | **Kiểm chứng 1-to-1 thành công**: Mỗi `match_key` có đúng 1 dòng home và 1 dòng away. Không trùng lặp, không mất bản ghi. | **Đã hoàn thành** |
| **Giải test & dummy ảo** | Phát hiện 553 trận thuộc giải `ESSL` trong OpenLigaDB ghép các đội thật thi đấu với "Empty Stands Dummy Team". | Dữ liệu rác làm nhiễu thống kê phong độ và mật độ thi đấu. | **Thiết lập danh sách loại trừ có kiểm soát** trong `int_openliga_matches.sql`: `test-del`, `goalunited23`, `bulimo`, `klb`, `essl`, `blclaude`, `tst1`. | **Đã hoàn thành** |

---

## 3. THIẾT KẾ KỸ THUẬT: CƠ CHẾ CHỐNG RÒ RỈ THỜI GIAN

### 3.1. Kiến trúc Hai Mốc Thời Gian (Two-Timestamp Architecture)

Để giải quyết triệt để sự nhập nhằng giữa thời điểm trận đấu bắt đầu và thời điểm kết quả trận đấu được phép sử dụng làm đặc trưng, hệ thống quy định 2 mốc thời gian độc lập:

```mermaid
timeline
    title Vòng đời thời gian của một trận đấu
    00:00:00 UTC : Mốc dự đoán mặc định khi thiếu giờ (ord_ts)
    15:00:00 UTC : Giờ bóng lăn thực tế (kickoff_utc / ord_ts)
    17:00:00 UTC : Trận đấu kết thúc thực tế
    18:00:00 UTC : Kết quả chính thức khả dụng (known_ts = kickoff_utc + 3h)
    23:59:59 UTC : Kết quả khả dụng bảo thủ khi thiếu giờ (known_ts = 23:59:59)
```

1. **`ord_ts` (Observation / Prediction Timestamp):**
   - Định nghĩa: Thời điểm mô hình thực hiện suy luận dự đoán (phải diễn ra **trước** giờ bóng lăn).
   - Công thức:
     $$\text{ord\_ts} = \begin{cases} \text{kickoff\_utc} & \text{nếu có kickoff\_utc} \\ \text{cast(match\_date as timestamp) (00:00:00 UTC)} & \text{nếu thiếu kickoff\_utc} \end{cases}$$
   - Ý nghĩa: Nếu trận đấu không có giờ cụ thể, mô hình giả định thời điểm dự đoán là ngay đầu ngày thi đấu (00:00:00). Trận đấu này chỉ được phép nhìn thấy những gì diễn ra **trước ngày hôm đó**.

2. **`known_ts` (Event Availability Timestamp):**
   - Định nghĩa: Thời điểm kết quả thi đấu, tỷ số, thẻ phạt, xG chính thức trở thành "dữ liệu lịch sử đã biết" và được phép đóng góp vào đặc trưng phong độ của các trận tiếp theo.
   - Công thức:
     $$\text{known\_ts} = \begin{cases} \text{kickoff\_utc} + 3\text{ hours} & \text{nếu có kickoff\_utc} \\ \text{cast(match\_date as timestamp) + 23h 59m 59s} & \text{nếu thiếu kickoff\_utc} \end{cases}$$
   - Ý nghĩa: Buffer 3 giờ đảm bảo trận đấu 90 phút cộng bù giờ, hiệp phụ, kiểm tra VAR và biên bản trọng tài đã hoàn tất. Khi thiếu giờ, quy ước bảo thủ `23:59:59 UTC` đảm bảo kết quả trận này chỉ có hiệu lực kể từ **ngày hôm sau** trở đi.

---

### 3.2. Cơ chế Snapshot Event-Sourcing và DuckDB ASOF JOIN

Thay vì dùng window function dạng `rows between 5 preceding and 1 preceding` trực tiếp trên bảng trận đấu (dễ gặp lỗi hoãn trận, đổi lịch hoặc thiếu giờ), hệ thống áp dụng mô hình **Event Sourcing & Point-in-time Snapshot**:

1. **Tách riêng bảng sự kiện đã kết thúc (`played`):**
   Chỉ những trận đã diễn ra hoàn tất (`played = true`) mới phát sinh một sự kiện biến đổi trạng thái tại thời điểm `known_ts`.

2. **Tích luỹ trạng thái theo trình tự thời gian khả dụng:**
   Trong CTE `roll`, `roll_side`, `roll_season`, `roll_h2h`, các cửa sổ trượt tích luỹ được sắp xếp đồng nhất:
   ```sql
   window wall as (partition by team_key order by known_ts, match_key rows between unbounded preceding and current row)
   ```
   Mỗi dòng trong `roll` đại diện cho "ảnh chụp trạng thái phong độ" của đội bóng **ngay sau thời điểm `known_ts`**.

3. **ASOF JOIN với điều kiện bất đẳng thức chặt:**
   Khi ghép phong độ vào trận đấu mục tiêu `base b`:
   ```sql
   asof left join roll r on r.team_key = b.team_key and b.ord_ts > r.known_ts
   ```
   DuckDB tìm bản ghi trong `roll` có `r.known_ts` lớn nhất thoả mãn `b.ord_ts > r.known_ts`.

#### Chứng minh triệt tiêu rò rỉ cùng ngày:
- Giả sử trận $A$ thiếu giờ diễn ra ngày $D \implies A.\text{known\_ts} = D\text{ 23:59:59}$.
- Xét trận $B$ bất kỳ diễn ra cùng ngày $D$:
  - Dù $B$ đá lúc 12:30, 15:00 hay 20:00 UTC thì $B.\text{ord\_ts} \le D\text{ 23:59:59}$.
  - Nếu $B$ cũng thiếu giờ $\implies B.\text{ord\_ts} = D\text{ 00:00:00} < D\text{ 23:59:59}$.
  - Điều kiện $B.\text{ord\_ts} > A.\text{known\_ts}$ luôn **SAI** ($B.\text{ord\_ts} \ngtr D\text{ 23:59:59}$).
- **Hệ quả:** Trận $A$ không bao giờ có thể lọt vào đặc trưng của trận $B$. Kết quả của $A$ chỉ có thể được nhìn thấy bởi các trận đấu diễn ra từ ngày $D+1$ trở đi.

---

### 3.3. Phân chia Split theo Cutoff Ngày Lịch (Calendar Date Split)

Thay thế hoàn toàn phương pháp `percent_rank()` (chia theo tỷ lệ số dòng) bằng các mốc ngày lịch cố định:

```sql
case
    when not (m.is_finished and m.home_goals is not null and m.away_goals is not null) then 'predict'
    when m.match_date <= date '{{ var('ml_train_until_date', '2024-06-30') }}' then 'train'
    when m.match_date <= date '{{ var('ml_valid_until_date', '2025-06-30') }}' then 'valid'
    else 'test'
end as split
```

#### Thống kê phân bố sau khi áp dụng (đo đạc chính xác từ `mart_ml_temporal_audit`):
| Phân tập (Split) | Khoảng thời gian | Số lượng trận | Tỷ lệ Chủ nhà thắng (H) | Tỷ lệ Hoà (D) | Tỷ lệ Khách thắng (A) | Số ngày phân mảnh |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Train** | 21/08/2020 – 30/06/2024 | 14.477 | 42.76% | 25.52% | 31.72% | **0** |
| **Validation** | 01/07/2024 – 30/06/2025 | 5.265 | 43.63% | 23.70% | 32.67% | **0** |
| **Test** | 01/07/2025 – 11/10/2026 | 5.083 | 43.50% | 24.61% | 31.89% | **0** |
| **Predict** | Các trận chưa đá / chưa có kết quả | 6.203 | - | - | - | **0** |
| **Tổng cộng** | Toàn bộ Feature Mart | **31.028** | *(Đã loại bỏ 646 trận ảo của các giải test OpenLigaDB)* | | | **0** |

Phân bố nhãn $Y$ giữa các tập đạt tính đồng nhất cao (Chủ nhà ~43%, Hoà ~24-25%, Khách ~32%), phản ánh chính xác phân bố tự nhiên của bóng đá châu Âu qua các mùa giải mà không bị rò rỉ ranh giới ngày.

---

## 4. HỆ THỐNG KIỂM THỬ TỰ ĐỘNG (AUTOMATED TRIPWIRES)

Hệ thống tích hợp 2 bộ kiểm thử dbt tự động trong pipeline CI/CD và DAG Airflow:

### 4.1. `assert_ml_no_label_leakage` (Tripwire rò rỉ nhãn)
- **Cơ chế:** Tính ma trận hệ số tương quan tuyến tính Pearson $|corr(f\_*, y\_\text{goal\_diff})|$ giữa mọi đặc trưng số $f\_*$ và hiệu số bàn thắng thực tế của trận đấu trên tập `is_warm`.
- **Ngưỡng chặn:** Nếu bất kỳ đặc trưng nào có $|corr| > 0.55$, test lập tức **FAIL**.
- **Ý nghĩa:** Bắt quả tang mọi trường hợp đưa cột dữ liệu trong trận (như thẻ đỏ, số cú sút trong trận, hoặc chỉ số xG forecast của Understat) vào nhầm nhóm đặc trưng $f\_*$.

### 4.2. `assert_ml_no_temporal_leakage` (Tripwire rò rỉ thời gian & tính toàn vẹn)
- **Cơ chế:** Quét 4 vi phạm logic thời gian và quan hệ dữ liệu:
  1. `split_date_overlap`: Đảm bảo $count(distinct\ split) = 1$ với mọi ngày thi đấu.
  2. `duplicate_match_key`: Đảm bảo mỗi trận đấu trong `mart_ml_match_features` có duy nhất 1 dòng.
  3. `duplicate_team_side`: Đảm bảo mỗi trận trong `mart_ml_team_match_features` có đúng 1 dòng đội nhà và 1 dòng đội khách.
  4. `negative_f_n_prev`: Đảm bảo số trận lịch sử không âm và trận mở màn của mọi đội luôn có $f\_n\_prev = 0$.

### 4.3. Bảng Kiểm Toán Cấp Trận (`mart_ml_temporal_audit`)
Mỗi trận đấu trong Feature Mart đều được ánh xạ vào `mart_ml_temporal_audit.parquet` với các trường truy vết:
- `match_key`, `competition_key`, `match_date`, `kickoff_utc`
- `is_missing_kickoff` (cờ đánh dấu trận thiếu giờ)
- `ord_ts`, `known_ts` (hai mốc thời gian chuẩn hóa)
- `split`, `f_home_n_prev`, `f_away_n_prev`, `is_warm`, `has_odds`, `y_result`

Bảng này cho phép data scientist truy vết tức thì nguyên nhân của mọi mẫu dị biệt khi phân tích lỗi dự đoán.

---

## 5. DANH SÁCH LOẠI TRỪ GIẢI ĐẤU (LEAGUE BLOCKLIST)

Dựa trên kết quả rà soát OpenLigaDB, danh sách các giải đấu được blocklist tại `int_openliga_matches.sql` gồm:

| Mã giải (`league_shortcut`) | Tên giải / Mô tả | Số trận loại trừ | Lý do xác minh |
| :--- | :--- | :---: | :--- |
| `essl` | Empty Stands League | 553 | 100% các trận đấu đều diễn ra với đội ảo mang tên `Empty Stands Dummy Team`. |
| `test-del` | Test Delete League | ~12 | Giải do người dùng tạo để thử nghiệm chức năng xoá của OpenLigaDB. |
| `goalunited23` | GoalUnited 2023 | ~34 | Trò chơi quản lý bóng đá mô phỏng trực tuyến, không phải giải ngoài đời thực. |
| `bulimo` | Bundesliga Manager Online | ~34 | Trò chơi Fantasy manager, dữ liệu điểm số ảo. |
| `klb` | Kreisliga B Test | ~10 | Dữ liệu thử nghiệm giải phong trào nghiệp dư Đức, thiếu thông tin kiểm chứng. |
| `blclaude` | Bundesliga Claude Test | ~46 | Giải đấu do bot tự động tạo phục vụ thử nghiệm API. |
| `tst1` | Test League 1 | ~40 | Giải đấu test có tên không xác định. |

---

## 6. QUY TRÌNH HUẤN LUYỆN ML KHUYẾN NGHỊ (NEXT STEPS)

Sau khi hoàn tất việc sửa đổi dbt và xác minh toàn bộ test:

1. **Rebuild toàn diện:**
   - Layer Core: `dbt build --select team_alias path:models/core`
   - Layer Mart: `dbt build --select team_alias int_team_alias path:models/mart`
2. **Khởi tạo Baseline mới trên LightGBM:**
   - Huấn luyện trên tập `train` (các trận có `split = 'train'` và `is_warm = true`).
   - Đánh giá trên tập `valid` (các trận có `split = 'valid'` và `is_warm = true`).
   - Giữ nguyên tập `test` độc lập hoàn toàn, chỉ dùng để đánh giá báo cáo cuối cùng.
3. **Thử nghiệm trọng số phân lớp (Class Weights):**
   - Áp dụng kỹ thuật điều chỉnh trọng số nhãn Hoà (`D`) hoặc threshold tuning dựa trên ma trận chi phí (Expected Value) kết hợp tỷ lệ cược `f_mkt_p_*`.
