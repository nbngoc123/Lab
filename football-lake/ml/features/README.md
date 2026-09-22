Vài điểm dữ liệu đáng chú ý phát hiện qua EDA: chỉ có 1 giải (E0), 2 mùa, 760 trận — mẫu khá nhỏ; thời tiết thiếu ~60%; Pinnacle odds thiếu ~22%; Elo khởi điểm đồng nhất 1500 nên kém tin cậy ở đầu mùa.

## Các bài toán ML có thể áp dụng (không code)

**A. Dự đoán kết quả trận đấu (từ `feature_match_ml`)**
- **Phân loại 3 lớp** (H/D/A) — bài toán kinh điển nhất, dùng toàn bộ feature form + elo + market + xG + position
- **Phân loại nhị phân "Home thắng hay không"** hoặc "không thua" — đơn giản hóa, thường dễ đạt độ chính xác cao hơn
- **Over/Under 2.5 bàn** (`target_over25`) — phân loại nhị phân, phổ biến trong cá cược thể thao
- **BTTS** (`target_btts`) — cả 2 đội ghi bàn hay không
- **Dự đoán tổng số bàn thắng** (`target_total_goals`) — hồi quy hoặc mô hình đếm (Poisson/Negative Binomial)
- **Dự đoán bàn thắng riêng từng đội** (`target_home_goals`, `target_away_goals`) — 2 mô hình Poisson độc lập hoặc mô hình đôi (bivariate Poisson) để tái tạo tỷ số chính xác, xác suất Over/Under, BTTS từ đó

**B. Bài toán liên quan đến thị trường cược (dùng `feature_market`)**
- **Value betting / phát hiện lệch giá**: so sánh xác suất model dự đoán với `mkt_p_*`/`pin_p_*` để tìm kèo có kỳ vọng dương
- **Dự đoán market margin** hoặc số nhà cái tham gia — ít phổ biến hơn nhưng khả thi
- **Mô hình hiệu chỉnh (calibration)**: đánh giá và cải thiện độ tin cậy xác suất dự đoán so với thị trường

**C. Bài toán xếp hạng / phong độ (dùng `feature_league_position`, `feature_team_match`)**
- **Dự đoán thứ hạng cuối mùa** của một đội tại một thời điểm giữa mùa
- **Dự đoán đội lọt Top 6 / xuống hạng** — phân loại nhị phân theo mùa
- **Phát hiện đội "quá phong độ" hoặc "dưới phong độ"** so với chất lượng đội hình thực (dùng xG overperformance)

**D. Bài toán dựa trên xG (dùng `feature_team_xg`)**
- **Dự đoán xG trận tiếp theo** của một đội — hồi quy, tiền đề cho mô hình mô phỏng tỷ số
- **Phân loại đội "may mắn" vs "xứng đáng"** dựa trên xG overperformance kéo dài
- **Mô phỏng Monte Carlo trận đấu** từ xG kỳ vọng để tính xác suất mọi tỷ số có thể

**E. Bài toán dùng Attention (dùng `feature_team_attention`)**
- **Phát hiện bất thường (anomaly detection)**: pv_ratio tăng đột biến có thể báo hiệu sự kiện ngoài sân cỏ ảnh hưởng phong độ
- **Feature bổ trợ** cho model chính, không dùng độc lập vì tín hiệu yếu

**F. Bài toán nâng cao / kết hợp nhiều nguồn**
- **Ensemble/stacking** nhiều model con (Elo-based, xG-based, Market-based) thành 1 model tổng hợp mạnh hơn
- **Time-series / sequence model** (LSTM, GRU) học trực tiếp từ chuỗi trận gần nhất thay vì feature rolling thủ công
- **Player-to-match aggregation** (nếu sau này có `obt_player_360`/`mart_player`): dự đoán ảnh hưởng của đội hình ra sân, cầu thủ chấn thương/treo giò lên kết quả
- **Uncertainty quantification**: thay vì chỉ dự đoán điểm, ước lượng khoảng tin cậy — quan trọng với mẫu nhỏ như hiện tại
- **Causal/what-if analysis**: ảnh hưởng của nghỉ ngơi (`rest_days`), thời tiết, giờ đá đến kết quả — hỗ trợ phân tích chứ không chỉ dự đoán

Với quy mô dữ liệu hiện tại (760 trận, 1 giải, 2 mùa), nhóm bài toán (A) và (D) là khả thi và có giá trị thực tế nhất để bắt đầu; nhóm (F) nên để lại khi có thêm dữ liệu/nhiều giải đấu hơn.