# 16 — PhysioRoom Injury Table (scrape HTML) → MinIO

**Kiểu ingest:** Web scraping (HTML table), không cần key/auth
**Tần suất:** hàng ngày (trang cập nhật thường xuyên trong mùa giải)
**Độ khó:** ★★☆☆☆ — dễ vì bảng có cấu trúc rõ, khó ở chỗ **không có API chính thức nên có thể đổi HTML bất kỳ lúc nào**

---

## 1. Vì sao chọn nguồn này

Đây là nguồn **đối chiếu chéo, không cần key**, dùng để kiểm tra chất lượng dữ liệu `player_absence` lấy từ API-Football (file `15`). PhysioRoom duy trì 1 trang tổng hợp injury table cho Premier League, cập nhật thường xuyên, và trình bày sẵn dưới dạng **bảng HTML** — không phải văn bản tự do như tin tức, nên parse được bằng `pandas.read_html` mà không cần NLP.

Giá trị dạy học: đây là ví dụ chuẩn cho **web scraping có cấu trúc** (khác với file `07` — Reddit/RSS là text phi cấu trúc). Đồng thời dạy thói quen bắt buộc với mọi nguồn scrape: kiểm tra `robots.txt`, đặt User-Agent, xử lý khi HTML đổi cấu trúc.

**Lưu ý về pháp lý/đạo đức:** đây không phải API chính thức. Trước khi dùng ở quy mô sản xuất hoặc thương mại, kiểm tra `https://www.physioroom.com/robots.txt` và điều khoản sử dụng của trang. Trong phạm vi học tập/demo cá nhân với tần suất thấp (1 lần/ngày), rủi ro thấp nhưng không phải bằng không.

## 2. Trang nguồn và cấu trúc

URL: `https://www.physioroom.com/advice/premier-league-injury-table/`

Trang chứa đúng **4 bảng HTML** liên tiếp theo thứ tự cố định:

| # | Nội dung bảng | Cột |
|---|---|---|
| 1 | Tổng số chấn thương theo CLB | `Team`, `Total Injuries`, `Position` |
| 2 | Danh sách cầu thủ chấn thương theo CLB | `Team`, `Injured Players` (list tên, phân cách `,`), `Injury Details` (list loại chấn thương, cùng thứ tự) |
| 3 | Loại chấn thương theo CLB (dạng tổng hợp) | `Team`, `Total Injuries`, `Injury Types` (chuỗi dạng `"Knee ×4, Thigh ×2"`) |
| 4 | Tần suất loại chấn thương toàn giải | `Injury Type`, `Count` |

Bảng số 2 là bảng quan trọng nhất — cần **explode** chuỗi phân cách dấu phẩy thành từng dòng 1 cầu thủ, vì `Injured Players` và `Injury Details` là 2 list cùng độ dài, cùng thứ tự (index thứ *i* của tên khớp với index thứ *i* của loại chấn thương).

## 3. Layout trong lake

```
bronze/physioroom/injury_table/ingest_date=2026-09-16/page.html
bronze/physioroom/injury_table/ingest_date=2026-09-16/tables_raw.json.gz   ← 4 bảng thô dạng list-of-dict

silver/players/pr_player_injuries/ingest_date=2026-09-16/part-0.parquet    ← đã explode, 1 dòng/cầu thủ
silver/dim/pr_club_injury_summary/ingest_date=2026-09-16/part-0.parquet
silver/dim/pr_injury_type_frequency/ingest_date=2026-09-16/part-0.parquet
```

Lưu cả `page.html` gốc ở bronze — nếu 3 tháng sau site đổi cấu trúc và script lỗi, bạn vẫn debug được bằng HTML đã lưu thay vì phải tải lại (trang có thể đã đổi).

## 4. Script ingest — `pipelines/p16_physioroom.py`

```python
"""Scrape PhysioRoom Premier League Injury Table -> MinIO."""
import pandas as pd
from io import StringIO
from lake.minio_io import put_bytes, put_json_gz, put_parquet, today, summary
from lake.http import get

SRC = "physioroom"
D = today()
URL = "https://www.physioroom.com/advice/premier-league-injury-table/"


# ---------- BRONZE ----------
def fetch_page() -> str:
    r = get(URL)
    html = r.text
    put_bytes(
        f"bronze/physioroom/injury_table/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")
    return html


def parse_tables(html: str) -> list:
    """
    Trang có đúng 4 bảng HTML liên tiếp theo thứ tự cố định:
    [0] tổng injuries/CLB, [1] danh sách cầu thủ, [2] loại chấn thương/CLB,
    [3] tần suất loại chấn thương toàn giải.
    pandas.read_html tự parse mọi <table> trong trang theo đúng thứ tự xuất hiện.
    """
    tables = pd.read_html(StringIO(html))
    if len(tables) < 4:
        raise RuntimeError(
            f"Chỉ tìm thấy {len(tables)} bảng, kỳ vọng 4 — "
            f"trang có thể đã đổi cấu trúc, kiểm tra page.html đã lưu ở bronze")

    put_json_gz(
        f"bronze/physioroom/injury_table/ingest_date={D}/tables_raw.json.gz",
        [t.to_dict(orient="records") for t in tables[:4]], SRC,
        meta={"n_tables": len(tables)})
    return tables[:4]


# ---------- SILVER ----------
def build_club_summary(t0: pd.DataFrame) -> pd.DataFrame:
    df = t0.rename(columns={
        "Team": "team", "Total Injuries": "total_injuries", "Position": "rank"})
    df["ingest_date"] = D
    put_parquet(
        f"silver/dim/pr_club_injury_summary/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


def build_player_injuries(t1: pd.DataFrame) -> pd.DataFrame:
    """
    Bảng gốc: 1 dòng/CLB, 2 cột dạng list phân cách dấu phẩy cùng độ dài.
    Explode thành 1 dòng/cầu thủ.
    """
    rows = []
    for _, r in t1.iterrows():
        team = r["Team"]
        players = [p.strip() for p in str(r["Injured Players"]).split(",")]
        details = [d.strip() for d in str(r["Injury Details"]).split(",")]
        if len(players) != len(details):
            # phòng trường hợp lệch số lượng do dấu phẩy trong tên/mô tả
            print(f"  ! {team}: lệch số lượng player ({len(players)}) "
                  f"vs injury ({len(details)}) — bỏ qua dòng lỗi này")
            continue
        for p, d_ in zip(players, details):
            rows.append({"team": team, "player_name": p,
                        "injury_type": d_, "ingest_date": D})
    df = pd.DataFrame(rows)
    put_parquet(
        f"silver/players/pr_player_injuries/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


def build_injury_frequency(t3: pd.DataFrame) -> pd.DataFrame:
    df = t3.rename(columns={"Injury Type": "injury_type", "Count": "count"})
    df["ingest_date"] = D
    put_parquet(
        f"silver/dim/pr_injury_type_frequency/ingest_date={D}/part-0.parquet",
        df, SRC)
    return df


if __name__ == "__main__":
    print("[1/3] tải trang")
    html = fetch_page()

    print("[2/3] parse 4 bảng HTML")
    tables = parse_tables(html)

    print("[3/3] silver")
    club_df = build_club_summary(tables[0])
    player_df = build_player_injuries(tables[1])
    freq_df = build_injury_frequency(tables[3])
    print(f"  ✓ {len(club_df)} CLB, {len(player_df)} bản ghi cầu thủ, "
          f"{len(freq_df)} loại chấn thương")

    summary("bronze/physioroom/")
    summary("silver/players/pr_player_injuries/")
```

Chạy:

```bash
python -m pipelines.p16_physioroom
```

## 5. Kết quả mong đợi

```
[1/3] tải trang
  ✓ s3://football-lake/bronze/physioroom/injury_table/ingest_date=2026-09-16/page.html  (182,441 B, ...)
[2/3] parse 4 bảng HTML
  ✓ s3://football-lake/bronze/physioroom/injury_table/ingest_date=2026-09-16/tables_raw.json.gz  (6,208 B, ...)
[3/3] silver
  ✓ 14 CLB, 58 bản ghi cầu thủ, 9 loại chấn thương

[summary] bronze/physioroom/: 2 objects, 0.18 MB
[summary] silver/players/pr_player_injuries/: 1 objects, 0.01 MB
```

Lưu ý: bảng đầu chỉ liệt kê **các CLB đang có chấn thương** — không phải toàn bộ 20 đội. Đội không xuất hiện nghĩa là (theo trang) đang không có ai chấn thương, không phải lỗi thiếu dữ liệu.

## 6. Truy vấn kiểm chứng

```sql
-- Top CLB nhiều chấn thương nhất theo PhysioRoom
SELECT team, total_injuries, rank
FROM read_parquet('s3://football-lake/silver/dim/pr_club_injury_summary/**/*.parquet')
ORDER BY rank;

-- Danh sách cầu thủ chấn thương của 1 đội cụ thể
SELECT player_name, injury_type
FROM read_parquet('s3://football-lake/silver/players/pr_player_injuries/**/*.parquet')
WHERE team = 'Arsenal';

-- ĐỐI CHIẾU CHÉO: cầu thủ nào PhysioRoom báo chấn thương nhưng
-- API-Football (file 15) KHÔNG có trong player_absence cùng ngày?
SELECT p.team, p.player_name, p.injury_type
FROM read_parquet('s3://football-lake/silver/players/pr_player_injuries/**/*.parquet') p
LEFT JOIN read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet') a
  ON LOWER(a.player_name) LIKE '%' || LOWER(SPLIT_PART(p.player_name, ' ', -1)) || '%'
 AND a.ingest_date = p.ingest_date
WHERE a.player_id IS NULL;
```

Truy vấn cuối là **data-quality check chéo nguồn** đúng nghĩa: nếu danh sách trả về dài bất thường, nghĩa là 1 trong 2 nguồn đang cập nhật chậm hoặc join theo tên (fuzzy) chưa đủ tốt — cần bảng ánh xạ tên cầu thủ chuẩn hơn (dùng QID từ Wikidata, file `06`).

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `RuntimeError: Chỉ tìm thấy N bảng` | PhysioRoom đổi cấu trúc trang (thêm/bớt bảng, đổi thành non-table layout) | Mở `page.html` đã lưu ở bronze, kiểm tra thủ công, sửa lại `parse_tables` |
| `403 Forbidden` | Site chặn request không có User-Agent trình duyệt thật | `lake/http.py` đã đặt `User-Agent` mặc định; nếu vẫn bị chặn, thử header `Accept-Language: en-US` |
| Lệch số lượng `players` vs `details` khi split dấu phẩy | Tên cầu thủ hoặc mô tả chấn thương chứa dấu phẩy (hiếm nhưng có thể) | Script đã in cảnh báo và bỏ qua dòng lỗi; kiểm tra thủ công nếu số dòng bỏ qua nhiều |
| Tên cầu thủ không khớp với API-Football khi join | Viết tắt khác nhau ("M. Odegaard" vs "Martin Odegaard") | Cần bảng ánh xạ tên — ưu tiên join qua họ (surname) hoặc qua QID Wikidata |
| Trang trả `200` nhưng nội dung là trang lỗi/CAPTCHA | Site có thể bật chống bot nếu gọi quá thường xuyên | Giữ tần suất 1 lần/ngày; không nên poll liên tục |

## 8. Mở rộng

- Thêm retry + so sánh checksum HTML giữa 2 lần chạy — nếu HTML không đổi gì so với hôm qua, có thể bỏ qua bước parse để tiết kiệm thời gian.
- Parse thêm bảng số 3 (`Injury Types` theo CLB) nếu cần phân tích sâu hơn — hiện script chỉ dùng bảng 1, 2, 4.
- Kết hợp với `player_absence` (file `15`) và `tm_player_absence` (file `17`) thành 1 bảng `gold/analytics/injury_consensus` — chỉ giữ lại cầu thủ được **ít nhất 2/3 nguồn** xác nhận, giảm nhiễu do 1 nguồn cập nhật trễ.
