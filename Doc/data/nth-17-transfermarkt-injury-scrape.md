# 17 — Transfermarkt: Injured/Suspended Players (scrape HTML) → MinIO

**Kiểu ingest:** Web scraping (HTML table), không cần key/auth
**Tần suất:** hàng ngày
**Độ khó:** ★★★☆☆ — khó hơn PhysioRoom (file `16`) vì Transfermarkt có **chống bot chặt hơn** và cấu trúc trang phức tạp hơn

---

## 1. Vì sao chọn nguồn này

Đây là nguồn đối chiếu chéo thứ hai (cùng vai trò với `16`), nhưng Transfermarkt có thế mạnh riêng: dữ liệu đi kèm **ngày dự kiến trở lại (expected return)** và **số trận đã bỏ lỡ** — hai trường mà cả API-Football (`15`) lẫn PhysioRoom (`16`) đều không có đầy đủ. Đây cũng là nguồn được báo chí thể thao (PlanetFootball, FourFourTwo...) dẫn lại thường xuyên khi thống kê injury toàn giải — cho thấy độ tin cậy được cộng đồng công nhận dù không phải API chính thức.

**Cảnh báo quan trọng trước khi làm theo file này:**

1. **Transfermarkt có Cloudflare/anti-bot khá chặt.** Request đơn giản bằng `requests` có thể bị chặn (403) hoặc trả trang chặn thay vì dữ liệu, khác với PhysioRoom (dễ scrape hơn nhiều). Script dưới đây là khung chuẩn, nhưng **có khả năng cần điều chỉnh header/selector thực tế tại thời điểm bạn chạy** — cấu trúc trang và cơ chế chống bot có thể đã đổi.
2. **Kiểm tra `https://www.transfermarkt.com/robots.txt` và điều khoản sử dụng trước khi dùng ở quy mô lớn hoặc thương mại.** Với tần suất thấp (1 lần/ngày, mục đích học tập) rủi ro thấp nhưng không phải bằng không.
3. Nếu bị chặn liên tục, coi đây là **nguồn tùy chọn** — pipeline `15` (API-Football) + `16` (PhysioRoom) đã đủ để có 2 nguồn đối chiếu độc lập; Transfermarkt là lớp thứ 3 để tăng độ tin cậy, không phải nguồn bắt buộc.

## 2. Trang nguồn

Transfermarkt có 2 dạng trang liên quan, theo cấu trúc URL đã dùng nhiều năm nay của site (kiểm tra lại thực tế trước khi chạy, vì site có thể đổi slug):

| Loại trang | URL mẫu | Nội dung |
|---|---|---|
| Toàn giải | `https://www.transfermarkt.com/premier-league/verletztespieler/wettbewerb/GB1` | Danh sách cầu thủ chấn thương của cả Premier League |
| Theo CLB | `https://www.transfermarkt.com/{club-slug}/sperrenundausfaelle/verein/{club_id}/plus/1` | Cả chấn thương **và** treo giò của 1 CLB, kèm ngày dự kiến trở lại |

`GB1` là mã competition_id của Premier League trên Transfermarkt. `{club_id}` cần tra cứu thủ công cho từng CLB (vd: Arsenal = 11, Liverpool = 31 — **xác minh lại số thật trên site**, không hardcode theo trí nhớ).

Cột dữ liệu thường thấy trên trang theo CLB:

| Cột (tiếng Anh trên site) | Ý nghĩa |
|---|---|
| Player | Tên cầu thủ |
| Position | Vị trí |
| Age | Tuổi |
| Injury / Ausfallgrund | Loại chấn thương hoặc lý do treo giò |
| Since | Ngày bắt đầu vắng mặt |
| Until / Expected return | Ngày dự kiến trở lại |
| Missed matches | Số trận đã bỏ lỡ |

## 3. Layout trong lake

```
bronze/transfermarkt/injuries_suspensions/club=Arsenal/ingest_date=2026-09-16/page.html
bronze/transfermarkt/injuries_suspensions/club=Arsenal/ingest_date=2026-09-16/table_raw.json.gz
...

silver/players/tm_player_absence/ingest_date=2026-09-16/part-0.parquet
```

Giống file `16`, **luôn lưu HTML gốc ở bronze** — đây là nguồn dễ đổi cấu trúc nhất trong cả bộ, HTML gốc là thứ duy nhất giúp debug khi script lỗi mà không cần tải lại (trang lúc đó có thể đã khác).

## 4. Script ingest — `pipelines/p17_transfermarkt.py`

```python
"""
Scrape Transfermarkt injuries + suspensions -> MinIO.

CẢNH BÁO: Transfermarkt có chống bot chặt hơn PhysioRoom. Nếu request
liên tục trả 403 hoặc HTML không chứa bảng mong đợi, đây là dấu hiệu bị
chặn — không cố gắng bypass bằng proxy/rotate IP, chỉ nên giảm tần suất
hoặc bỏ qua nguồn này và dựa vào file 15 + 16.
"""
import time
import pandas as pd
from io import StringIO
from lake.minio_io import put_bytes, put_json_gz, put_parquet, today, summary
from lake.http import SESSION

SRC = "transfermarkt"
D = today()

# User-Agent trình duyệt thật — bắt buộc, request mặc định của thư viện
# hầu như luôn bị chặn ngay lập tức trên site này.
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
}

# XÁC MINH LẠI club_id trên site trước khi dùng — đây chỉ là khung ví dụ,
# không đảm bảo đúng 100% tại thời điểm bạn chạy.
CLUBS = {
    "arsenal-fc": ("Arsenal", 11),
    "fc-liverpool": ("Liverpool", 31),
    "manchester-city": ("Manchester City", 281),
    "fc-chelsea": ("Chelsea", 631),
}


def fetch_club_page(slug: str, club_id: int) -> str:
    url = f"https://www.transfermarkt.com/{slug}/sperrenundausfaelle/verein/{club_id}/plus/1"
    r = SESSION.get(url, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} cho {slug} — có thể bị chặn bot")
    return r.text


# ---------- BRONZE ----------
def ingest_club(slug: str, name: str, club_id: int):
    try:
        html = fetch_club_page(slug, club_id)
    except Exception as e:
        print(f"  ! {name}: lỗi tải trang — {e}")
        return None

    put_bytes(
        f"bronze/transfermarkt/injuries_suspensions/club={name}"
        f"/ingest_date={D}/page.html",
        html.encode("utf-8"), SRC, content_type="text/html")

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        print(f"  ! {name}: không tìm thấy bảng nào trong HTML — "
              f"khả năng cao đã bị chặn bot hoặc trang đổi cấu trúc")
        return None

    # Trang thường có nhiều bảng phụ (quảng cáo, liên quan...); bảng
    # injuries/suspensions chính thường là bảng lớn nhất theo số dòng.
    main_table = max(tables, key=len)
    put_json_gz(
        f"bronze/transfermarkt/injuries_suspensions/club={name}"
        f"/ingest_date={D}/table_raw.json.gz",
        main_table.to_dict(orient="records"), SRC,
        meta={"club": name, "rows": len(main_table)})

    main_table["club"] = name
    main_table["ingest_date"] = D
    return main_table


# ---------- SILVER ----------
def build_player_absence(tables: list) -> pd.DataFrame:
    valid = [t for t in tables if t is not None and not t.empty]
    if not valid:
        print("  ! không có dữ liệu nào ingest được — kiểm tra bronze/page.html thủ công")
        return pd.DataFrame()

    df = pd.concat(valid, ignore_index=True)
    # chuẩn hóa tên cột — Transfermarkt hay đổi tên cột theo ngôn ngữ/mùa,
    # nên map "best effort" và giữ nguyên cột gốc nếu không khớp.
    rename_map = {
        "Player": "player_name", "Injury": "absence_reason",
        "Since": "absence_since", "Until": "expected_return",
        "Missed matches": "matches_missed",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    put_parquet(
        f"silver/players/tm_player_absence/ingest_date={D}/part-0.parquet",
        df, SRC, meta={"rows": len(df)})
    return df


if __name__ == "__main__":
    print(f"[1/2] scrape {len(CLUBS)} CLB")
    tables = []
    for slug, (name, cid) in CLUBS.items():
        print(f"  · {name}...")
        t = ingest_club(slug, name, cid)
        tables.append(t)
        time.sleep(3)          # lịch sự hơn hẳn so với PhysioRoom — site nhạy hơn

    print("[2/2] silver")
    df = build_player_absence(tables)
    print(f"  ✓ {len(df)} dòng tổng hợp")

    summary("bronze/transfermarkt/")
    summary("silver/players/tm_player_absence/")
```

Chạy:

```bash
python -m pipelines.p17_transfermarkt
```

## 5. Kết quả mong đợi (khi chạy thành công)

```
[1/2] scrape 4 CLB
  · Arsenal...
  ✓ s3://football-lake/bronze/transfermarkt/injuries_suspensions/club=Arsenal/ingest_date=2026-09-16/page.html  (211,004 B, ...)
  ✓ .../table_raw.json.gz  (2,884 B, ...)
  · Liverpool...
  ✓ ...
[2/2] silver
  ✓ .../tm_player_absence/ingest_date=2026-09-16/part-0.parquet  (4,112 B, 18 rows)
  ✓ 18 dòng tổng hợp

[summary] bronze/transfermarkt/: 8 objects, 0.84 MB
[summary] silver/players/tm_player_absence/: 1 objects, 0.01 MB
```

**Nếu bị chặn thay vào đó**, kết quả sẽ trông như:

```
[1/2] scrape 4 CLB
  · Arsenal...
  ! Arsenal: lỗi tải trang — HTTP 403 cho arsenal-fc — có thể bị chặn bot
  · Liverpool...
  ! Liverpool: không tìm thấy bảng nào trong HTML — khả năng cao đã bị chặn bot
...
[2/2] silver
  ! không có dữ liệu nào ingest được — kiểm tra bronze/page.html thủ công
```

Đây **không phải lỗi code** — là hành vi mong đợi khi site chặn bot. Xem mục 7.

## 6. Truy vấn kiểm chứng

```sql
-- Cầu thủ nào dự kiến trở lại sớm nhất?
SELECT club, player_name, absence_reason, expected_return
FROM read_parquet('s3://football-lake/silver/players/tm_player_absence/**/*.parquet')
WHERE expected_return IS NOT NULL
ORDER BY expected_return;

-- ĐỐI CHIẾU 3 NGUỒN: cầu thủ được cả 15 (API-Football), 16 (PhysioRoom)
-- và 17 (Transfermarkt) cùng xác nhận -> độ tin cậy cao nhất
WITH af AS (
  SELECT DISTINCT player_name, team_name AS club
  FROM read_parquet('s3://football-lake/silver/players/player_absence/**/*.parquet')
),
pr AS (
  SELECT DISTINCT player_name, team AS club
  FROM read_parquet('s3://football-lake/silver/players/pr_player_injuries/**/*.parquet')
),
tm AS (
  SELECT DISTINCT player_name, club
  FROM read_parquet('s3://football-lake/silver/players/tm_player_absence/**/*.parquet')
)
SELECT af.club, af.player_name, 'API-Football' AS nguon_1,
       CASE WHEN pr.player_name IS NOT NULL THEN 'PhysioRoom' END AS nguon_2,
       CASE WHEN tm.player_name IS NOT NULL THEN 'Transfermarkt' END AS nguon_3
FROM af
LEFT JOIN pr ON LOWER(pr.player_name) LIKE '%' || LOWER(SPLIT_PART(af.player_name, ' ', -1)) || '%'
LEFT JOIN tm ON LOWER(tm.player_name) LIKE '%' || LOWER(SPLIT_PART(af.player_name, ' ', -1)) || '%'
WHERE pr.player_name IS NOT NULL OR tm.player_name IS NOT NULL;
```

## 7. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| `HTTP 403` liên tục cho mọi CLB | Cloudflare chặn bot dựa trên fingerprint request | Đây là giới hạn thực tế của nguồn này — **không** cố bypass; coi Transfermarkt là nguồn "nice-to-have", dựa chính vào file `15` + `16` |
| `page.html` tải về nhưng là trang "Just a moment..." / CAPTCHA | Cloudflare challenge page thay vì nội dung thật | Kiểm tra `page.html` đã lưu ở bronze để xác nhận; nếu đúng vậy thì cùng hướng xử lý như trên |
| `pd.read_html` báo lỗi `No tables found` dù HTML tải về bình thường | Trang dùng JavaScript render bảng (client-side), `requests` không chạy JS | Cần công cụ render JS (Selenium/Playwright) nếu muốn tiếp tục nguồn này — vượt phạm vi 1 script `requests` đơn giản |
| `club_id` sai → trang trả về club khác hoặc 404 | ID trong `CLUBS` chỉ là ví dụ, chưa xác minh | Vào từng trang CLB trên transfermarkt.com, lấy đúng `verein/{id}` từ URL thật |
| Tên cột sau `pd.read_html` không khớp `rename_map` | Transfermarkt đổi tên cột theo session/locale (`en` vs auto-detect) | Thêm `?LANG=en` vào cuối URL nếu có, hoặc mở rộng `rename_map` dựa trên cột thực tế quan sát được |

## 8. Mở rộng

- Nếu quyết định đầu tư thêm cho nguồn này, cân nhắc `playwright` thay cho `requests` để vượt qua trang render bằng JS — nhưng đây là nâng cấp đáng kể về độ phức tạp, chỉ nên làm nếu file `15`+`16` chưa đủ.
- Field `matches_missed` (số trận đã bỏ lỡ) là dữ liệu **không có ở 2 nguồn kia** — nếu lấy được ổn định, đây là feature giá trị cho model dự đoán (cầu thủ vắng mặt lâu ảnh hưởng phong độ đội khi trở lại).
- Gộp cả 3 nguồn (`15`, `16`, `17`) thành `gold/analytics/injury_consensus` — chỉ giữ bản ghi được ≥2/3 nguồn xác nhận, và gắn cờ `data_quality_score` theo số nguồn đồng thuận.
