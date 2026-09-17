# 14 — Báo cáo tuần tự động (docx + PDF từ Gold layer) → đẩy ngược MinIO

**Kiểu ingest:** Không thu thập gì — **lake tự sinh ra tài liệu output** rồi ghi ngược vào chính nó
**Tần suất:** hàng tuần (ví dụ mỗi thứ Hai, tổng kết tuần vừa qua)
**Độ khó:** ★★★☆☆ — khó ở việc phối hợp nhiều nguồn dữ liệu vào 1 tài liệu, không phải ở kỹ thuật ingest

---

## 1. Vì sao cần bước này

File 12 (ELO) đã chứng minh lake tự sinh ra **dữ liệu** mới. File này đi xa hơn một bước: lake tự sinh ra **tài liệu hoàn chỉnh** — một file `.docx`/`.pdf` mà con người đọc trực tiếp được, không cần mở DuckDB hay viết SQL.

Đây là mảnh ghép thường bị bỏ qua khi dạy data lake: mọi người dừng lại ở "dữ liệu đã sạch, đã có gold layer" mà quên rằng **giá trị cuối cùng của một lake là báo cáo tới tay người ra quyết định**. Pipeline này khép vòng lặp: *thu thập → làm sạch → tính toán → trình bày → lưu trữ lại chính vào lake* để tuần sau đối chiếu.

Kiến trúc:

```
silver/matches (file 03/09)  ─┐
gold/analytics/elo_ratings (file 12) ─┼─→ DuckDB tổng hợp ─→ docx-js dựng .docx ─→ LibreOffice xuất .pdf ─→ MinIO
silver/text/entity_mentions (file 07) ─┘                                                    │
                                                                                    (cùng 1 báo cáo, 2 định dạng)
```

## 2. Layout trong lake

```
gold/reports/weekly/week=2026-W38/weekly_report.docx
gold/reports/weekly/week=2026-W38/weekly_report.pdf
gold/reports/weekly/week=2026-W38/data_snapshot.json     ← số liệu thô đã dùng để viết báo cáo (tái tạo được)
_meta/reports/run_log/week=2026-W38.json                  ← nhật ký: nguồn nào, bao nhiêu dòng, thời gian chạy
```

Lưu `data_snapshot.json` là chủ ý: nếu 6 tháng sau bạn thắc mắc "sao báo cáo tuần đó lại viết vậy", bạn tái dựng lại được chính xác input mà không cần chạy lại toàn bộ 12 pipeline.

## 3. Bước 1 — Tổng hợp số liệu từ gold/silver bằng DuckDB (Python)

```python
# pipelines/p14a_gather_weekly_data.py
"""Gom số liệu từ nhiều nguồn trong lake thành 1 snapshot JSON cho báo cáo tuần."""
import json
from datetime import date, timedelta
import duckdb
from lake.minio_io import S3, BUCKET

def week_id(d: date) -> str:
    return f"{d.isocalendar().year}-W{d.isocalendar().week:02d}"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("""
      SET s3_endpoint='localhost:9000'; SET s3_use_ssl=false;
      SET s3_url_style='path';
      SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin123';
    """)
    return con


def gather(week_start: date, week_end: date) -> dict:
    con = connect()

    # 1. Top 5 ELO hiện tại (file 12)
    elo_top = con.sql("""
        SELECT rank, team, elo
        FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/current/*.parquet')
        ORDER BY rank LIMIT 5
    """).df().to_dict("records")

    # 2. Biến động ELO trong tuần (so đầu tuần với cuối tuần)
    elo_change = con.sql(f"""
        WITH bounds AS (
          SELECT home_team AS team, elo_home_after AS elo, match_date
          FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/history/*.parquet')
          WHERE match_date BETWEEN '{week_start}' AND '{week_end}'
          UNION ALL
          SELECT away_team, elo_away_after, match_date
          FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/history/*.parquet')
          WHERE match_date BETWEEN '{week_start}' AND '{week_end}'
        )
        SELECT team,
               MAX(elo) FILTER (WHERE match_date = MAX(match_date) OVER (PARTITION BY team)) AS elo_end,
               MIN(elo) FILTER (WHERE match_date = MIN(match_date) OVER (PARTITION BY team)) AS elo_start
        FROM bounds GROUP BY team
    """).df()
    elo_change["change"] = elo_change["elo_end"] - elo_change["elo_start"]

    # 3. Trận đấu trong tuần, kèm cờ "bất ngờ" (đối chiếu dự đoán ELO)
    matches = con.sql(f"""
        SELECT match_date, home_team, away_team, home_goals, away_goals,
               predicted_prob_home_win, result
        FROM read_parquet('s3://football-lake/gold/analytics/elo_ratings/history/*.parquet')
        WHERE match_date BETWEEN '{week_start}' AND '{week_end}'
        ORDER BY match_date
    """).df()
    matches["surprise"] = (
        ((matches.result == "A") & (matches.predicted_prob_home_win > 0.7)) |
        ((matches.result == "H") & (matches.predicted_prob_home_win < 0.3))
    )

    # 4. Sentiment mạng xã hội trong tuần (file 07)
    sentiment = con.sql(f"""
        SELECT team,
               SUM(mentions) AS total_mentions,
               ROUND(AVG(sentiment_score), 3) AS avg_sentiment
        FROM read_parquet('s3://football-lake/silver/text/entity_mentions/**/*.parquet')
        WHERE ts BETWEEN '{week_start}' AND '{week_end}'
        GROUP BY team ORDER BY total_mentions DESC LIMIT 8
    """).df()

    return {
        "week_start": str(week_start), "week_end": str(week_end),
        "elo_top": elo_top,
        "elo_change": elo_change.round(1).to_dict("records"),
        "matches": matches.to_dict("records"),
        "sentiment": sentiment.to_dict("records"),
    }


def save_snapshot(data: dict, wk: str):
    S3.put_object(
        Bucket=BUCKET,
        Key=f"gold/reports/weekly/week={wk}/data_snapshot.json",
        Body=json.dumps(data, default=str, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json")
    print(f"  ✓ snapshot lưu tại gold/reports/weekly/week={wk}/data_snapshot.json")


if __name__ == "__main__":
    today_d = date.today()
    week_start = today_d - timedelta(days=today_d.weekday() + 7)   # thứ 2 tuần trước
    week_end = week_start + timedelta(days=6)
    wk = week_id(week_start)

    print(f"[1/2] gom số liệu tuần {wk} ({week_start} → {week_end})")
    data = gather(week_start, week_end)
    print(f"  · {len(data['matches'])} trận, {len(data['sentiment'])} đội có mention")

    print("[2/2] lưu snapshot")
    save_snapshot(data, wk)
```

## 4. Bước 2 — Dựng file .docx từ snapshot (Node, dùng skill `docx`)

**Trước khi viết bước này, đọc `/mnt/skills/public/docx/SKILL.md`** — nó có các lưu ý bắt buộc (page size DXA, cách làm bảng, không dùng `\n`...). Script dưới tuân theo đúng các gotcha đó và **đã được chạy thử thật** để kiểm chứng ra file đúng định dạng (xem ảnh chụp ở mục 5).

```javascript
// pipelines/p14b_build_report.js
// Đọc data_snapshot.json (đã tải xuống từ MinIO) -> dựng weekly_report.docx
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell,
  WidthType, ShadingType, BorderStyle, AlignmentType,
} = require("docx");
const fs = require("fs");

const snapshotPath = process.argv[2] || "data_snapshot.json";
const outPath = process.argv[3] || "weekly_report.docx";
const data = JSON.parse(fs.readFileSync(snapshotPath, "utf-8"));

function headerCell(text, width) {
  return new TableCell({
    width: { size: width, type: WidthType.DXA },
    shading: { type: ShadingType.CLEAR, fill: "1F2937" },
    children: [new Paragraph({
      children: [new TextRun({ text, bold: true, color: "FFFFFF", size: 20 })] })],
  });
}
function cell(text, width, opts = {}) {
  return new TableCell({
    width: { size: width, type: WidthType.DXA },
    children: [new Paragraph({
      children: [new TextRun({ text: String(text), size: 20, ...opts })] })],
  });
}
function h1(text) {
  return new Paragraph({ heading: HeadingLevel.HEADING_1,
    spacing: { before: 300, after: 150 }, children: [new TextRun({ text })] });
}
function p(text, opts = {}) {
  return new Paragraph({ spacing: { after: 150 },
    children: [new TextRun({ text, size: 22, ...opts })] });
}

// ---- Bảng 1: Top ELO ----
const eloTable = new Table({
  width: { size: 9000, type: WidthType.DXA },
  columnWidths: [1200, 4000, 2000, 1800],
  rows: [
    new TableRow({ children: [headerCell("Hạng", 1200), headerCell("Đội", 4000),
      headerCell("ELO", 2000), headerCell("Thay đổi tuần", 1800)] }),
    ...data.elo_top.map(r => {
      const chg = data.elo_change.find(c => c.team === r.team);
      const change = chg ? chg.change : 0;
      return new TableRow({ children: [
        cell(r.rank, 1200), cell(r.team, 4000), cell(r.elo.toFixed(1), 2000),
        cell((change >= 0 ? "+" : "") + change.toFixed(1), 1800,
          { color: change >= 0 ? "16A34A" : "DC2626", bold: true }),
      ] });
    }),
  ],
});

// ---- Bảng 2: Trận đấu trong tuần ----
const matchTable = new Table({
  width: { size: 9000, type: WidthType.DXA },
  columnWidths: [1400, 2600, 2600, 1200, 1200],
  rows: [
    new TableRow({ children: [headerCell("Ngày", 1400), headerCell("Nhà", 2600),
      headerCell("Khách", 2600), headerCell("Tỷ số", 1200), headerCell("Bất ngờ?", 1200)] }),
    ...data.matches.map(m => new TableRow({ children: [
      cell(String(m.match_date).slice(0, 10), 1400),
      cell(m.home_team, 2600), cell(m.away_team, 2600),
      cell(`${m.home_goals}-${m.away_goals}`, 1200),
      cell(m.surprise ? "Có" : "Không", 1200,
        { color: m.surprise ? "DC2626" : "374151" }),
    ] })),
  ],
});

// ---- Bảng 3: Sentiment ----
const sentimentTable = new Table({
  width: { size: 9000, type: WidthType.DXA },
  columnWidths: [3000, 3000, 3000],
  rows: [
    new TableRow({ children: [headerCell("Đội", 3000),
      headerCell("Lượt nhắc", 3000), headerCell("Sentiment TB", 3000)] }),
    ...data.sentiment.map(s => new TableRow({ children: [
      cell(s.team, 3000), cell(s.total_mentions, 3000),
      cell(s.avg_sentiment.toFixed(2), 3000,
        { color: s.avg_sentiment >= 0 ? "16A34A" : "DC2626" }),
    ] })),
  ],
});

const doc = new Document({
  sections: [{
    properties: { page: { size: { width: 12240, height: 15840 } } },
    children: [
      new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 60 },
        children: [new TextRun({ text: "BÁO CÁO TUẦN — FOOTBALL DATA LAKE",
          bold: true, size: 36, color: "1F2937" })] }),
      new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 300 },
        children: [new TextRun({ text: `Tuần ${data.week_start} → ${data.week_end}`,
          size: 24, color: "6B7280" })] }),
      new Paragraph({ border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: "D1D5DB" } },
        spacing: { after: 200 }, children: [] }),

      h1("1. Bảng xếp hạng ELO (gold layer, tự tính — file 12)"),
      eloTable,

      h1("2. Kết quả trận đấu trong tuần"),
      p("\"Bất ngờ\" = kết quả đi ngược xác suất dự đoán của mô hình ELO trước trận (>70% cho một bên nhưng bên kia thắng)."),
      matchTable,

      h1("3. Sentiment mạng xã hội (nguồn Reddit — file 07)"),
      sentimentTable,

      h1("4. Ghi chú"),
      p("Báo cáo sinh tự động từ gold layer của Football Data Lake, không qua chỉnh sửa thủ công.",
        { italics: true, color: "6B7280" }),
      p(`Thời điểm tạo: ${new Date().toISOString()}`, { italics: true, color: "6B7280", size: 18 }),
    ],
  }],
});

Packer.toBuffer(doc).then(buf => {
  fs.writeFileSync(outPath, buf);
  console.log(`Đã tạo ${outPath}, ${buf.length} bytes`);
});
```

## 5. Bước 3 — Xuất PDF và kiểm tra trực quan (bắt buộc theo skill `docx`)

```bash
python /mnt/skills/public/docx/scripts/office/soffice.py \
  --headless --convert-to pdf weekly_report.docx

pdftoppm -jpeg -r 100 weekly_report.pdf page
```

**Đã chạy thử thật với dữ liệu mẫu** để kiểm chứng — kết quả console:

```
Đã tạo weekly_report.docx, 10126 bytes
convert /home/claude/weekly_report_demo/weekly_report.docx as a Writer document
  -> weekly_report.pdf using filter : writer_pdf_Export
```

Ảnh chụp trang 1 (`page-1.jpg`) cho thấy đúng những gì mong đợi:

- Tiêu đề "BÁO CÁO TUẦN — FOOTBALL DATA LAKE" căn giữa, có dòng kẻ phân cách
- Bảng 1 (ELO): 5 dòng, cột "Thay đổi" hiện đúng màu xanh cho số dương (+6.1, +14.7, +2.0) và đỏ cho số âm (-3.2, -9.4)
- Bảng 2 (Trận đấu): 3 trận, cột "Bất ngờ?" tô đỏ đúng 2 dòng có giá trị "Có"
- Bảng 3 (Sentiment): 3 đội, điểm sentiment tô xanh (0.34, 0.08) và đỏ (-0.12) đúng theo dấu
- Không tràn lề trang khổ Letter, không lệch cột — chứng tỏ `columnWidths` cộng đúng bằng `width` của từng bảng

Nếu bảng bị tràn hoặc lệch cột, nguyên nhân gần như luôn là `columnWidths` không cộng đúng bằng `width` của `Table` — đây là gotcha số 1 của skill `docx`.

## 6. Bước 4 — Đẩy cả 2 file ngược lên MinIO — `pipelines/p14c_publish_report.py`

```python
"""Đẩy weekly_report.docx + .pdf lên MinIO, ghi log lần chạy."""
import json
from datetime import datetime, timezone
from pathlib import Path
from lake.minio_io import put_file, S3, BUCKET

SRC = "weekly-report-generator"


def publish(week_id: str, docx_path: str, pdf_path: str, snapshot_rows: dict):
    base = f"gold/reports/weekly/week={week_id}"

    put_file(f"{base}/weekly_report.docx", docx_path, SRC,
             content_type="application/vnd.openxmlformats-officedocument"
                          ".wordprocessingml.document")
    put_file(f"{base}/weekly_report.pdf", pdf_path, SRC,
             content_type="application/pdf")

    S3.put_object(
        Bucket=BUCKET, Key=f"_meta/reports/run_log/week={week_id}.json",
        Body=json.dumps({
            "week": week_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "matches_included": len(snapshot_rows.get("matches", [])),
            "teams_in_sentiment": len(snapshot_rows.get("sentiment", [])),
            "files": ["weekly_report.docx", "weekly_report.pdf"],
        }).encode(), ContentType="application/json")

    print(f"  ✓ đã publish báo cáo tuần {week_id} lên {base}/")


if __name__ == "__main__":
    import sys
    wk, docx_p, pdf_p, snap_p = sys.argv[1:5]
    snapshot = json.loads(Path(snap_p).read_text(encoding="utf-8"))
    publish(wk, docx_p, pdf_p, snapshot)
```

## 7. Chạy toàn bộ chuỗi (orchestration thủ công)

```bash
# 1. Gom số liệu -> lưu snapshot lên MinIO + xuống local để bước sau dùng
python -m pipelines.p14a_gather_weekly_data
mc cp local/football-lake/gold/reports/weekly/week=2026-W38/data_snapshot.json .

# 2. Dựng docx từ snapshot
node pipelines/p14b_build_report.js data_snapshot.json weekly_report.docx

# 3. Xuất PDF + kiểm tra hình ảnh
python /mnt/skills/public/docx/scripts/office/soffice.py \
  --headless --convert-to pdf weekly_report.docx
pdftoppm -jpeg -r 100 weekly_report.pdf page   # xem thử page-1.jpg

# 4. Đẩy ngược cả 2 file + log lên MinIO
python -m pipelines.p14c_publish_report \
  2026-W38 weekly_report.docx weekly_report.pdf data_snapshot.json
```

## 8. Kết quả mong đợi (toàn chuỗi)

```
[1/2] gom số liệu tuần 2026-W38 (2026-09-14 → 2026-09-20)
  · 3 trận, 3 đội có mention
[2/2] lưu snapshot
  ✓ snapshot lưu tại gold/reports/weekly/week=2026-W38/data_snapshot.json

Đã tạo weekly_report.docx, 10126 bytes
convert weekly_report.docx -> weekly_report.pdf using filter: writer_pdf_Export

  ✓ s3://football-lake/gold/reports/weekly/week=2026-W38/weekly_report.docx  (10,126 B, ...)
  ✓ s3://football-lake/gold/reports/weekly/week=2026-W38/weekly_report.pdf  (80,178 B, ...)
  ✓ đã publish báo cáo tuần 2026-W38 lên gold/reports/weekly/week=2026-W38/
```

## 9. Truy vấn kiểm chứng

```sql
-- Danh sách mọi báo cáo tuần đã sinh ra — chứng minh vòng lặp tự động đang chạy đều
SELECT week, generated_at, matches_included, teams_in_sentiment
FROM read_json_auto('s3://football-lake/_meta/reports/run_log/*.json')
ORDER BY week;

-- Tải lại snapshot của 1 tuần bất kỳ để tái dựng báo cáo mà không cần chạy lại 12 pipeline
SELECT * FROM read_json_auto(
  's3://football-lake/gold/reports/weekly/week=2026-W38/data_snapshot.json');
```

Truy vấn đầu tiên là bằng chứng tốt nhất cho "lake tự sinh output": sau vài tuần vận hành, bạn có một bảng log liệt kê **chính những báo cáo mà lake tự viết ra**, không phải dữ liệu thu thập từ đâu cả.

## 10. Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Bảng tràn lề trang | `columnWidths` không cộng bằng `width` của Table | Kiểm tra tổng: `1200+4000+2000+1800 = 9000` phải khớp `width.size` |
| Chữ có dấu tiếng Việt bị vỡ font khi convert PDF | Font mặc định của LibreOffice thiếu glyph tiếng Việt | Cài `fonts-noto` hoặc chỉ định `font: "Noto Sans"` trong `TextRun` |
| `PageBreak` không xuống trang mới | Đặt ngoài `Paragraph` | Luôn bọc `new Paragraph({ children: [new PageBreak()] })` |
| Snapshot rỗng (`matches: []`) | Tuần chọn chưa có trận nào ghi vào `elo_ratings/history` | Kiểm tra file 03/09 và file 12 đã chạy cho tuần đó chưa — đây là **phụ thuộc pipeline**, không phải lỗi của file 14 |
| `soffice.py` báo lỗi không tìm thấy binary | Môi trường thiếu LibreOffice | Cài `libreoffice` hoặc chạy trong container đã có sẵn (như môi trường Claude) |

## 11. Mở rộng

- Thêm biểu đồ: dùng `ImageRun` trong docx-js để nhúng PNG biểu đồ ELO theo thời gian (vẽ bằng matplotlib trước, lưu file tạm, `type: "png"` khi nhúng).
- Sinh thêm bản **HTML** cùng lúc (tái dùng đúng `data_snapshot.json`) để publish lên Artifact — cho phép người nhận xem trên trình duyệt mà không cần mở Word.
- Lên lịch bằng cron chạy sáng thứ Hai hàng tuần → sau vài tháng bạn có kho báo cáo lịch sử hoàn chỉnh, mỗi cái tái tạo được 100% từ snapshot JSON kèm theo.
- Gửi file PDF vừa publish qua email/Slack ngay sau bước 4 — biến pipeline từ "tạo báo cáo" thành "phát hành báo cáo" hoàn chỉnh.
