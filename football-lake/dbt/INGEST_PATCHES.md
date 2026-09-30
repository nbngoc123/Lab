# Sửa nhỏ bên code ingest để staging chạy chắc hơn

## 1. p16 physioroom - NaN làm hỏng JSON
`DataFrame.to_dict` giữ NaN; `json.dumps` ghi ra literal `NaN` (JSON không hợp lệ) => DuckDB không đọc được file.
```python
raw_data = [t.astype(object).where(t.notna(), None).to_dict(orient="records") for t in tables[:4]]
```

## 2. p09 football-data.org - đừng lưu body lỗi thành data
```python
def call(path):
    r = SESSION.get(f"{BASE}{path}", headers=HEADERS, timeout=30)
    time.sleep(6.5)
    r.raise_for_status()      # 403/429 => task fail => retry, không ghi bronze
    return r.json()
```
(staging đã chịu được body lỗi cũ: chúng cho ra 0 dòng.)

## 3. p22 odds hourly
Key hiện là `ingest_date={D}` + `exists()` nên chạy giờ thứ 2..24 trong ngày đều bỏ qua.
Đổi thành `ingest_date={D}T{HH}` (staging đã đọc được cả dạng này: `path_date` lấy 10 ký tự đầu)
hoặc đổi DAG sang `@daily`.
