# Cài thư viện (chạy 1 lần; bỏ comment nếu thiếu). Sau khi cài -> Restart Kernel.
# %pip install -q duckdb pandas pyarrow s3fs sqlalchemy psycopg2-binary scikit-learn matplotlib requests
print("Nếu vừa cài đặt, hãy Restart Kernel rồi chạy lại.")

# ====== CẤU HÌNH CHUNG (sửa cho khớp docker-compose của bạn) ======
CFG = {
    "s3_endpoint": "localhost:9000",
    "s3_key":      "minioadmin",
    "s3_secret":   "minioadmin123",
    "pg_dwh":      "postgresql+psycopg2://dataeng:dataeng123@localhost:5432/dwh",
    "pg_source":   "postgresql+psycopg2://dataeng:dataeng123@localhost:5432/source_db",
    "taxi_url":    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "zone_url":    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    "taxi_local":  "",          # nếu có file .parquet tải sẵn, điền đường dẫn để dùng offline
    "sample_rows": 150_000,
}
import duckdb, pandas as pd, s3fs
S3_OPTS = {"key":CFG["s3_key"], "secret":CFG["s3_secret"],
           "client_kwargs":{"endpoint_url":"http://"+CFG["s3_endpoint"]}}
fs = s3fs.S3FileSystem(key=CFG["s3_key"], secret=CFG["s3_secret"],
                       client_kwargs={"endpoint_url":"http://"+CFG["s3_endpoint"]})
def duck():
    "DuckDB đã nạp sẵn secret truy cập MinIO."
    con = duckdb.connect(); con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f'''CREATE OR REPLACE SECRET minio (TYPE s3, KEY_ID '{CFG["s3_key"]}',
        SECRET '{CFG["s3_secret"]}', ENDPOINT '{CFG["s3_endpoint"]}',
        USE_SSL false, URL_STYLE 'path');''')
    return con
def ensure_buckets():
    for b in ["raw","staging","curated"]:
        try:
            if not fs.exists(b): fs.mkdir(b)
        except Exception as e: print(b, e)
ensure_buckets()
print("Cấu hình OK.")

src = CFG["taxi_local"] or CFG["taxi_url"]
trips = pd.read_parquet(src).sample(CFG["sample_rows"], random_state=42)
zones = pd.read_csv(CFG["zone_url"])
print("trips:", trips.shape, "| zones:", zones.shape)
trips.to_parquet("s3://raw/nyc_taxi/yellow_taxi.parquet", storage_options=S3_OPTS, index=False)
zones.to_parquet("s3://raw/nyc_taxi/taxi_zones.parquet", storage_options=S3_OPTS, index=False)
print("✅ Đã ghi vào s3://raw/nyc_taxi/")

import requests
d = requests.get("https://archive-api.open-meteo.com/v1/archive", params={
    "latitude":40.71,"longitude":-74.01,"start_date":"2024-01-01","end_date":"2024-01-31",
    "daily":"temperature_2m_mean,precipitation_sum","timezone":"America/New_York"}).json()["daily"]
weather = pd.DataFrame({"date":d["time"],"temp_mean":d["temperature_2m_mean"],"precip_sum":d["precipitation_sum"]})
weather.to_parquet("s3://raw/external/weather_daily.parquet", storage_options=S3_OPTS, index=False)
print(weather.head()); print("✅ Đã ghi weather_daily")

con = duck()
print("Số chuyến:", con.execute("SELECT count(*) FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')").fetchone()[0])
display(con.execute('''SELECT payment_type, count(*) so_chuyen, round(avg(total_amount),2) tb_tien
                       FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')
                       GROUP BY 1 ORDER BY 2 DESC''').df())
print("Các đối tượng trong raw:"); [print(" -", p) for p in fs.find("raw")]

# TODO Bài tập 1


# TODO Bài tập 2


