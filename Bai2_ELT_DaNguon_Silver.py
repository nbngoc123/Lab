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

# ====== HÀM TIỀN ĐỀ (tự dựng dữ liệu nếu chưa chạy bài trước) ======
import requests
from sqlalchemy import create_engine, text

def _seed_source_db():
    import numpy as np
    raw = create_engine(CFG["pg_dwh"], isolation_level="AUTOCOMMIT")
    with raw.connect() as c:
        if not c.execute(text("select 1 from pg_database where datname='source_db'")).first():
            c.execute(text("CREATE DATABASE source_db"))
    eng = create_engine(CFG["pg_source"])
    pd.DataFrame({"payment_type":[1,2,3,4],
        "name":["Credit card","Cash","No charge","Dispute"]}).to_sql("payment_type",eng,if_exists="replace",index=False)
    pd.DataFrame({"driver_id":range(1,51),
        "shift":np.random.choice(["day","night"],50)}).to_sql("driver",eng,if_exists="replace",index=False)
    cal = pd.DataFrame({"date":pd.date_range("2024-01-01","2024-01-31")})
    cal["is_weekend"]=cal["date"].dt.dayofweek>=5
    cal["is_holiday"]=cal["date"].isin(pd.to_datetime(["2024-01-01"]))
    cal.to_sql("calendar",eng,if_exists="replace",index=False)

def ensure_raw():
    "Bảo đảm tầng Bronze (raw) đã tồn tại trong MinIO."
    if fs.exists("raw/nyc_taxi/yellow_taxi.parquet"):
        return "raw đã sẵn sàng."
    print("Đang dựng tầng raw (Bronze)...")
    src = CFG["taxi_local"] or CFG["taxi_url"]
    trips = pd.read_parquet(src).sample(CFG["sample_rows"], random_state=42)
    trips.to_parquet("s3://raw/nyc_taxi/yellow_taxi.parquet", storage_options=S3_OPTS, index=False)
    pd.read_csv(CFG["zone_url"]).to_parquet("s3://raw/nyc_taxi/taxi_zones.parquet", storage_options=S3_OPTS, index=False)
    d = requests.get("https://archive-api.open-meteo.com/v1/archive", params={
        "latitude":40.71,"longitude":-74.01,"start_date":"2024-01-01","end_date":"2024-01-31",
        "daily":"temperature_2m_mean,precipitation_sum","timezone":"America/New_York"}).json()["daily"]
    pd.DataFrame({"date":d["time"],"temp_mean":d["temperature_2m_mean"],"precip_sum":d["precipitation_sum"]}
        ).to_parquet("s3://raw/external/weather_daily.parquet", storage_options=S3_OPTS, index=False)
    _seed_source_db()
    for t in ["driver","payment_type","calendar"]:
        pd.read_sql(f"SELECT * FROM {t}", create_engine(CFG["pg_source"])
            ).to_parquet(f"s3://raw/oltp/{t}.parquet", storage_options=S3_OPTS, index=False)
    return "Đã dựng xong raw."

def ensure_silver():
    ensure_raw()
    if fs.exists("staging/stg_trips.parquet"): return "silver đã sẵn sàng."
    con = duck()
    con.execute('''COPY (
      SELECT CAST(tpep_pickup_datetime AS TIMESTAMP) pickup_ts,
             CAST(tpep_pickup_datetime AS DATE) pickup_date,
             EXTRACT(hour FROM tpep_pickup_datetime) pickup_hour,
             PULocationID pu_zone_id, DOLocationID do_zone_id,
             passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type
      FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')
      WHERE fare_amount>0 AND trip_distance>0
        AND tpep_pickup_datetime>=TIMESTAMP '2024-01-01'
        AND tpep_pickup_datetime< TIMESTAMP '2024-02-01'
    ) TO 's3://staging/stg_trips.parquet' (FORMAT parquet);''')
    return "Đã dựng xong silver."

def ensure_gold():
    ensure_silver()
    if fs.exists("curated/fct_trips_daily.parquet"): return "gold đã sẵn sàng."
    con = duck()
    con.execute('''COPY (SELECT LocationID zone_id, Borough borough, Zone zone_name, service_zone
        FROM read_parquet('s3://raw/nyc_taxi/taxi_zones.parquet'))
        TO 's3://curated/dim_zone.parquet' (FORMAT parquet);''')
    con.execute('''COPY (
      SELECT t.pickup_date, t.pu_zone_id, count(*) trip_cnt,
             round(sum(t.total_amount),2) revenue, round(avg(t.trip_distance),2) avg_distance,
             round(sum(t.tip_amount)/nullif(sum(t.fare_amount),0),4) tip_rate,
             any_value(w.temp_mean) temp_mean, any_value(w.precip_sum) precip_sum
      FROM read_parquet('s3://staging/stg_trips.parquet') t
      LEFT JOIN read_parquet('s3://raw/external/weather_daily.parquet') w
             ON t.pickup_date = CAST(w.date AS DATE)
      GROUP BY t.pickup_date, t.pu_zone_id
    ) TO 's3://curated/fct_trips_daily.parquet' (FORMAT parquet);''')
    return "Đã dựng xong gold."

print("Đã nạp hàm tiền đề: ensure_raw / ensure_silver / ensure_gold")

from sqlalchemy import create_engine
r = ensure_raw(); print(r)   # bảo đảm có sẵn File+API+seed source_db
for tbl in ["driver","payment_type","calendar"]:
    df = pd.read_sql(f"SELECT * FROM {tbl}", create_engine(CFG["pg_source"]))
    df.to_parquet(f"s3://raw/oltp/{tbl}.parquet", storage_options=S3_OPTS, index=False)
    print(f"✅ raw/oltp/{tbl}: {df.shape}")

con = duck()
display(con.execute('''
  SELECT count(*) tong,
         sum(CASE WHEN fare_amount<=0 THEN 1 ELSE 0 END) fare_loi,
         sum(CASE WHEN trip_distance<=0 THEN 1 ELSE 0 END) distance_loi,
         sum(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) thieu_hanh_khach
  FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')''').df())

con = duck()
con.execute('''CREATE OR REPLACE TABLE stg_trips AS
  SELECT CAST(tpep_pickup_datetime AS TIMESTAMP) pickup_ts,
         CAST(tpep_pickup_datetime AS DATE) pickup_date,
         EXTRACT(hour  FROM tpep_pickup_datetime) pickup_hour,
         EXTRACT(dow   FROM tpep_pickup_datetime) pickup_dow,
         PULocationID pu_zone_id, DOLocationID do_zone_id,
         passenger_count, trip_distance, fare_amount, tip_amount, total_amount, payment_type
  FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')
  WHERE fare_amount>0 AND trip_distance>0 AND trip_distance<100
    AND tpep_pickup_datetime>=TIMESTAMP '2024-01-01'
    AND tpep_pickup_datetime< TIMESTAMP '2024-02-01';''')
before = con.execute("SELECT count(*) FROM read_parquet('s3://raw/nyc_taxi/yellow_taxi.parquet')").fetchone()[0]
after  = con.execute("SELECT count(*) FROM stg_trips").fetchone()[0]
print(f"Trước lọc: {before} -> Sau lọc: {after} (loại {before-after} bản ghi lỗi)")
con.execute("COPY stg_trips TO 's3://staging/stg_trips.parquet' (FORMAT parquet);")
print("✅ Đã ghi Silver: s3://staging/stg_trips.parquet")

con = duck()
display(con.execute('''SELECT pickup_hour, count(*) so_chuyen, round(avg(total_amount),2) tb_tien
    FROM read_parquet('s3://staging/stg_trips.parquet')
    GROUP BY 1 ORDER BY 1''').df())

# TODO Bài tập 1


# TODO Bài tập 2


