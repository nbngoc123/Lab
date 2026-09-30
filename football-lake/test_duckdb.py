import duckdb

conn = duckdb.connect(':memory:')
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("SET s3_endpoint='localhost:9000'; SET s3_access_key_id='minioadmin'; SET s3_secret_access_key='minioadmin123'; SET s3_use_ssl=false; SET s3_url_style='path';")

try:
    df = conn.execute("DESCRIBE SELECT * FROM read_json_auto('s3://football-lake/raw/google_news/**/*.json.gz')").df()
    print(df.to_string())
except Exception as e:
    print(f"Error: {e}")
