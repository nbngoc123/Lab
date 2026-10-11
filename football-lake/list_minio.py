import duckdb

con = duckdb.connect()
con.execute('INSTALL httpfs; LOAD httpfs;')
con.execute('''
    CREATE SECRET m (
        TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
''')

files = con.execute("SELECT file FROM glob('s3://football-lake/dwh/**/*.parquet')").fetchall()
for f in files:
    print(f[0])

files_raw = con.execute("SELECT file FROM glob('s3://football-lake/lake_raw/**/*.parquet')").fetchall()
for f in files_raw[:20]:
    print(f[0])
