import duckdb
import pandas as pd
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 20)
try:
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute('''
        CREATE SECRET m (
            TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
        );
    ''')
    files = con.execute("SELECT file FROM glob('s3://football-lake/dwh/mart_ml_*.parquet')").fetchall()
    print('--- DANH SACH FILE ML MART ---')
    for f in files:
        file_path = f[0]
        print(file_path)
        try:
            df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{file_path}')").df()
            print(f"Schema: {file_path}")
            print(df[['column_name', 'column_type']].to_string())
            print('-' * 40)
        except Exception as ex:
            print("Loi doc schema:", ex)
except Exception as e:
    print('Error:', e)
