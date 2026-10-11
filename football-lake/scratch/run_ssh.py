import subprocess

script = """
import duckdb
import pandas as pd
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 20)
try:
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute('''
        CREATE SECRET m (
            TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin', 
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
        );
    ''')
    files = con.execute("SELECT file FROM glob('s3://football-lake/dwh/mart_ml_*.parquet')").fetchall()
    print('--- DANH SÁCH FILE ML MART ---')
    for f in files:
        print(f[0])
        try:
            df = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{f[0]}')").df()
            print(f"Schema của {f[0]}:")
            print(df[['column_name', 'column_type']].to_string())
            print('-' * 40)
        except Exception as ex:
            print("Lỗi đọc schema:", ex)
except Exception as e:
    print('Error:', e)
"""

import base64
b64 = base64.b64encode(script.encode()).decode()
cmd = ["ssh", "vm", f"echo {b64} | base64 -d > /tmp/script.py && source venv/bin/activate && python3 /tmp/script.py"]
result = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:")
print(result.stdout)
print("STDERR:")
print(result.stderr)
