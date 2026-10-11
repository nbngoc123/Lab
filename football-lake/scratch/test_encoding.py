import duckdb
import json

con = duckdb.connect()
con.execute('INSTALL httpfs; LOAD httpfs;')
con.execute('''
    CREATE SECRET m (
        TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
        ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
    );
''')

print("1. Kiểm tra đọc dữ liệu JSON thô của Goals từ S3...")
try:
    # Thử đọc trực tiếp toàn bộ dữ liệu xem file nào bị lỗi
    res = con.execute("SELECT filename FROM read_json_objects('s3://football-lake/raw/openliga/goals/**/*.json.gz', filename=true)").fetchall()
    print(f"Đọc được {len(res)} files thành công!")
    
    # Lấy thử 1 file để xem
    if len(res) > 0:
        first_file = res[0][0]
        print(f"Thử đọc file: {first_file}")
        con.execute(f"SELECT * FROM read_json_objects('{first_file}')")
        print("Đọc thành công!")

except Exception as e:
    print("LỖI KHI ĐỌC QUA DUCKDB:", type(e).__name__, e)

print("2. Dùng thư viện gzip của Python để đọc nhị phân (phát hiện byte 0xb0)...")
import urllib.request
import gzip

try:
    files = con.execute("SELECT file FROM glob('s3://football-lake/raw/openliga/goals/**/*.json.gz')").fetchall()
    print(f"Tìm thấy {len(files)} files goals trên S3.")
    for f in files:
        url = f[0].replace('s3://football-lake/', 'http://localhost:9000/football-lake/')
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            compressed_data = response.read()
            try:
                uncompressed_data = gzip.decompress(compressed_data)
                # Thử decode bằng utf-8
                text = uncompressed_data.decode('utf-8')
            except UnicodeDecodeError as ue:
                print(f"\n[BẮT ĐƯỢC LỖI ENCODING] File: {f[0]}")
                print(f"Lỗi: {ue}")
                print(f"Dữ liệu thô quanh byte bị lỗi: {uncompressed_data[ue.start-10:ue.start+10]}")
                break
            except Exception as e2:
                print(f"\n[LỖI GZIP] File: {f[0]}")
                print(f"Lỗi: {e2}")
                break
except Exception as e3:
    print("Lỗi python urllib:", e3)
