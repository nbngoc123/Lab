import duckdb
import pandas as pd

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

con = duckdb.connect()
con.execute('INSTALL httpfs; LOAD httpfs;')
con.execute('''
    CREATE SECRET m (
        TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
        ENDPOINT '20.41.113.183:9000', URL_STYLE 'path', USE_SSL 'false'
    );
''')

print("=== KIỂM TRA ĐƯỜNG DẪN LAKE_RAW ===")
try:
    # Chỉ lấy các parquet file trong lake_raw
    raw_files = con.execute("SELECT file FROM glob('s3://football-lake/lake_raw/**/*.parquet')").df()
    if not raw_files.empty:
        raw_files['folder'] = raw_files['file'].apply(lambda x: x.split('lake_raw/')[1].split('/')[0] if 'lake_raw/' in x else 'unknown')
        print(raw_files['folder'].value_counts())
    else:
        print("Không tìm thấy file parquet nào trong lake_raw/**/*.parquet, thử đuôi khác...")
        all_raw = con.execute("SELECT file FROM glob('s3://football-lake/lake_raw/**/*')").df()
        if not all_raw.empty:
            all_raw['ext'] = all_raw['file'].apply(lambda x: x.split('.')[-1] if '.' in x else 'none')
            print(all_raw['ext'].value_counts())
except Exception as e:
    print("Lỗi RAW:", e)


print("\n=======================================================")
print("=== RÀ SOÁT TẦNG STAGING & CORE (DWH) ===")
sources = {
    'fdo': 'stg_football_data_org_matches',
    'couk': 'stg_football_data_co_uk',
    'understat': 'stg_understat_matches',
    'api_football': 'stg_fixtures',
    'openliga': 'stg_openliga_matches'
}

results = []
for src_name, stg_table in sources.items():
    row = {'Nguồn': src_name.upper()}
    
    # STG
    try:
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/staging/{stg_table}.parquet')").fetchone()[0]
        row['Staging'] = cnt
    except Exception as e:
        row['Staging'] = f"Lỗi: {e}"
        
    # FACT_MATCH
    try:
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/fact_match.parquet') WHERE in_{src_name} = true").fetchone()[0]
        row['Fact_Match'] = cnt
    except Exception as e:
        row['Fact_Match'] = f"Lỗi"
        
    # MART_ML_MATCH_FEATURES
    try:
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet') WHERE in_{src_name} = true").fetchone()[0]
        row['Mart_ML'] = cnt
    except Exception as e:
        row['Mart_ML'] = f"Lỗi"
        
    results.append(row)

df_audit = pd.DataFrame(results)
print(df_audit.to_string(index=False))

print("\n=======================================================")
print("=== TỔNG HỢP TOÀN BỘ CÁC NGUỒN VÀ SỰ THAY ĐỔI ===")
