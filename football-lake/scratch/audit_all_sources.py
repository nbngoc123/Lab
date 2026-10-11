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

print("1. RÀ SOÁT TẦNG RAW (LAKE_RAW)")
try:
    raw_files = con.execute("SELECT file FROM glob('s3://football-lake/lake_raw/**/*.parquet')").df()
    raw_files['source'] = raw_files['file'].apply(lambda x: x.split('/')[4] if len(x.split('/'))>4 else 'unknown')
    raw_summary = raw_files['source'].value_counts()
    print("Các thư mục raw chính và số lượng file:")
    print(raw_summary)
    
    # Query count of matches in some main raw tables
    print("\nĐang đếm số records trong các bảng RAW chính...")
    queries = {
        "RAW_FDO": "SELECT count(*) FROM read_parquet('s3://football-lake/lake_raw/postgres_football_source_public_fdo_matches/**/*.parquet')",
        "RAW_COUK": "SELECT count(*) FROM read_parquet('s3://football-lake/lake_raw/postgres_football_source_public_couk_matches/**/*.parquet')",
        "RAW_OPENLIGA": "SELECT count(*) FROM read_parquet('s3://football-lake/lake_raw/postgres_football_source_public_openliga_matches/**/*.parquet')",
        "RAW_UNDERSTAT": "SELECT count(*) FROM read_parquet('s3://football-lake/lake_raw/postgres_football_source_public_understat_matches/**/*.parquet')",
        "RAW_API_FOOTBALL": "SELECT count(*) FROM read_parquet('s3://football-lake/lake_raw/api_football/**/*.parquet')"
    }
    for name, q in queries.items():
        try:
            cnt = con.execute(q).fetchone()[0]
            print(f"{name}: {cnt} records")
        except Exception as e:
            print(f"{name}: LỖI - {str(e)[:50]}")
except Exception as e:
    print("Lỗi khi đọc RAW:", e)

print("\n=======================================================")
print("2. RÀ SOÁT TẦNG STAGING & CORE (DWH)")
sources = ['fdo', 'couk', 'understat', 'api_football', 'openliga', 'odds_api']

results = []
for src in sources:
    row = {'Nguồn': src.upper()}
    
    # STG
    try:
        if src == 'api_football':
            cnt = con.execute("SELECT count(*) FROM read_parquet('s3://football-lake/dwh/stg_af_fixtures.parquet')").fetchone()[0]
        elif src == 'odds_api':
            cnt = con.execute("SELECT count(*) FROM read_parquet('s3://football-lake/dwh/stg_odds_api_events.parquet')").fetchone()[0]
        else:
            cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/stg_{src}_matches.parquet')").fetchone()[0]
        row['Staging'] = cnt
    except Exception as e:
        row['Staging'] = "N/A"
        
    # INT / CORE PREP
    try:
        if src == 'api_football':
            cnt = con.execute("SELECT count(*) FROM read_parquet('s3://football-lake/dwh/int_af_matches.parquet')").fetchone()[0]
        elif src == 'odds_api':
            cnt = con.execute("SELECT count(*) FROM read_parquet('s3://football-lake/dwh/int_odds_api_matches.parquet')").fetchone()[0]
        else:
            cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/int_{src}_matches.parquet')").fetchone()[0]
        row['Core_Int'] = cnt
    except Exception as e:
        row['Core_Int'] = "N/A"

    # FACT_MATCH (Tồn tại trong bảng fact chính)
    try:
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/fact_match.parquet') WHERE in_{src} = true").fetchone()[0]
        row['Fact_Match'] = cnt
    except Exception as e:
        row['Fact_Match'] = "N/A"
        
    # MART_ML_MATCH_FEATURES
    try:
        cnt = con.execute(f"SELECT count(*) FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet') WHERE in_{src} = true").fetchone()[0]
        row['Mart_ML'] = cnt
    except Exception as e:
        row['Mart_ML'] = "N/A"
        
    results.append(row)

df_audit = pd.DataFrame(results)
print(df_audit.to_string(index=False))

print("\n=======================================================")
print("3. PHÂN TÍCH DROPOFF (MẤT MÁT DỮ LIỆU)")
print("So sánh lượng records giữa Staging -> Fact_Match -> Mart_ML để xem nguồn nào bị rụng nhiều nhất.")
