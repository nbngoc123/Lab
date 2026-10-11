import duckdb
import pandas as pd
pd.set_option('display.max_rows', 200)

con = duckdb.connect()
con.execute('INSTALL httpfs; LOAD httpfs;')
con.execute('''
    CREATE SECRET m (
        TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
        ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
    );
''')

print("=== DEEP EDA: MATCH FEATURES ===")
df = con.execute("SELECT * FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet')").df()
print(f"Tổng số trận đấu: {len(df)}")

print("\n--- TỶ LỆ DỮ LIỆU BỊ THIẾU TỪNG CỘT ---")
missing_pct = (df.isnull().sum() / len(df)) * 100
missing_pct = missing_pct[missing_pct > 0].sort_values(ascending=False)
print(missing_pct.head(20).to_string())

print("\n--- PHÂN BỐ GIẢI ĐẤU ---")
print(df['competition_key'].value_counts().head(10))

print("\n--- TỶ LỆ CÓ TỶ LỆ CƯỢC (ODDS) THEO TIER ---")
df['has_odds'] = df['f_mkt_p_home'].notnull()
odds_by_tier = df.groupby('competition_key')['has_odds'].mean().sort_values(ascending=False) * 100
print(odds_by_tier.to_string())

df_premium = df[df['has_odds'] == True]
print(f"\nSố trận Premium (Có Odds): {len(df_premium)}")
