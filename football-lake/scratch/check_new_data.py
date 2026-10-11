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

print("=== KIỂM TRA DATA MỚI TẠI TẦNG STAGING (OPENLIGA) ===")

try:
    print("\n1. THỐNG KÊ SỐ TRẬN THEO MÙA GIẢI (STG_OPENLIGA_MATCHES)")
    df_seasons = con.execute("""
        SELECT 
            season, 
            count(*) as match_count,
            min(match_time_utc) as min_date,
            max(match_time_utc) as max_date
        FROM read_parquet('s3://football-lake/dwh/staging/stg_openliga_matches.parquet')
        GROUP BY season
        ORDER BY season DESC
    """).df()
    print(df_seasons.to_string(index=False))

    print("\n2. XEM TẬN MẮT 5 TRẬN ĐẤU CŨ NHẤT TỪ MÙA 2020")
    df_samples = con.execute("""
        SELECT 
            season, 
            match_time_utc::date as match_date,
            home_team, 
            away_team, 
            concat(score_team1, ' - ', score_team2) as score
        FROM read_parquet('s3://football-lake/dwh/staging/stg_openliga_matches.parquet')
        WHERE season = 2020
        ORDER BY match_time_utc ASC
        LIMIT 5
    """).df()
    print(df_samples.to_string(index=False))
    
    print("\n3. KIỂM TRA SỐ BÀN THẮNG THEO MÙA (Dựa vào match_id)")
    df_goals = con.execute("""
        SELECT 
            m.season, 
            count(g.goal_id) as total_goals
        FROM read_parquet('s3://football-lake/dwh/staging/stg_openliga_goals.parquet') g
        JOIN read_parquet('s3://football-lake/dwh/staging/stg_openliga_matches.parquet') m 
          ON g.match_id = m.match_id
        GROUP BY m.season
        ORDER BY m.season DESC
    """).df()
    print(df_goals.to_string(index=False))

except Exception as e:
    print("Lỗi:", e)
