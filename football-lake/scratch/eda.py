import duckdb
import pandas as pd
pd.set_option('display.max_rows', 200)

try:
    con = duckdb.connect()
    con.execute('INSTALL httpfs; LOAD httpfs;')
    con.execute('''
        CREATE SECRET m (
            TYPE S3, KEY_ID 'minioadmin', SECRET 'minioadmin123', 
            ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL 'false'
        );
    ''')

    print("=== EDA: MATCH FEATURES ===")
    df_match = con.execute("SELECT y_result, count(*) as count FROM read_parquet('s3://football-lake/dwh/mart_ml_match_features.parquet') GROUP BY y_result").df()
    print("Phân phối kết quả (y_result):\\n", df_match)

    print("\\n=== EDA: SHOTS ===")
    df_shots = con.execute("SELECT y_is_goal, count(*) as count FROM read_parquet('s3://football-lake/dwh/mart_ml_shots.parquet') GROUP BY y_is_goal").df()
    print("Phân phối cú sút thành bàn (y_is_goal):\\n", df_shots)

    print("\\n=== EDA: YOUTUBE COMMENTS ===")
    df_yt = con.execute("SELECT count(*) as total_comments, avg(n_words) as avg_words FROM read_parquet('s3://football-lake/dwh/mart_ml_text_youtube_comments.parquet')").df()
    print(df_yt)

    print("\\n=== EDA: GOOGLE NEWS ===")
    df_news = con.execute("SELECT count(*) as total_news, avg(n_words) as avg_words FROM read_parquet('s3://football-lake/dwh/mart_ml_text_google_news.parquet')").df()
    print(df_news)

except Exception as e:
    print('Error:', e)
