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

print("=== KIỂM TRA DROPOFF CÁC THỰC THỂ KHÁC (GOALS, SHOTS, PLAYERS, NLP) ===")

def get_count(table_path):
    try:
        return con.execute(f"SELECT count(*) FROM read_parquet('{table_path}')").fetchone()[0]
    except Exception as e:
        return f"Lỗi"

audits = [
    {
        "Thực Thể": "GOALS (OpenLiga)",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_openliga_goals.parquet"),
        "Core": get_count("s3://football-lake/dwh/fact_goal.parquet"),
        "Mart": get_count("s3://football-lake/dwh/mart_ml_goals.parquet")
    },
    {
        "Thực Thể": "SHOTS (Understat)",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_understat_shots.parquet"),
        "Core": "N/A (Bắn thẳng ra Mart)",
        "Mart": get_count("s3://football-lake/dwh/mart_ml_shots.parquet")
    },
    {
        "Thực Thể": "PLAYERS (Understat)",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_understat_players.parquet"),
        "Core": get_count("s3://football-lake/dwh/dim_player.parquet"),
        "Mart": "N/A"
    },
    {
        "Thực Thể": "PLAYERS (FDO)",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_football_data_org_players.parquet"),
        "Core": "Gộp vào dim_player",
        "Mart": "N/A"
    },
    {
        "Thực Thể": "NLP - YouTube Comments",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_youtube_comments.parquet"),
        "Core": "N/A",
        "Mart": get_count("s3://football-lake/dwh/mart_ml_text_youtube_comments.parquet")
    },
    {
        "Thực Thể": "NLP - Football News",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_football_news.parquet"),
        "Core": "N/A",
        "Mart": get_count("s3://football-lake/dwh/mart_ml_text_football_news.parquet")
    },
    {
        "Thực Thể": "NLP - Google News",
        "Staging": get_count("s3://football-lake/dwh/staging/stg_google_news.parquet"),
        "Core": "N/A",
        "Mart": get_count("s3://football-lake/dwh/mart_ml_text_google_news.parquet")
    }
]

df_audit = pd.DataFrame(audits)
print(df_audit.to_string(index=False))

print("\n--- CHI TIẾT TÌNH TRẠNG MẤT DỮ LIỆU GOALS ---")
try:
    missing_goals = con.execute("""
        SELECT count(*) 
        FROM read_parquet('s3://football-lake/dwh/staging/stg_openliga_goals.parquet') g
        LEFT JOIN read_parquet('s3://football-lake/dwh/fact_goal.parquet') c 
          ON cast(g.goal_id as varchar) = c.openliga_goal_id
        WHERE c.openliga_goal_id IS NULL
    """).fetchone()[0]
    print(f"Số lượng bàn thắng (Goals) có ở Staging nhưng bị vứt khi lên Core: {missing_goals}")
except Exception as e:
    print(e)
