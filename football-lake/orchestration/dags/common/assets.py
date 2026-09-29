from airflow.sdk import Asset

# ==========================================
# raw ASSETS (MinIO Data Lake)
# ==========================================
football_fixtures_raw = Asset("minio://football-lake/raw/api_football/fixtures")
football_teams_raw = Asset("minio://football-lake/raw/api_football/teams")
football_players_raw = Asset("minio://football-lake/raw/api_football/players")

odds_raw = Asset("minio://football-lake/raw/odds")
reddit_raw = Asset("minio://football-lake/raw/reddit")
news_raw = Asset("minio://football-lake/raw/news")

# ==========================================
# RAW ASSETS (Snowflake)
# ==========================================
football_fixtures_raw = Asset("snowflake://FOOTBALL_DWH/RAW/FIXTURES")
football_teams_raw = Asset("snowflake://FOOTBALL_DWH/RAW/TEAMS")
football_players_raw = Asset("snowflake://FOOTBALL_DWH/RAW/PLAYERS")

odds_raw = Asset("snowflake://FOOTBALL_DWH/RAW/ODDS")

# ==========================================
# STAGING ASSETS (dbt - Snowflake)
# ==========================================
stg_fixtures = Asset("snowflake://FOOTBALL_DWH/STAGING/STG_FIXTURES")
stg_teams = Asset("snowflake://FOOTBALL_DWH/STAGING/STG_TEAMS")
stg_odds = Asset("snowflake://FOOTBALL_DWH/STAGING/STG_ODDS")

# ==========================================
# CORE ASSETS (dbt - Snowflake)
# ==========================================
dim_team = Asset("snowflake://FOOTBALL_DWH/CORE/DIM_TEAM")
dim_player = Asset("snowflake://FOOTBALL_DWH/CORE/DIM_PLAYER")
fact_match = Asset("snowflake://FOOTBALL_DWH/CORE/FACT_MATCH")
fact_odds = Asset("snowflake://FOOTBALL_DWH/CORE/FACT_ODDS")

# ==========================================
# MART ASSETS (dbt - Snowflake)
# ==========================================
football_obt = Asset("snowflake://FOOTBALL_DWH/MART/FOOTBALL_OBT")
team_performance = Asset("snowflake://FOOTBALL_DWH/MART/TEAM_PERFORMANCE")
