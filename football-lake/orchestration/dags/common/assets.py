from airflow.sdk import Asset

# ==========================================
# BRONZE ASSETS (MinIO Data Lake)
# ==========================================
football_fixtures_raw_in = Asset("minio://football-lake/raw/api_football/fixtures")
football_teams_raw_in = Asset("minio://football-lake/raw/api_football/teams")
football_players_raw_in = Asset("minio://football-lake/raw/api_football/players")

odds_raw_in = Asset("minio://football-lake/raw/odds")
reddit_raw_in = Asset("minio://football-lake/raw/reddit")
news_raw_in = Asset("minio://football-lake/raw/news")

# ==========================================
# RAW ASSETS (Snowflake)
# ==========================================
football_fixtures_raw = Asset("snowflake://my_account/FOOTBALL_DWH/RAW/API_FOOTBALL_FIXTURES")
football_teams_raw = Asset("snowflake://my_account/FOOTBALL_DWH/RAW/API_FOOTBALL_TEAMS")
football_players_raw = Asset("snowflake://my_account/FOOTBALL_DWH/RAW/API_FOOTBALL_PLAYERS")

odds_raw = Asset("snowflake://my_account/FOOTBALL_DWH/RAW/ODDS_API")

# ==========================================
# STAGING ASSETS (dbt - Snowflake)
# ==========================================
stg_fixtures = Asset("snowflake://my_account/FOOTBALL_DWH/STAGING/STG_FIXTURES")
stg_teams = Asset("snowflake://my_account/FOOTBALL_DWH/STAGING/STG_TEAMS")
stg_odds = Asset("snowflake://my_account/FOOTBALL_DWH/STAGING/STG_ODDS")

# ==========================================
# CORE ASSETS (dbt - Snowflake)
# ==========================================
dim_team = Asset("snowflake://my_account/FOOTBALL_DWH/CORE/DIM_TEAM")
dim_player = Asset("snowflake://my_account/FOOTBALL_DWH/CORE/DIM_PLAYER")
fact_match = Asset("snowflake://my_account/FOOTBALL_DWH/CORE/FACT_MATCH")
fact_odds = Asset("snowflake://my_account/FOOTBALL_DWH/CORE/FACT_ODDS")

# ==========================================
# MART ASSETS (dbt - Snowflake)
# ==========================================
football_obt = Asset("snowflake://my_account/FOOTBALL_DWH/MART/FOOTBALL_OBT")
team_performance = Asset("snowflake://my_account/FOOTBALL_DWH/MART/TEAM_PERFORMANCE")
