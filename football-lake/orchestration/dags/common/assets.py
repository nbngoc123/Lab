from airflow.assets import Asset

# ==========================================
# BRONZE ASSETS (MinIO Data Lake)
# ==========================================
football_fixtures_bronze = Asset("minio://football-lake/bronze/api_football/fixtures")
football_teams_bronze = Asset("minio://football-lake/bronze/api_football/teams")
football_players_bronze = Asset("minio://football-lake/bronze/api_football/players")

odds_bronze = Asset("minio://football-lake/bronze/odds")
reddit_bronze = Asset("minio://football-lake/bronze/reddit")
news_bronze = Asset("minio://football-lake/bronze/news")

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
