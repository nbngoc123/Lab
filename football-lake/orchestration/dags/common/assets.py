from airflow.sdk import Asset

# ==========================================
# BRONZE / RAW ASSETS (MinIO Data Lake)
# ==========================================

# API Football
api_football_fixtures = Asset("minio://football-lake/raw/api_football/fixtures")
api_football_teams = Asset("minio://football-lake/raw/api_football/teams")
api_football_players = Asset("minio://football-lake/raw/api_football/players")

# Major Sources
football_data_co_uk = Asset("minio://football-lake/raw/football_data_couk")
football_data_org = Asset("minio://football-lake/raw/football_data_org")
understat = Asset("minio://football-lake/raw/understat")
thesportsdb = Asset("minio://football-lake/raw/thesportsdb")
open_meteo = Asset("minio://football-lake/raw/open_meteo")
physioroom = Asset("minio://football-lake/raw/physioroom")

# News & Social
football_news = Asset("minio://football-lake/raw/football_news")
google_news = Asset("minio://football-lake/raw/google_news")
youtube = Asset("minio://football-lake/raw/youtube")
reddit = Asset("minio://football-lake/raw/reddit")

# Wiki & Odds
wikimedia = Asset("minio://football-lake/raw/wikimedia_pageviews")
wikidata = Asset("minio://football-lake/raw/wikidata")
wikipedia = Asset("minio://football-lake/raw/wikipedia")
odds_api = Asset("minio://football-lake/raw/odds")

# OpenLigaDB (CDC từ Postgres football_source qua Debezium/Kafka)
openliga = Asset("minio://football-lake/raw/openliga")

# ==========================================
# STAGING ASSETS (dbt - DuckDB)
# ==========================================
# Nhóm Staging Cốt Lõi (Core dependencies)
stg_football_data_co_uk = Asset("duckdb://football-lake/staging/football_data_co_uk")
stg_football_data_org   = Asset("duckdb://football-lake/staging/football_data_org")
stg_understat           = Asset("duckdb://football-lake/staging/understat")
stg_openliga            = Asset("duckdb://football-lake/staging/openliga")
stg_api_football        = Asset("duckdb://football-lake/staging/api_football")
stg_open_meteo          = Asset("duckdb://football-lake/staging/open_meteo")
stg_wikidata            = Asset("duckdb://football-lake/staging/wikidata")
stg_thesportsdb         = Asset("duckdb://football-lake/staging/thesportsdb")

# Nhóm Staging Text & Social Media (ML-only dependencies)
stg_wikimedia           = Asset("duckdb://football-lake/staging/wikimedia")
stg_wikipedia           = Asset("duckdb://football-lake/staging/wikipedia")
stg_google_news         = Asset("duckdb://football-lake/staging/google_news")
stg_youtube             = Asset("duckdb://football-lake/staging/youtube")
stg_football_news       = Asset("duckdb://football-lake/staging/football_news")

dbt_staging_complete    = Asset("duckdb://football-lake/staging/complete")

# ==========================================
# CORE ASSETS (dbt - DuckDB)
# ==========================================
dim_team   = Asset("duckdb://football-lake/core/dim_team")
dim_player = Asset("duckdb://football-lake/core/dim_player")
fact_match = Asset("duckdb://football-lake/core/fact_match")

# ==========================================
# MART ASSETS (dbt - DuckDB & Postgres DWH)
# ==========================================
mart_bi = Asset("duckdb://football-lake/mart/bi")
mart_ml = Asset("duckdb://football-lake/mart/ml")
football_obt = Asset("duckdb://football-lake/mart/football_obt")
