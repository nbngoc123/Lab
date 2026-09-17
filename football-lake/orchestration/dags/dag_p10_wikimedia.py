import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

with DAG(
    dag_id="p10_wikimedia_pageviews",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["wikimedia", "pageviews", "bronze", "silver", "timeseries"],
) as dag:

    @task
    def task_discover_players():
        """Load full player list từ Silver fdo_players (p09) + seed list."""
        from pipelines.p10.main import load_players_from_silver, SEED_PLAYERS
        auto = load_players_from_silver()
        merged = {**auto, **SEED_PLAYERS}
        print(f"  · Tổng: {len(merged)} players (seed={len(SEED_PLAYERS)}, auto={len(auto)})")
        return merged

    @task
    def task_ingest_teams():
        from pipelines.p10.main import ingest_entities_multilang, TEAMS
        return ingest_entities_multilang(TEAMS, "team")

    @task
    def task_ingest_players(all_players: dict):
        from pipelines.p10.main import ingest_entities_multilang
        return ingest_entities_multilang(all_players, "player")

    @task
    def task_ingest_top_viral():
        from pipelines.p10.main import ingest_top_daily_multilang
        ingest_top_daily_multilang(n_days=7)

    @task
    def task_build_silver(team_results: list, player_results: list):
        import pandas as pd
        from pipelines.p10.main import build_silver_timeseries, build_silver_spikes, summary
        team_df   = build_silver_timeseries(team_results, "team")
        player_df = build_silver_timeseries(player_results, "player")
        build_silver_spikes(team_df, player_df)
        summary("bronze/wikimedia_pageviews/")
        summary("silver/text/wm_pageviews/")
        summary("silver/text/wm_pageviews_multilang/")

    # Define dependencies
    discovered = task_discover_players()
    team_res   = task_ingest_teams()
    player_res = task_ingest_players(discovered)
    top_task   = task_ingest_top_viral()

    silver_task = task_build_silver(team_res, player_res)

    [team_res, player_res, top_task] >> silver_task
