import sys
from pathlib import Path
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

# Safeguard to ensure Airflow finds the pipelines module
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

with DAG(
    dag_id="test_p01_fpl_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["fpl", "test", "bronze", "silver"],
) as dag:

    @task
    def ingest_bootstrap():
        from pipelines.p01.main import ingest_bootstrap
        return ingest_bootstrap()

    @task
    def ingest_fixtures():
        from pipelines.p01.main import ingest_fixtures
        return ingest_fixtures()

    @task
    def ingest_player_histories(pids):
        from pipelines.p01.main import ingest_player_histories
        ingest_player_histories(pids)

    @task
    def ingest_live_gw():
        from lake.minio_io import read_json_gz, today
        from pipelines.p01.main import ingest_live_gw
        D = today()
        bs = read_json_gz(f"bronze/fpl/bootstrap_static/ingest_date={D}/bootstrap.json.gz")
        current = next((e["id"] for e in bs.get("events", []) if e.get("is_current")), 1)
        ingest_live_gw(current)

    @task
    def build_player_dim():
        from pipelines.p01.main import build_player_dim
        build_player_dim()

    @task
    def build_player_gw_fact(pids):
        from pipelines.p01.main import build_player_gw_fact
        build_player_gw_fact(pids)

    @task
    def build_fixtures():
        from pipelines.p01.main import build_fixtures
        build_fixtures()

    # --- DEFINE DEPENDENCIES ---
    
    # 1. Bronze Layer Extraction
    pids = ingest_bootstrap()
    t_fx = ingest_fixtures()
    t_hist = ingest_player_histories(pids)
    
    # live_gw needs bootstrap data to find the current gameweek
    t_live = ingest_live_gw()
    pids >> t_live
    
    # 2. Silver Layer Transformation
    t_dim = build_player_dim()
    t_fact = build_player_gw_fact(pids)
    t_fx_silver = build_fixtures()

    # Set up orchestration flow
    pids >> t_dim
    t_hist >> t_fact
    [t_fx, pids] >> t_fx_silver
