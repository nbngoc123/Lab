import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(DAGS_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# StatsBomb là dữ liệu tĩnh -> chạy 1 lần bằng schedule=None, trigger thủ công
with DAG(
    dag_id="p04_statsbomb_open_data",
    start_date=datetime(2023, 1, 1),
    schedule=None,    # Trigger thủ công - dữ liệu tĩnh không cần chạy lại
    catchup=False,
    tags=["statsbomb", "events", "xg", "bronze", "silver"],
) as dag:

    @task
    def task_clone_repo():
        from pipelines.p04.main import clone_repo
        clone_repo()

    @task
    def task_ingest_competitions():
        from pipelines.p04.main import ingest_competitions
        ingest_competitions()

    @task
    def task_ingest_competition(cid_sid: tuple):
        from pipelines.p04.main import ingest_competition
        cid, sid = cid_sid
        return ingest_competition(cid, sid)

    @task
    def task_build_silver(cid_sid: tuple, match_ids: list):
        from pipelines.p04.main import (
            flatten_events, build_shots, build_matches_dim, summary
        )
        cid, sid = cid_sid
        print(f"Flatten {len(match_ids)} trận comp={cid} season={sid}")
        ev = flatten_events(cid, sid, match_ids)
        build_shots(ev, cid, sid)
        build_matches_dim(cid, sid)
        summary("bronze/statsbomb/")
        summary("silver/events/")

    TARGETS = [(43, 106), (55, 43), (11, 90), (2, 44)]

    clone   = task_clone_repo()
    comps   = task_ingest_competitions()

    clone >> comps

    for cid_sid in TARGETS:
        mids = task_ingest_competition.override(
            task_id=f"ingest_comp_{cid_sid[0]}_s{cid_sid[1]}"
        )(cid_sid)
        silver = task_build_silver.override(
            task_id=f"silver_comp_{cid_sid[0]}_s{cid_sid[1]}"
        )(cid_sid, mids)
        comps >> mids >> silver
