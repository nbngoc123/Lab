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
    dag_id="p07_reddit_rss_text",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["reddit", "rss", "text", "bronze", "silver"],
) as dag:

    @task
    def task_ingest_reddit():
        from pipelines.p07.main import reddit_client, ingest_reddit
        try:
            return ingest_reddit(reddit_client())
        except Exception as e:
            print(f"  ! Reddit bị bỏ qua: {e}")
            return []

    @task
    def task_ingest_rss():
        from pipelines.p07.main import ingest_rss
        return ingest_rss()

    @task
    def task_build_silver(posts: list, entries: list):
        import pandas as pd
        from pipelines.p07.main import (
            build_reddit_silver, build_news_silver, build_entity_mentions, summary
        )
        rdf = build_reddit_silver(posts)
        ndf = build_news_silver(entries)
        build_entity_mentions(rdf, ndf)
        summary("bronze/reddit/")
        summary("bronze/rss/")
        summary("silver/text/")

    reddit_data = task_ingest_reddit()
    rss_data    = task_ingest_rss()
    task_build_silver(reddit_data, rss_data)
