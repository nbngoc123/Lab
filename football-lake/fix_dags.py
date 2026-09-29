import os

dags_dir = r"d:\O\DOC\Năm 4\Data Mining\football-lake\orchestration\dags\ingestion"

dag_template = '''from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.assets import Asset

asset_bronze = Asset("minio://football-lake/bronze/{source_name}")

with DAG(
    dag_id="{dag_id}",
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=["ingestion", "bronze", "{source_name}"],
) as dag:

    @task
    def task_get_partitions():
        from pipelines.{p_name}.main import {get_func}
        return {get_func}()

    @task(outlets=[asset_bronze])
    def task_ingest(partition: dict):
        from pipelines.{p_name}.main import {ingest_func}
        return {ingest_func}(partition)

    partitions = task_get_partitions()
    ingest_tasks = task_ingest.expand(partition=partitions)
'''

dags = [
    ("p10", "wikimedia", "dag_ingest_p10_wikimedia.py", "get_partitions", "ingest"),
    ("p13", "open_meteo", "dag_ingest_p13_open_meteo.py", "get_partitions", "ingest"),
    ("p16", "physioroom", "dag_ingest_p16_physioroom.py", "get_partitions", "ingest"),
    ("p19", "understat", "dag_ingest_p19_understat.py", "get_partitions", "ingest"),
    ("p20", "google_news", "dag_ingest_p20_google_news.py", "get_partitions_news", "ingest_news"),
    ("p20", "wikipedia", "dag_ingest_p20_wikipedia.py", "get_partitions_wiki", "ingest_wiki"),
    ("p21", "youtube", "dag_ingest_p21_youtube.py", "get_partitions", "ingest"),
    ("p25", "football_news", "dag_ingest_p25_football_news.py", "get_partitions", "ingest"),
]

for p, src, filename, get_f, in_f in dags:
    content = dag_template.format(p_name=p, source_name=src, dag_id=filename.replace(".py", ""), get_func=get_f, ingest_func=in_f)
    path = os.path.join(dags_dir, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)

print("Updated DAGs to match new python modules.")
