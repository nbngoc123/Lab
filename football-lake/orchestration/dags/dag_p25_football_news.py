from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_p25_football_news',
    default_args=default_args,
    description='Cào tin tức từ Football News Aggregator (RapidAPI)',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['football_lake', 'news'],
) as dag:

    run_pipeline = BashOperator(
        task_id='run_p25_pipeline',
        bash_command='python /opt/airflow/pipelines/p25/main.py',
    )
