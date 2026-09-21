import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "master_pipeline_test_mode",
    default_args=default_args,
    description="Chạy toàn bộ Data Lake ở chế độ TEST (lấy ít dữ liệu) một cách tuần tự để tránh sập VM",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["test", "master"],
) as dag:
    
    # Định nghĩa môi trường test
    env_vars = {"TEST_MODE": "1", "PYTHONPATH": "/opt/project"}
    
    # Các task chạy tuần tự (tránh song song gây OOM)
    # Lưu ý: Các pipeline được gọi qua BashOperator để giải phóng bộ nhớ (RAM) ngay sau khi chạy xong
    t_p03 = BashOperator(task_id="p03_football_data", bash_command="python /opt/project/pipelines/p03/main.py", env=env_vars)
    t_p06 = BashOperator(task_id="p06_wikidata", bash_command="python /opt/project/pipelines/p06/main.py", env=env_vars)
    t_p08 = BashOperator(task_id="p08_thesportsdb", bash_command="python /opt/project/pipelines/p08/main.py", env=env_vars)
    t_p09 = BashOperator(task_id="p09_api_football", bash_command="python /opt/project/pipelines/p09/main.py", env=env_vars)
    t_p10 = BashOperator(task_id="p10_wikimedia", bash_command="python /opt/project/pipelines/p10/main.py", env=env_vars)
    t_p13 = BashOperator(task_id="p13_open_meteo", bash_command="python /opt/project/pipelines/p13/main.py", env=env_vars)
    t_p16 = BashOperator(task_id="p16_physioroom", bash_command="python /opt/project/pipelines/p16/main.py", env=env_vars)
    t_p19 = BashOperator(task_id="p19_understat", bash_command="python /opt/project/pipelines/p19/main.py", env=env_vars)
    t_p21 = BashOperator(task_id="p21_youtube", bash_command="python /opt/project/pipelines/p21/main.py", env=env_vars)
    t_p22 = BashOperator(task_id="p22_odds", bash_command="python /opt/project/pipelines/p22/main.py", env=env_vars)
    t_p24 = BashOperator(task_id="p24_api_football", bash_command="python /opt/project/pipelines/p24/main.py", env=env_vars)
    t_p25 = BashOperator(task_id="p25_news", bash_command="python /opt/project/pipelines/p25/main.py", env=env_vars)
    
    # Thiết lập chạy TUẦN TỰ: Từng task một để tiết kiệm RAM tối đa
    t_p03 >> t_p06 >> t_p08 >> t_p09 >> t_p10 >> t_p13 >> t_p16 >> t_p19 >> t_p21 >> t_p22 >> t_p24 >> t_p25
