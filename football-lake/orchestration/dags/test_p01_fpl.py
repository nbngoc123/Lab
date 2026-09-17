from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Default directory where airflow might run it if mounted correctly, 
# or just run it via python module if PYTHONPATH is configured.
# We'll use a python -m command and assume the dags folder is a sibling of pipelines,
# but to be safe we can point to the absolute path of the script if known, or run the module.

with DAG(
    dag_id="test_p01_fpl_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fpl", "test", "bronze", "silver"],
) as dag:

    # Use BashOperator to execute the pipeline script
    # This assumes the project root is in PYTHONPATH or we run it by absolute path.
    # Since we added sys.path.append in main.py, running it directly works.
    
    # Normally Airflow runs in /opt/airflow. We can cd to the directory containing pipelines if needed,
    # but let's just construct the path relative to the DAG folder.
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(dag_dir))
    script_path = os.path.join(project_root, "pipelines", "p01", "main.py")
    
    run_pipeline = BashOperator(
        task_id="run_p01_ingestion",
        bash_command=f"python '{script_path}'"
    )

    run_pipeline
