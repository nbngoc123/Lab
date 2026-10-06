import subprocess
import sys
import re

def run_docker_cmd(cmd):
    """Chạy lệnh bên trong container airflow-scheduler và trả về output"""
    docker_cmd = ["docker", "exec", "airflow-scheduler", "bash", "-c", cmd]
    try:
        result = subprocess.run(docker_cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip().split('\n')
    except subprocess.CalledProcessError as e:
        return []

def get_subdirs(path):
    """Lấy danh sách các thư mục con trong một đường dẫn"""
    lines = run_docker_cmd(f"ls -1t {path} 2>/dev/null")
    return [d for d in lines if d]

def main():
    print("=== CÔNG CỤ XEM LOG AIRFLOW (Qua Docker) ===\n")
    
    # 1. Chọn DAG
    base_log_dir = "/opt/airflow/logs"
    dags_raw = get_subdirs(base_log_dir)
    dags = [d.replace('dag_id=', '') for d in dags_raw if d.startswith('dag_id=')]
    
    if not dags:
        print("Không tìm thấy log của bất kỳ DAG nào!")
        return

    print("Danh sách DAG đã chạy:")
    for i, d in enumerate(dags):
        print(f" {i+1}. {d}")
    
    dag_choice = input("\nChọn số của DAG (hoặc 'q' để thoát): ")
    if dag_choice.lower() == 'q': return
    try:
        selected_dag = dags[int(dag_choice) - 1]
    except:
        print("Không hợp lệ."); return

    # 2. Chọn Run ID
    dag_path = f"{base_log_dir}/dag_id={selected_dag}"
    runs_raw = get_subdirs(dag_path)
    runs = [r.replace('run_id=', '') for r in runs_raw if r.startswith('run_id=')]
    
    if not runs:
        print("Không có log cho lần chạy nào của DAG này.")
        return

    print(f"\nCác lần chạy của '{selected_dag}':")
    for i, r in enumerate(runs[:5]): # Chỉ hiển thị 5 lần gần nhất
        print(f" {i+1}. {r}")
        
    run_choice = input("\nChọn lần chạy (1-5): ")
    try:
        selected_run = runs[int(run_choice) - 1]
    except:
        print("Không hợp lệ."); return

    # 3. Chọn Task
    run_path = f"{dag_path}/run_id={selected_run}"
    tasks_raw = get_subdirs(run_path)
    tasks = [t.replace('task_id=', '') for t in tasks_raw if t.startswith('task_id=')]
    
    if not tasks:
        print("Không có log của task nào trong lần chạy này.")
        return

    print(f"\nCác Task trong lần chạy này:")
    for i, t in enumerate(tasks):
        print(f" {i+1}. {t}")
        
    task_choice = input("\nChọn Task muốn xem log: ")
    try:
        selected_task = tasks[int(task_choice) - 1]
    except:
        print("Không hợp lệ."); return

    # 4. Lấy file log mới nhất (attempt lớn nhất)
    task_path = f"{run_path}/task_id={selected_task}"
    attempts = get_subdirs(task_path)
    if not attempts:
        print("Chưa có file log nào được tạo.")
        return
        
    # attempt thường có dạng attempt=1.log. Lấy file đầu tiên do đã sort by time (ls -1t)
    latest_log = attempts[0]
    full_log_path = f"{task_path}/{latest_log}"
    
    print(f"\n--- ĐANG ĐỌC LOG: {selected_task} ({latest_log}) ---")
    
    # Lấy 100 dòng cuối
    log_lines = run_docker_cmd(f"tail -n 100 {full_log_path}")
    
    for line in log_lines:
        print(line)
        
    print("\n--- KẾT THÚC (Chỉ hiển thị 100 dòng cuối) ---")

if __name__ == '__main__':
    main()
