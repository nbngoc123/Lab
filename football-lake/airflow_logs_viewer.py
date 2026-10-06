import requests
from requests.auth import HTTPBasicAuth
import json
import sys

# Cấu hình kết nối tới Airflow
AIRFLOW_URL = "http://localhost:8080/api/v1"
AUTH = HTTPBasicAuth("admin", "admin")

def get_dags():
    response = requests.get(f"{AIRFLOW_URL}/dags", auth=AUTH)
    if response.status_code == 200:
        return [dag['dag_id'] for dag in response.json().get('dags', [])]
    return []

def get_latest_dag_runs(dag_id, limit=3):
    response = requests.get(f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns?limit={limit}&order_by=-logical_date", auth=AUTH)
    if response.status_code == 200:
        return response.json().get('dag_runs', [])
    return []

def get_task_instances(dag_id, dag_run_id):
    response = requests.get(f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances", auth=AUTH)
    if response.status_code == 200:
        return response.json().get('task_instances', [])
    return []

def get_task_log(dag_id, dag_run_id, task_id, try_number):
    response = requests.get(f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}", auth=AUTH)
    if response.status_code == 200:
        return response.text
    return f"Không thể lấy log (HTTP {response.status_code})"

def main():
    print("=== CÔNG CỤ XEM TRẠNG THÁI & LOG AIRFLOW ===")
    dags = get_dags()
    if not dags:
        print("Không tìm thấy DAG nào hoặc không thể kết nối tới Airflow (localhost:8080).")
        return

    print("Danh sách DAG:")
    for i, dag in enumerate(dags):
        print(f" {i+1}. {dag}")
    
    choice = input("\nChọn số của DAG muốn xem log (hoặc 'q' để thoát): ")
    if choice.lower() == 'q':
        return
    try:
        dag_idx = int(choice) - 1
        dag_id = dags[dag_idx]
    except:
        print("Lựa chọn không hợp lệ.")
        return

    runs = get_latest_dag_runs(dag_id)
    if not runs:
        print("DAG này chưa có lần chạy nào.")
        return

    print(f"\n3 lần chạy gần nhất của DAG '{dag_id}':")
    for i, run in enumerate(runs):
        print(f" [{i+1}] ID: {run['dag_run_id']} | Trạng thái: {run['state']} | Bắt đầu: {run['start_date']}")

    run_choice = input("\nChọn lần chạy muốn xem (1-3): ")
    try:
        run_idx = int(run_choice) - 1
        selected_run = runs[run_idx]
    except:
        print("Lựa chọn không hợp lệ.")
        return

    print(f"\nĐang tải các task của lần chạy {selected_run['dag_run_id']}...")
    tasks = get_task_instances(dag_id, selected_run['dag_run_id'])
    
    if not tasks:
        print("Không có task nào trong lần chạy này.")
        return

    print("\nDanh sách Tasks:")
    for i, task in enumerate(tasks):
        print(f" {i+1}. Task: {task['task_id']} | Trạng thái: {task['state']} | Lần thử (Try): {task['try_number']}")

    task_choice = input("\nChọn Task muốn lấy log (hoặc nhấn Enter để bỏ qua): ")
    if not task_choice.strip():
        return
    try:
        task_idx = int(task_choice) - 1
        selected_task = tasks[task_idx]
    except:
        print("Lựa chọn không hợp lệ.")
        return

    print(f"\n--- ĐANG KÉO LOG CỦA TASK '{selected_task['task_id']}' ---")
    log_content = get_task_log(dag_id, selected_run['dag_run_id'], selected_task['task_id'], selected_task['try_number'])
    
    # Airflow logs trả về có thể kèm theo một số metadata hoặc nhiều dòng dài, in ra 50 dòng cuối cho dễ nhìn
    lines = log_content.split('\n')
    print("\n".join(lines[-100:])) # Chỉ in 100 dòng cuối
    print("\n--- KẾT THÚC LOG ---")
    print(f"(Chỉ hiển thị 100 dòng cuối. Tổng số dòng: {len(lines)})")

if __name__ == "__main__":
    main()
