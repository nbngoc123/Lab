import csv
import time
import json
import random
from datetime import datetime
try:
    from kafka import KafkaProducer
except ImportError:
    print("Vui lòng cài đặt thư viện kafka-python: pip install kafka-python")
    exit(1)

# --- CẤU HÌNH ---
CSV_FILE_PATH = r"D:\O\DOC\Năm 4\Data Mining\Data3\employees.csv"
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'attendance_events'
MAX_EMPLOYEES_TO_LOAD = 5000  # Load 5000 nhân viên để test, tránh nặng máy

def load_employee_ids(filepath, max_records=5000):
    print(f"Đang đọc danh sách nhân viên từ: {filepath}")
    emp_ids = []
    try:
        with open(filepath, mode='r', encoding='utf-8') as file:
            reader = csv.reader(file)
            next(reader)  # Bỏ qua header
            count = 0
            for row in reader:
                if row:
                    emp_ids.append(row[0])  # Cột emp_no
                    count += 1
                if count >= max_records:
                    break
        print(f"Đã tải thành công {len(emp_ids)} mã nhân viên để mô phỏng.")
        return emp_ids
    except FileNotFoundError:
        print(f"Không tìm thấy file: {filepath}")
        exit(1)

def main():
    # 1. Khởi tạo Kafka Producer
    print(f"Kết nối tới Kafka Broker tại {KAFKA_BROKER}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kết nối Kafka thành công!")
    except Exception as e:
        print(f"Lỗi kết nối Kafka: {e}")
        print("Đảm bảo Kafka Server (Zookeeper & Broker) đang chạy trên localhost:9092")
        exit(1)

    # 2. Lấy danh sách nhân viên hợp lệ từ file Data3
    employee_ids = load_employee_ids(CSV_FILE_PATH, MAX_EMPLOYEES_TO_LOAD)
    
    locations = ["Main_Gate", "Basement_Parking", "Gate_A", "Gate_B"]
    event_types = ["CHECK_IN", "CHECK_OUT"]

    print(f"\nBắt đầu sinh dữ liệu streaming và gửi tới topic '{KAFKA_TOPIC}' (Nhấn Ctrl+C để dừng)...")
    
    try:
        while True:
            # Random thông tin sự kiện
            event_data = {
                "event_id": f"EVT-{random.randint(100000, 999999)}",
                "emp_no": random.choice(employee_ids),
                "event_type": random.choice(event_types),
                "location": random.choice(locations),
                "timestamp": datetime.now().isoformat()
            }
            
            # Bắn vào Kafka
            producer.send(KAFKA_TOPIC, value=event_data)
            print(f"Sent -> {event_data}")
            
            # Nghỉ ngẫu nhiên từ 0.5 đến 3 giây để trông giống real-time thật
            time.sleep(random.uniform(0.5, 3.0))
            
    except KeyboardInterrupt:
        print("\nĐã dừng sinh dữ liệu.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
