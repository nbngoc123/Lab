from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.assets import Asset

# Khai báo Asset
asset_football_data_bronze = Asset("minio://football-lake/bronze/football_data")

with DAG(
    dag_id="dag_ingest_p09_football_data",
    start_date=datetime(2023, 1, 1),
    schedule='@daily', # Airflow 3: có thể trigger theo Asset nếu muốn, nhưng đây là ingestion nên hẹn giờ hoặc trigger ngoài
    catchup=False,
    tags=["ingestion", "bronze", "football_data"],
) as dag:

    @task
    def get_partitions():
        """Lấy danh sách các partition (ví dụ: ngày, mùa giải, giải đấu)."""
        return ["2026-09-27", "2026-09-28", "2026-09-29"] # Giả lập partition theo ngày (Data-aware partitioning)

    @task(outlets=[asset_football_data_bronze])
    def ingest_to_bronze(partition_date: str):
        """
        Chỉ tập trung Ingest data vào MinIO (lớp Bronze).
        Sử dụng Dynamic Task Mapping để lấy dữ liệu song song.
        """
        print(f"Fetching data for partition: {partition_date}")
        # from pipelines.p09.main import ingest
        # ingest(partition_date)
        return f"Ingested partition {partition_date}"

    # 1. Lấy danh sách partition
    partitions = get_partitions()
    
    # 2. Dynamic Task Mapping -> Fetch song song & Ghi vào MinIO
    # 3. Phát ra Asset (Asset cập nhật sẽ tự động kích hoạt downstream DAGs như Snowflake load)
    ingest_tasks = ingest_to_bronze.expand(partition_date=partitions)
