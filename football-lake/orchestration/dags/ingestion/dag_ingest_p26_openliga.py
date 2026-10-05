from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os
import boto3
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def ingest_from_kafka():
    from confluent_kafka import Consumer, KafkaException
    print("Connecting to Kafka...")
    
    conf = {
        'bootstrap.servers': 'kafka-cdc:29092',
        'group.id': 'airflow_ingestion_group',
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    topics = ['football.public.matches', 'football.public.goals']
    consumer.subscribe(topics)
    
    data = {'matches': [], 'goals': []}
    
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                break # No more messages
            if msg.error():
                raise KafkaException(msg.error())
            
            val = json.loads(msg.value().decode('utf-8'))
            topic = msg.topic()
            
            # Debezium 'after' payload contains the actual row
            if val and 'payload' in val and val['payload'] and val['payload']['after']:
                row = val['payload']['after']
                if 'matches' in topic:
                    data['matches'].append(row)
                elif 'goals' in topic:
                    data['goals'].append(row)
    finally:
        consumer.close()

    print(f"Consumed {len(data['matches'])} matches, {len(data['goals'])} goals.")
    
    if not data['matches'] and not data['goals']:
        print("No new data to ingest.")
        return

    # Upload to MinIO Bronze Layer
    s3 = boto3.client('s3',
                      endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://minio:9000'),
                      aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
                      aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123'))
    
    bucket = os.getenv('MINIO_BUCKET', 'football-lake')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for entity in ['matches', 'goals']:
        if data[entity]:
            df = pd.DataFrame(data[entity])
            local_path = f"/tmp/{entity}_{timestamp}.parquet"
            s3_path = f"bronze/{entity}/{entity}_{timestamp}.parquet"
            
            df.to_parquet(local_path, index=False)
            s3.upload_file(local_path, bucket, s3_path)
            os.remove(local_path)
            print(f"Uploaded {s3_path} to MinIO")

with DAG(
    'p26_ingest_openliga_cdc',
    default_args=default_args,
    description='INGEST: Đọc dữ liệu CDC từ Kafka và lưu file Parquet thô vào MinIO (Bronze Tier)',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['ingest', 'bronze', 'football']
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_kafka_to_minio',
        python_callable=ingest_from_kafka,
    )
