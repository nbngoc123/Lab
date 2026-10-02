import duckdb
import boto3
import os

def create_views():
    print("Connecting to MinIO to discover parquet files...")
    
    endpoint = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
    access_key = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
    secret_key = os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
    bucket = 'football-lake'
    
    # Superset DB
    db_path = '/opt/project/dbt/superset_bi.duckdb'
    if not os.path.exists('/opt/project/dbt'):
        db_path = 'dbt/superset_bi.duckdb'
        endpoint = 'http://localhost:9000'
        
    s3 = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_key)
                      
    try:
        objs = s3.list_objects_v2(Bucket=bucket, Prefix='dwh/')['Contents']
    except KeyError:
        print("No files found in dwh/ prefix on MinIO. You need to run DBT first!")
        return
        
    # Extract unique table names and their schema
    # Pattern: dwh/schema/table_name.parquet
    tables = {}
    for obj in objs:
        key = obj['Key']
        print(f"Found key: {key}")
        if key.endswith('.parquet') and not key.endswith('/.parquet'):
            parts = key.split('/')
            if len(parts) >= 2:
                table_name = parts[-1].replace('.parquet', '')
                if table_name:
                    if len(parts) >= 3:
                        schema_name = parts[-2]
                    else:
                        if table_name.startswith('stg_'):
                            schema_name = 'main_staging'
                        elif table_name.startswith('int_') or table_name.startswith('dim_') or table_name.startswith('fact_') or table_name.startswith('audit_'):
                            schema_name = 'main_core'
                        else:
                            schema_name = 'main_mart'
                    tables[(schema_name, table_name)] = f"s3://{bucket}/{key}"
                
    if not tables:
        print("No parquet files found in dwh/")
        return
        
    print(f"Connecting to DuckDB BI instance: {db_path}...")
    db_path_tmp = db_path + '.tmp'
    if os.path.exists(db_path_tmp):
        os.remove(db_path_tmp)
    con = duckdb.connect(db_path_tmp)
    
    # Configure S3 for DuckDB
    con.execute("INSTALL httpfs; LOAD httpfs;")
    s3_endpoint = endpoint.replace('http://', '').replace('https://', '')
    con.execute(f"SET s3_endpoint='{s3_endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    
    for (schema, table), path in tables.items():
        print(f"Creating view for {schema}.{table} pointing to {path}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        con.execute(f"CREATE OR REPLACE VIEW {schema}.{table} AS SELECT * FROM read_parquet('{path}');")
        
    print("All views created successfully in superset_bi.duckdb.tmp!")
    con.close()
    
    os.replace(db_path_tmp, db_path)
    print("Replaced superset_bi.duckdb atomically!")

if __name__ == '__main__':
    create_views()
