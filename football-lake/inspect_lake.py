import os, boto3
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3',
    endpoint_url=os.getenv('MINIO_ENDPOINT', 'http://localhost:9000'),
    aws_access_key_id=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
    aws_secret_access_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin123')
)
bucket = os.getenv('MINIO_BUCKET', 'football-lake')

def list_all():
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    
    tree = {}
    total_size = 0
    total_files = 0
    
    try:
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    total_size += size
                    total_files += 1
                    
                    parts = key.split('/')
                    if len(parts) >= 2:
                        layer = parts[0]
                        domain = parts[1]
                        folder = f'{layer}/{domain}'
                        if folder not in tree:
                            tree[folder] = {'count': 0, 'size': 0}
                        tree[folder]['count'] += 1
                        tree[folder]['size'] += size
                        
        print(f'=== TỔNG QUAN DATA LAKE: {total_files} files, {total_size / 1024 / 1024:.2f} MB ===\n')
        for k, v in sorted(tree.items()):
            mb = v['size'] / 1024 / 1024
            print(f"- {k}/ : {v['count']} files ({mb:.2f} MB)")
    except Exception as e:
        print('Lỗi:', e)

if __name__ == '__main__':
    list_all()
