import os
import json
import pandas as pd
from minio import Minio
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

def get_minio_client():
    """创建 Minio 客户端"""
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False  # 本地开发用 HTTP
    )
    return client

def upload_to_minio():
    """上传 JSON 数据到 Minio source bucket"""
    client = get_minio_client()
    source_bucket = os.getenv('MINIO_BUCKET_SOURCE')
    
    # 读取本地 JSON 文件
    with open('data/api_data.json', 'r') as f:
        data = f.read()
    
    # 转换为 bytes
    data_bytes = data.encode('utf-8')
    
    # 上传到 Minio
    client.put_object(
        source_bucket,
        'products.json',
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/json'
    )
    
    print(f"✅ Uploaded products.json to {source_bucket}")

def transform_and_move():
    """从 source bucket 读取，修改列名，存到 target bucket"""
    client = get_minio_client()
    source_bucket = os.getenv('MINIO_BUCKET_SOURCE')
    target_bucket = os.getenv('MINIO_BUCKET_TARGET')
    
    # 从 source bucket 读取数据
    response = client.get_object(source_bucket, 'products.json')
    data = json.loads(response.read().decode('utf-8'))
    
    print(f"✅ Read {len(data)} records from {source_bucket}")
    
    # 转换为 DataFrame
    df = pd.DataFrame(data)
    
    # 修改列名：title -> product_name
    df = df.rename(columns={'title': 'product_name'})
    
    print(f"✅ Renamed column: 'title' -> 'product_name'")
    print(f"Columns: {list(df.columns)}")
    
    # 转换回 JSON
    transformed_data = df.to_json(orient='records', indent=2)
    data_bytes = transformed_data.encode('utf-8')
    
    # 上传到 target bucket
    client.put_object(
        target_bucket,
        'products_transformed.json',
        BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/json'
    )
    
    print(f"✅ Uploaded transformed data to {target_bucket}")

if __name__ == "__main__":
    print("=" * 50)
    print("Step 1: Upload to Minio source bucket")
    print("=" * 50)
    upload_to_minio()
    
    print("\n" + "=" * 50)
    print("Step 2: Transform and move to target bucket")
    print("=" * 50)
    transform_and_move()