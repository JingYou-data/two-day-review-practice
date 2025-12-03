import os
import boto3
from minio import Minio
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

def get_s3_client():
    """创建 S3 客户端"""
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
    s3_client = session.client('s3', region_name=os.getenv('AWS_REGION'))
    return s3_client

def get_minio_client():
    """创建 Minio 客户端"""
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    return client

def copy_s3_to_minio():
    """从 S3 下载数据并上传到 Minio"""
    
    # 从 S3 下载
    s3_client = get_s3_client()
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')
    s3_key = f"{prefix}/products_from_postgres.csv"
    
    print(f"📥 Downloading from S3: s3://{bucket}/{s3_key}")
    
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    csv_data = response['Body'].read()
    
    print(f"✅ Downloaded {len(csv_data)} bytes from S3")
    
    # 上传到 Minio
    minio_client = get_minio_client()
    target_bucket = os.getenv('MINIO_BUCKET_TARGET')
    
    minio_client.put_object(
        target_bucket,
        'products_from_s3.csv',
        BytesIO(csv_data),
        length=len(csv_data),
        content_type='text/csv'
    )
    
    print(f"✅ Uploaded to Minio: {target_bucket}/products_from_s3.csv")
    print(f"🌐 View in Minio Console: http://localhost:9001")

if __name__ == "__main__":
    print("=" * 50)
    print("Copy data from AWS S3 to Minio")
    print("=" * 50)
    copy_s3_to_minio()