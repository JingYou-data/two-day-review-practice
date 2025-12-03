import os
import psycopg2
import pandas as pd
import boto3
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def get_postgres_connection():
    """创建 Postgres 连接"""
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    return conn

def get_s3_client():
    """创建 S3 客户端"""
    session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'))
    s3_client = session.client('s3', region_name=os.getenv('AWS_REGION'))
    return s3_client

def export_postgres_to_s3():
    """从 Postgres 读取数据并上传到 S3"""
    
    # 从 Postgres 读取数据
    conn = get_postgres_connection()
    query = "SELECT * FROM products;"
    df = pd.read_sql(query, conn)
    conn.close()
    
    print(f"✅ Read {len(df)} records from Postgres")
    print(f"Columns: {list(df.columns)}")
    
    # 转换为 CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    
    # 上传到 S3
    s3_client = get_s3_client()
    bucket = os.getenv('S3_BUCKET')
    prefix = os.getenv('S3_PREFIX')
    
    # 创建文件路径
    s3_key = f"{prefix}/products_from_postgres.csv"
    
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_data
    )
    
    print(f"✅ Uploaded to S3: s3://{bucket}/{s3_key}")
    
    # 验证上传
    s3_url = f"https://s3.{os.getenv('AWS_REGION')}.amazonaws.com/{bucket}/{s3_key}"
    print(f"📍 S3 Location: {s3_url}")

if __name__ == "__main__":
    print("=" * 50)
    print("Export Postgres data to AWS S3")
    print("=" * 50)
    export_postgres_to_s3()
