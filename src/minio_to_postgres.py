import os
import json
import psycopg2
from minio import Minio
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def get_minio_client():
    """创建 Minio 客户端"""
    client = Minio(
        os.getenv('MINIO_ENDPOINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False
    )
    return client

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

def create_products_table():
    """在 Postgres 创建 products 表"""
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # 创建表
    create_table_query = """
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        product_name TEXT,
        price DECIMAL(10, 2),
        description TEXT,
        category TEXT,
        image TEXT,
        rating_rate DECIMAL(3, 2),
        rating_count INTEGER
    );
    """
    
    cursor.execute(create_table_query)
    conn.commit()
    
    print("✅ Table 'products' created successfully")
    
    cursor.close()
    conn.close()

def load_from_minio_to_postgres():
    """从 Minio 读取数据并加载到 Postgres"""
    
    # 从 Minio 读取数据
    minio_client = get_minio_client()
    target_bucket = os.getenv('MINIO_BUCKET_TARGET')
    
    response = minio_client.get_object(target_bucket, 'products_transformed.json')
    data = json.loads(response.read().decode('utf-8'))
    
    print(f"✅ Read {len(data)} records from Minio")
    
    # 转换为 DataFrame
    df = pd.DataFrame(data)
    
    # 展开 rating 字段
    df['rating_rate'] = df['rating'].apply(lambda x: x['rate'])
    df['rating_count'] = df['rating'].apply(lambda x: x['count'])
    df = df.drop('rating', axis=1)
    
    print(f"Columns: {list(df.columns)}")
    
    # 连接 Postgres
    conn = get_postgres_connection()
    cursor = conn.cursor()
    
    # 插入数据
    insert_query = """
    INSERT INTO products (id, product_name, price, description, category, image, rating_rate, rating_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """
    
    inserted_count = 0
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['id'],
            row['product_name'],
            row['price'],
            row['description'],
            row['category'],
            row['image'],
            row['rating_rate'],
            row['rating_count']
        ))
        inserted_count += 1
    
    conn.commit()
    
    print(f"✅ Inserted {inserted_count} records into Postgres")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    print("=" * 50)
    print("Step 1: Create products table in Postgres")
    print("=" * 50)
    create_products_table()
    
    print("\n" + "=" * 50)
    print("Step 2: Load data from Minio to Postgres")
    print("=" * 50)
    load_from_minio_to_postgres()