import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 测试读取
print("=" * 50)
print("Testing Environment Variables")
print("=" * 50)
print(f"✅ API Base URL: {os.getenv('API_BASE_URL')}")
print(f"✅ S3 Bucket: {os.getenv('S3_BUCKET')}")
print(f"✅ S3 Prefix: {os.getenv('S3_PREFIX')}")
print(f"✅ AWS Profile: {os.getenv('AWS_PROFILE')}")
print(f"✅ Postgres Host: {os.getenv('POSTGRES_HOST')}")
print(f"✅ Minio Endpoint: {os.getenv('MINIO_ENDPOINT')}")
print("=" * 50)
print("Environment variables loaded successfully!")