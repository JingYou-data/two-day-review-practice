import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

def fetch_data_from_api():
    """从免费 API 获取数据并保存到本地文件"""
    
    # 使用练习用的免费 API
    api_url = os.getenv('PRACTICE_API_URL')
    
    print(f"Fetching data from: {api_url}")
    
    try:
        # 发送 GET 请求（不需要认证）
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # 获取数据
        data = response.json()
        
        print(f"✅ Successfully fetched {len(data)} records")
        
        # 打印第一条数据看看
        print("\nFirst record:")
        print(json.dumps(data[0], indent=2))
        
        # 保存到本地文件
        output_file = 'data/api_data.json'
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"\n✅ Data saved to {output_file}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        return None

if __name__ == "__main__":
    fetch_data_from_api()