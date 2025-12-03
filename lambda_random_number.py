import json
import random

def lambda_handler(event, context):
    """
    Lambda 函数：返回一个随机数
    GET 请求
    """
    
    # 生成随机数
    random_number = random.randint(1, 1000)
    
    # 返回响应
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': 'Random number generated successfully',
            'random_number': random_number
        })
    }

# 本地测试
if __name__ == "__main__":
    test_event = {}
    test_context = {}
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))