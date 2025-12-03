import json

def lambda_handler(event, context):
    """
    Lambda 函数：返回 POST 请求的 body
    POST 请求
    """
    
    # 从 event 中获取 body
    body = event.get('body', '{}')
    
    # 如果 body 是字符串，解析为 JSON
    if isinstance(body, str):
        try:
            body_data = json.loads(body)
        except:
            body_data = {'raw_body': body}
    else:
        body_data = body
    
    # 返回响应
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'message': 'Received your data successfully',
            'your_data': body_data
        })
    }

# 本地测试
if __name__ == "__main__":
    test_event = {
        'body': json.dumps({
            'name': 'Jing',
            'course': 'Data Engineering',
            'project': 'Two Day Review'
        })
    }
    test_context = {}
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))
