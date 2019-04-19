#encoding: utf-8
import requests

apiUrl = 'http://www.tuling123.com/openapi/api'
data = {
    'key'    : '8edce3ce905a4c1dbb965e6b35c3834d', # 如果这个Tuling Key不能用，那就换一个
    'info'   : '你好', # 这是我们发出去的消息
    'userid' : 'wechat-robot', # 这里你想改什么都可以
}

r = requests.post(apiUrl, data=data).json()

print r.get('text')
