import MySQLdb
from urllib2 import Request
import urllib2
import json



def getPendingDebtSaleTask():
    conn = MySQLdb.connect(host = '172.16.0.101',port = 3306,user='dev',passwd='beX5kFn4', db='XXXX')
    cur = conn.cursor()
    sql = 'select id,ds_id from XXXX_debt_sale_task WHERE  status =2  and date_sub(now(),INTERVAL 12 HOUR) > update_time'
    cur.execute(sql)
    taskList=list()
    for item in cur.fetchall():
        itemStr = consistMessageData(item[1],item[0])
        print item
        taskList.append(itemStr)
    cur.close()
    conn.close()
    return taskList

def consistMessageData(id,taskId):
    param = dict()
    param['topic'] = 'XXXX'
    param['tag'] = 'tag1'
    message = dict()
    message['id'] = str(id)
    message['ID'] = str(id)
    message['type'] = '9'
    message['taskId'] = str(taskId)
    param['message'] = message
    return json.dumps(param)

def reSendMsgToProduct():
    conn = MySQLdb.connect(host = '192.168.3.155:3306',port = 3306,user='yanfa',passwd='yanfa#123', db='XXXX')
    cur = conn.cursor()
    sql = 'select id,ds_id from XXXX_debt_sale_task WHERE  status =2  and date_sub(now(),INTERVAL 12 HOUR) > update_time'
    cur.execute(sql)
    taskList=list()
    for item in cur.fetchall():
        itemStr = consistMessageData(item[1],item[0])
        print item
        taskList.append(itemStr)
    cur.close()
    conn.close()
    return taskList

def consistMessageData(id,taskId):
    param = dict()
    param['topic'] = 'XXXX'
    param['tag'] = ''
    message = dict()
    message['id'] = str(id)
    message['ID'] = str(id)
    message['type'] = '9'
    message['taskId'] = str(taskId)
    param['message'] = message
    return json.dumps(param)

def reSendMsgToProduct():
    url = 'http://XXXX.stb.com/XXXX-XXXX/msg/sendMsg'
    req = Request(url=url)
    req.add_header('Content-type', 'application/json')
    taskList = getPendingDebtSaleTask()
    for item in taskList:
        urllib2.urlopen(req,item)

if __name__ == '__main__':
    reSendMsgToProduct()