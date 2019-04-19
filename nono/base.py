#!/usr/bin/python
# from MySQLdb.cursors import DictCursor
import pymysql
from urllib2 import Request
import json
import MySQLdb
import urllib2
import urllib
import requests
from pymysql.cursors import DictCursor


class Connection(object):

    def query(self, sql, param=None):
        # prod
        # conn = MySQLdb.connect(host='172.16.0.101', port=3306, user='dev', passwd='beX5kFn4', db='XXXX')
        #sit
        # conn = MySQLdb.connect(host='192.1d68.4.13', port=3306, user='yanfa', passwd='yanfa#123', db='XXXX')
        conn = pymysql.connect(host='192.168.3.253', port=3306, user='u_XXXX', passwd='XXXX@2018', db='XXXX')
        cursor = conn.cursor(cursor=DictCursor)
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    def query_one(self, sql, param=None):
        # prod
        conn =  MySQLdb.connect(host='192.168.3.155', port=3306, user='yanfa', passwd='yanfa#123', db='XXXX')
        #sit
        # conn = MySQLdb.connect(host='192.168.4.13', port=3306, user='yanfa', passwd='yanfa#123', db='XXXX')
        cursor = conn.cursor(cursor=DictCursor)
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        if len(result) == 0:
            return dict()

        return result[0]

    def query_tuple(self, sql, param=None):
        # prod
        # conn = MySQLdb.connect(host='172.16.0.101', port=3306, user='dev', passwd='beX5kFn4', db='XXXX')
        #sit
        # conn = MySQLdb.connect(host='192.1d68.4.13', port=3306, user='yanfa', passwd='yanfa#123', db='XXXX')
        conn = MySQLdb.connect(host='192.168.3.155', port=3306, user='yanfa', passwd='yanfa#123', db='XXXX')
        cursor = conn.cursor()
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result


def process_request(path, data):
    item = json.dumps(data)
    print item
    headers = {'Content-type':'application/json'}
    resp = requests.request("post", url=path, headers=headers, data=item, json=item)
    # print ''' curl -l -H "Content-type: application/json" -X POST -d ' ''' + str(item)  + ''' ' ''' + path


def process_get_request(path, data):
    param_path = urllib.urlencode(data)
    request = Request(url='%s%s%s'%(path, '?', param_path))
    resp = urllib2.urlopen(request)
    print resp.read()


def get_transfer_deb_param(target_id, dea_item, target_type=2):
    param = dict()
    param['pid'] = dea_item["bo_id"]
    param['userId'] = dea_item["user_id"]
    param['vaId'] = dea_item['va_id']
    if target_type == 3:
        param['targetFpId'] = 0
        param['targetVaId'] = target_id
    else:
        param['targetFpId'] = target_id
        param['targetVaId'] = 0
    param['fromType'] = 2
    param['seriNo'] = dea_item["seri_no"]
    return param
