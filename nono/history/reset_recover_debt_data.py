#encoding:utf-8
import json
from collections import defaultdict

import MySQLdb
import time

import requests
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB


XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.1.90', db='XXXX', user='yanfa', passwd='yanfa#123', port=3306,charset='utf8', blocking=True)
loan_pool = PooledDB(MySQLdb, 4, host='192.168.1.90', db='XXXX', user='yanfa', passwd='yanfa#123', port=3306,charset='utf8', blocking=True)


dea_result_dic = defaultdict()
ba_result_dic = defaultdict()

br_filter_sql = '''SELECT  1 from borrows_repayment where bo_id = %s and br_time = %s and br_is_repay <> 1;'''
receipt_filter_sql = '''SELECT  1 from XXXX_receipt_msg where bo_id = %s and plan_time= %s and status < 80;'''

back_up_path = '''http://172.16.3.100:8080/XXXX-XXXX/historyDebt/backupDebt'''
fix_up_path = '''http://172.16.3.100:8080/XXXX-XXXX/historyDebt/fixUpDebt'''
recover_sql = '''select id from XXXX_ba_recover_tmp'''


def query(sql, pool=XXXX_pool, param=None):
    conn = pool.connection()
    dbc = conn.cursor(cursorclass=DictCursor)
    dbc.execute(sql, param)
    result = dbc.fetchall()
    dbc.close()
    conn.close()
    return result


def back_debt_data():
    print 'start to back up dea data'
    tmp_result = query(sql=recover_sql)
    for item in tmp_result:
        process_request(fix_up_path, data=item)
    print 'ba data back finished !!!'


def process_request(path, data):
    item = json.dumps(data)
    print item
    headers = {'Content-type':'application/json'}
    rep = requests.request("post", url=path, headers=headers, data=item, json=item)
    print rep


def filter_data(item):
    bo_id = item['bo_id']
    plan_time = item['plan_time']
    br_result = query(br_filter_sql, loan_pool, (bo_id, plan_time))
    if len(br_result) > 0:
        return False
    receipt_result = query(receipt_filter_sql, XXXX_pool,  (bo_id, plan_time))
    if receipt_result is None or len(receipt_result) >0:
        print "receipt filter for %s"%(bo_id)
        return False
    return True


if __name__ == '__main__':
    print "start to modify the data"
    back_debt_data()
    time.sleep(1)
    print "modify data success"




