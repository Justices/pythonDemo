#!/usr/bin/python
from nono.base import Connection, process_request
from threadpool import makeRequests
from unlock_prod_bo import get_transfer_param
import time
import sys
import threadpool

conn = Connection()
pool = threadpool.ThreadPool(10)
path = 'http://prod.prod.com/prod-XXXX/financeAmount/debtSaleRepaymentUnLock'


def fetch_unlock_bo():
    start = page_size*index
    end = page_size* (index+1)
    pro_sql = 'SELECT distinct trans_id,bo_id from prod_debt_repayment_lock  GROUP BY bo_id,trans_id  HAVING count(trans_id) <2 order by bo_id asc limit %s, %s'
    return conn.query(pro_sql, (start, end))


def filter_paid_bo(repayment_item):
    bo_id = repayment_item['bo_id']
    trans_id = repayment_item['trans_id']
    re_sql = '''select * from XXXX_receipt_msg where bo_id =%s and trans_id = %s AND STATUS = 85'''
    receipt_item = conn.query_one(re_sql, (bo_id, trans_id))
    if len(receipt_item) == 0:
        print "the bo %s with trans id %s still in paying "%(bo_id, trans_id)
        return False
    return True


def process_unlock_bo(item_list):
    for item in filter(filter_paid_bo, item_list):
        # process_unlock_request(item)
        bo_id = item['bo_id']
        trans_id = item['trans_id']
        #print "unlock the bo %s trans id %s "%(bo_id, trans_id)
        process_request(path, get_transfer_param(bo_id, trans_id))

if __name__ == '__main__':
    argv = sys.argv
    page_size = 1000
    index = 0
    while True:
        task_item_list = list(fetch_unlock_bo())
        if len(task_item_list) == 0:
            break
        index+=1
        prod_task_dict = dict()
        prod_task_dict[1] = task_item_list[::4]
        prod_task_dict[2] = task_item_list[1::4]
        prod_task_dict[3] = task_item_list[2::4]
        prod_task_dict[4] = task_item_list[3::4]
        requests = makeRequests(callable_=process_unlock_bo, args_list=prod_task_dict.values())
        [pool.putRequest(req) for req in requests]
    pool.wait()