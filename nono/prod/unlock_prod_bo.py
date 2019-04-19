#!/usr/bin/python
from nono.base import Connection, process_request
import sys



conn = Connection()
path = 'http://prod.prod.com/prod-XXXX/financeAmount/debtSaleRepaymentUnLock'


def fetch_try_limit_tasks():
    task_sql = '''select * from XXXX_debt_sale_task where  try_count > 20 and status in (3,4) and create_time> '2017-10-23' '''
    task_list = conn.query(task_sql)
    return task_list


def filter_ds_id(task_item):
    ds_id = task_item['ds_id']
    ds_sql = '''select ds.status as ds_status, pds.status as pds_status from debt_sale ds INNER join prod_debt_sale pds on ds.id = pds.id where ds.id = %s'''
    ds_status_item = conn.query_one(ds_sql, (ds_id, ))
    ds_status = ds_status_item['ds_status']
    pds_status = ds_status_item['pds_status']
    if ds_status != 3 or pds_status != 5:
        print "the ds %s status is not valid , the ds status is %s and the pds status is %s "%(ds_id, ds_status, pds_status)
        return False
    bo_id = task_item['bo_id']
    ba_sql = "select * from XXXX_borrows_accept_unpaid where bo_id=%s and is_pay = 2"
    ba_list = conn.query(ba_sql, (bo_id,))
    if len(ba_list) > 0:
        print "the bo %s still in paying "%bo_id
        return False
    return True


def process_unlock_request(task_item):
    va_id = task_item['from_id']
    bo_id = task_item['bo_id']
    seri_no = task_item['seri_no']
    operation_sql = '''select * from XXXX_debt_operation_log where va_id = %s and bo_id=%s and seri_no=%s AND trans_type = 7 and status = 10'''
    operation_item_list = conn.query(operation_sql, (va_id, bo_id, seri_no))

    if len(operation_item_list) == 0:
        print "the bo %s is paying , the va_id is %s " % (bo_id, va_id)
        return

    for operation_item in operation_item_list:
        trans_id = operation_item['trans_id']
        repay_sql = '''select * from prod_debt_repayment_lock where bo_id = %s and trans_id = %s and status = 2 '''
        lock_item = conn.query_one(repay_sql, (bo_id, trans_id))
        if len(lock_item) == 0 :
            process_request(path, get_transfer_param(bo_id, trans_id))



def get_transfer_param(bo_id, trans_id):
    param = dict()
    param['boId']= bo_id
    param['transId'] = trans_id
    return param


def process_history_data(bo_id):
    pending_sql = '''SELECT bo_id, trans_id FROM prod_debt_repayment_lock where bo_id = %s group by trans_id , bo_id HAVING count(1) <2 '''
    lock_list = conn.query(pending_sql, (bo_id, ))
    bo_dict = dict()
    if lock_list is None and len(lock_list) == 0:
        print "the bo %s have already unlocked"%(bo_id)
        return
    for item in lock_list:
        trans_id = item['trans_id']
        bo_id = item['bo_id']
        #print "repair the history data bo %s trans_id %s "%(item['bo_id'], item['trans_id'])
        if trans_id not in bo_dict:
            process_request(path, get_transfer_param(bo_id, trans_id))
            bo_dict[trans_id] = bo_id
            print "process_history_data bo %s transid %s"%(bo_id, trans_id)


if __name__ == '__main__':
    argv = sys.argv
    task_item_list = list(fetch_try_limit_tasks())
    for item in filter(filter_ds_id, task_item_list):
        process_unlock_request(item)