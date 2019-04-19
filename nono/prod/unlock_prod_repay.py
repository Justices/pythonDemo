#!/usr/bin/python
from nono.base import Connection, process_request

conn = Connection()
path = 'http://prod.prod.com/prod-XXXX/financeAmount/debtSaleRepaymentUnLock'


def fetch_lock_list():
    lock_sql = '''select bo_id, trans_id from prod_debt_repayment_lock where date_format(create_time, '%Y-%m-%d') >= '2017-08-01' and date_format(create_time, '%Y-%m-%d') <= '2017-10-22' group by bo_id, trans_id having count(*) = 1'''
    lock_list = conn.query(lock_sql)
    return lock_list


def filter_bo_trans_id(item):
    bo_id = item['bo_id']
    trans_id = item['trans_id']
    unlock_sql = '''select * from prod_debt_repayment_lock where status = 2 and bo_id = %s and trans_id = %s'''
    unlock_list = conn.query(unlock_sql, (bo_id, trans_id))
    if len(unlock_list) > 0:
        print "The bo %s have already unlocked, the trans id is %s" % (bo_id, trans_id)
        return False
    receipt_sql = '''select * from XXXX_receipt_msg where status= 85 and bo_id = %s and trans_id = %s'''
    receipt_list = conn.query(receipt_sql, (bo_id, trans_id))
    if receipt_list is None or len(receipt_list) == 0:
        print "The bo %s is paying, the trans id is %s" % (bo_id, trans_id)
        return False
    log_sql = '''select * from XXXX_debt_operation_log where status <> 10 and trans_type = 7 and bo_id = %s and trans_id = %s'''
    log_list = conn.query(log_sql, (bo_id, trans_id))
    if len(log_list) > 0:
        print "The bo %s is paying, the trans id is %s" % (bo_id, trans_id)
        return False
    return True


def process_unlock_request(item):
    bo_id = item['bo_id']
    trans_id = item['trans_id']
    process_request(path, get_transfer_param(bo_id, trans_id))
    print "process_history_data bo %s transid %s" % (bo_id, trans_id)


def get_transfer_param(bo_id, trans_id):
    param = dict()
    param['boId']= bo_id
    param['transId'] = trans_id
    return param


if __name__ == '__main__':
    lock_item_list = list(fetch_lock_list())
    for item in filter(filter_bo_trans_id, lock_item_list):
        process_unlock_request(item)