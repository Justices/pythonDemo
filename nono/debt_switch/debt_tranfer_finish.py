#!/usr/bin/python
from nono.base import Connection, process_request
import sys
import time
import json

conn = Connection()
path = "http://192.168.3.196:8084/trd-XXXX/testCase/debtNotifyResend"

def fetch_error_msg(end_time):
    proof_sql = "select * from XXXX_proof where biz_type = 7 and status = 0 and create_time >=%s"
    proof_list = conn.query(proof_sql, (end_time, ))
    return proof_list


def process_resend_message(proof_item):
    error_msg = json.loads(proof_item['err_msg'])
    param = dict()
    param['transId'] = error_msg['transId']
    time.sleep(3)
    process_request(path, param)

if __name__ == '__main__':
    # arg_arr = sys.argv
    # end_time = arg_arr[1]
    proof_item_list = fetch_error_msg(end_time='2018-01-24')

    for item in proof_item_list:
        process_resend_message(item)