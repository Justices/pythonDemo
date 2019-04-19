#!/usr/bin/python
from nono.base import Connection, process_request
import sys
import time

db_conn = Connection()
url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer'
fp_dict = dict()



def get_v3_dea_by_page(page_size=500, page_index=0):
    start_num = 0
    end_num = page_size
    if page_index > 0:
        start_num = (page_index-1)*page_size + 1
        end_num = page_size*page_index

    dea_sql = '''SELECT bo.bo_id,dea.id as dea_id, dea.price-dea.lock_price as price,dea.user_id, dea.seri_no
		FROM debt_exchange_account dea
		  INNER JOIN XXXX_borrows_info bo ON dea.bo_id = bo.bo_id
		WHERE dea.va_id = 23665 AND dea.hold_num > 0
		AND dea.locking_num = 0 AND dea.lock_price = 0
		AND dea.bank_flag = 1
    and dea.price > dea.lock_price and dea.price>= 10000
    AND status in (1, 2)   AND NOT exists(
            SELECT 1
            FROM debt_package_borrows dpb
            WHERE dpb.bo_id = dea.bo_id AND dpb.seri_no = dea.seri_no
                  AND is_delete = 0
            )
    order by dea.price  desc limit %s, %s'''
    dea_list = db_conn.query(dea_sql, (start_num, end_num))
    # return filter(is_bo_not_in_transfer,  dea_list)
    return dea_list


def get_transfer_param(target_id, dea_item):
    param = dict()
    param['pid'] = dea_item["bo_id"]
    param['userId'] = dea_item["user_id"]
    param['vaId'] = dea_item['va_id']
    param['targetFpId'] = target_id
    param['targetVaId'] = 0
    param['fromType'] = 1
    param['seriNo'] = dea_item["seri_no"]
    param['price'] = dea_item['price']
    return param


def process_debt_transfer(dea_item_list, fp_item_list):
    for dea_item in dea_item_list:
        dea_index = dea_item_list.index(dea_item)
        while True:
            fp_item = fetch_fp_item(dea_index, fp_item_list)
            fp_id = fp_item['id']
            if fp_id > 0 or len(fp_item_list) == 0:
                break

        if len(fp_item_list) == 0 or fp_id == 0:
            print "all of the plan have finished debt match"
            return False
        fp_dict[fp_id] += dea_item['price']
        param = get_transfer_param(dea_item, fp_id)
        time.sleep(6)
        process_request(path=url, data=param)
    return True


def fetch_fp_item(dea_index, fp_list):
    fp_count = len(fp_list)
    fp_index = dea_index % fp_count
    fp_item = fp_list[fp_index]
    fp_flag = check_plan(fp_item)
    if fp_flag is False:
        print "the fp id %s have already finished the debt match with the price %s and the debt match price is %s "%(fp_item['id'], fp_item['price_finish'], fp_dict[fp_item['id']])
        fp_list.remove(fp_item)
        return {'id': 0}
    return fp_item


def check_plan(fp_item):
    target_id = fp_item['id']
    if target_id not in fp_dict:
        fp_dict[target_id] = 0

    price = fp_dict[target_id]
    if price > fp_item['price_finish']:
        return False

    return True


def is_bo_not_in_transfer(dea):
    bo_id = dea['bo_id']
    seri_no = dea['seri_no']
    task_sql = '''select count(1) as count from XXXX_debt_sale_task where from_id = 23665 and bo_id = %s and seri_no = %s '''
    result = db_conn.query_one(task_sql, (bo_id, seri_no))
    if result['count'] > 0:
        return False
    return True


if __name__ == '__main__':
    arg_arr = sys.argv
    fp_item = dict()
    fp_item['id'] = 8973
    fp_item['price_finish'] = 608516.67
    fp_list = list((fp_item,))
    page_index = 0
    while True:
        dea_list = get_v3_dea_by_page(page_index=page_index)
        flag = process_debt_transfer(dea_list, fp_list)
        if flag is False:
            break
        page_index += 1









