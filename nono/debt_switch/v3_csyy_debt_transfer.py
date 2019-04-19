#!/usr/bin/python
from nono.base import Connection, process_request
import sys
import time
from multiprocessing import Pool, Manager
from functools import partial

db_conn = Connection()
url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer'
fp_dict = Manager().dict()
price_dict = Manager().dict()
price_dict['expect'] = 0

def get_fp_by_date():
    fp_sql = '''SELECT sum(fa.balance-fa.locking) as price_finish, va.fp_id as id 
from finance_account fa
LEFT JOIN vip_account va
  on fa.owner_id = va.id
LEFT JOIN vip_autobidder vab
  on fa.owner_id = va.id and va.id = vab.va_id
where va.fp_id in (
SELECT id
FROM finance_plan
WHERE start_date >= '2017-10-21' AND start_date <= '2017-10-24' AND finance_plan.platform_type = 2 AND
      finance_plan.scope = 11) and vab.enable = 1 and fa.role_id = 13
group by va.fp_id'''
    fp_list = db_conn.query(fp_sql)
    return fp_list


def get_v3_dea_by_page(page_size=500, page_index=0):
    start_num = 0
    end_num = page_size
    if page_index > 0:
        start_num = (page_index-1)*page_size + 1
        end_num = page_size*page_index

    dea_sql = '''SELECT bo.bo_id,dea.id as dea_id, dea.price-dea.lock_price as price,dea.user_id, dea.seri_no,dea.va_id
		FROM debt_exchange_account dea
		  INNER JOIN XXXX_borrows_info bo ON dea.bo_id = bo.bo_id
		WHERE dea.va_id = 23665 AND dea.hold_num > 0
		AND dea.locking_num = 0 AND dea.lock_price = 0
		AND dea.bank_flag = 1
    and dea.price > dea.lock_price and dea.price>= 10000 and bo.p_id in (87,77,83,88,102)
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


def get_transfer_param(dea_item, target_id):
    param = dict()
    param['pid'] = dea_item["bo_id"]
    param['userId'] = dea_item["user_id"]
    param['vaId'] = dea_item['va_id']
    param['targetFpId'] = target_id
    param['targetVaId'] = 0
    param['fromType'] = 1
    param['seriNo'] = dea_item["seri_no"]
    return param


def process_debt_transfer(fp_item_list, dea_item_list):
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
        if price_dict['expect'] > 20000000:
            print "have transfer 2000000"
            return False

        fp_dict[fp_id] += dea_item['price']
        param = get_transfer_param(dea_item, fp_id)
        print param
        price_dict['expect'] += dea_item['price']
        time.sleep(1)
        # process_request(path=url, data=param)
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

process_pool = Pool(processes=4)

if __name__ == '__main__':
    arg_arr = sys.argv
    fp_list = list(get_fp_by_date())
    page_index = 0
    result_list = list()
    while True:
        dea_list = get_v3_dea_by_page(page_index=page_index)
        # flag = process_debt_transfer(dea_list, fp_list)
        if len(dea_list) == 0:
            break
        page_index += 1
        result_list.append(dea_list)

    method_partial = partial(process_debt_transfer, fp_list)
    process_pool.map_async(method_partial, result_list)
    process_pool.close()
    process_pool.join()









