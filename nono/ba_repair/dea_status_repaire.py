from nono.db_factory import DBFactory
from nono.base import process_request

conn = DBFactory()

url = '''http://XXXX.sit.com/XXXX-XXXX/ass/handleDebtOperationLog'''

def fetch_dea_list():
    dea_sql = '''SELECT va_id, bo_id, seri_no, id FROM debt_exchange_account dea where va_id  not in (0,23665) and status = 3 and 
                  not exists (SELECT 1 from XXXX_debt_operation_log ol where dea.id = ol.dea_id  and ol.status  in (1,0)); '''
    dea_result = conn.query_with_tuple(dea_sql)
    if len(dea_result) == 0:
        return None
    ex_dea_list = filter(is_bo_not_in_pay, dea_result)
    return ex_dea_list


def is_bo_not_in_pay(item):
    ba_sql = '''select 1 from XXXX_borrows_accept_unpaid where va_id = %s and bo_id=%s and  seri_no=%s and is_pay = 2;'''
    va_id = item[0]
    bo_id = item[1]
    seri_no = item[2]
    ba_result = conn.query(ba_sql, (va_id, bo_id, seri_no))
    if len(ba_result) == 0:
        return True
    return False


def process_dea_repair():
    dea_list = fetch_dea_list()
    if dea_list is None:
        return
    param_list = map(map_param, dea_list)
    for param in param_list:
        process_request(url, param)


def map_param(dea_item):
    dea_id = dea_item[3]
    debt_log = '''select trans_id as transId from XXXX_debt_operation_log where dea_id = %s and status = 10 order by id desc limit 1 '''
    result_item = conn.query_one(debt_log, (dea_id, ))
    result_item['deaId'] = dea_id
    result_item['type'] = 2
    return result_item

if __name__ == '__main__':
    process_dea_repair()