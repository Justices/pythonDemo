from nono.db_factory import DBFactory
from nono.base import process_request

conn = DBFactory()
url = "http://localhost:8000/XXXX-XXXX/ass/initNonBankDeaPirce"


def fetch_dea():
    # task_sql = '''select * from XXXX_debt_sale_task where task_type = 6 and status < 4'''
    dea_sql = '''select id from debt_exchange_account where va_id=%s and bo_id=%s and seri_no = %s '''

    task_list = conn.query(dea_va_sql, (80691, ))
    for task_item in task_list:
        bo_id = task_item['bo_id']
        va_id = task_item['from_id']
        seri_no = task_item['seri_no']
        dea_item = conn.query(dea_sql, (va_id, bo_id, seri_no))
        if len(dea_item) == 0 :
            continue
        process_request(url, {"ids":str(dea_item[0]['id'])})

def repaire_dea():
    dea_va_sql = '''select * from debt_exchange_account where va_id=%s AND hold_num>0 and price =0 '''
    task_list = conn.query(dea_va_sql, (80691, ))
    for id_item in task_list:
        process_request(url, {"ids": str(id_item['id'])})

if __name__ == '__main__':
    repaire_dea()