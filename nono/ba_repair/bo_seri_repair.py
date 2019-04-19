from nono.db_factory import DBFactory
import time

dea_sql = '''update debt_exchange_account set seri_no = %s where va_id = %s and bo_id = %s'''
task_sql = '''update XXXX_debt_sale_task set seri_no = %s where from_id = %s and bo_id = %s and seri_no = '' '''
ba_sql = '''update XXXX_borrows_accept_unpaid set seri_no = %s where va_id = %s and bo_id = %s'''

bo_sql = '''select * from XXXX_single_debt_bo'''
dea_va_sql = '''select DISTINCT  bo_id, va_id from XXXX_borrows_accept_unpaid WHERE bo_id = %s and plan_time = %s and price_principal >0  '''


def gen_seri_no(bo_id,  va_id):

    return '0000' + time.strftime("%y%d%m") + str(bo_id)+str(va_id)

if __name__ == '__main__':
    db = DBFactory()
    single_list = db.query(bo_sql, ())
    for bo_item in single_list:
        dea_list = db.query(dea_va_sql, (bo_item['bo_id'], bo_item['plan_time']))
        for dea_item in dea_list:
            bo_id = dea_item['bo_id']
            va_id = dea_item['va_id']
            seri_no = gen_seri_no(dea_item['bo_id'], dea_item['va_id'])
            print(dea_sql%(seri_no, va_id, bo_id)) + ";"
            # print(task_sql%(seri_no, va_id, bo_id))+ ";"
            print(ba_sql%(seri_no, va_id, bo_id))+ ";"