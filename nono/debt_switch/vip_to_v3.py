from nono.base import  process_request, get_transfer_deb_param
from nono.db_factory import DBFactory
import sys

db = DBFactory()
url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer'


def process_debt_change(va_id, target_id):
    sql = '''select va_id, bo_id, seri_no, user_id 
            from debt_exchange_account 
            where va_id = %s and price > 0 and status in (1,2) and bank_flag = 1 '''
    dea_list = db.query(sql=sql, param=(va_id, ))
    if len(dea_list) == 0:
        return None
    for dea_item in dea_list:
        data = get_transfer_deb_param(dea_item=dea_item, target_id=target_id, target_type=3)
        process_request(path=url, data=data)


if __name__ == '__main__':
    argv = sys.argv
    source_id = argv[1]
    target_id = argv[2]
    process_debt_change(source_id, target_id)