# encoding:utf-8
# /usr/bin/python
from nono.base import get_transfer_deb_param, process_request,Connection
import sys
from decimal import Decimal
import  csv

factory = Connection()

path = "http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer"
price_dict = dict()
price_dict['expect'] = 0

def fetch_debt_list(va_id, start, end):
    dea_sql = '''SELECT bo.bo_id,dea.id as dea_id, dea.price-dea.lock_price as price,dea.user_id, dea.seri_no,dea.va_id
		FROM debt_exchange_account dea
		  INNER JOIN XXXX_borrows_info bo ON dea.bo_id = bo.bo_id
		  LEFT join XXXX_overdue_bo_tmp bt ON dea.bo_id = bt.bo_id and bo.bo_id = bt.bo_id
		WHERE dea.va_id = %s AND dea.hold_num > 0
		AND dea.locking_num = 0 AND dea.lock_price = 0
		AND dea.bank_flag = 1 and bt.id is null
    and dea.price > dea.lock_price and dea.price>= 10000
    AND status in (1, 2)   AND NOT exists(
            SELECT 1
            FROM debt_package_borrows dpb
            WHERE dpb.bo_id = dea.bo_id AND dpb.seri_no = dea.seri_no
                  AND is_delete = 0
            )
    order by dea.price  desc limit %s, %s'''

    result_list = factory.query(dea_sql, (va_id, start, end))
    return result_list


def transfer_debt(param_list):
    for dea_item in param_list:
        if price_dict['expect'] > expect_money:
            return
        price_dict['expect'] += dea_item['price']
        param = get_transfer_deb_param(0, dea_item, target_type=0)
        print param
        process_request(path, param)
        # time.sleep(4)
        

if __name__ == '__main__':
    arg_arr = sys.argv
    from_id = arg_arr[1]

    expect_money = Decimal(arg_arr[2])
    start_index = 0
    page_size = 500
    flag = True
    while flag:
        dea_list = fetch_debt_list(from_id, start_index*page_size, (start_index+1)*page_size)
        if len(dea_list) == 0:
            flag = False
            print "the vaId %s have already transfer the debt"
        transfer_debt(dea_list)
        start_index += 1


