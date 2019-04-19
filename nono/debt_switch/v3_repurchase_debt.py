from nono.db_factory import DBFactory
from nono.base import process_request, get_transfer_deb_param
import time

url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer'

conn = DBFactory()

expect_price = 20000000


def fetch_bo_list(va_id, start, end):
    dea_sql = '''SELECT * from debt_exchange_account dea where va_id = %s
           and status in (1) and bank_flag = 1 and price >0 and locking_num = 0  and bo_id not in (2143881,2156960,2149682,2149730,2176851,2176903,2131167,2141030,2149521) order by price desc limit %s, %s '''
    return conn.query(dea_sql,(va_id, start, end))


def process_transfer(dea_item_list):
    transfer_price = 0
    for dea_item in dea_item_list:
        transfer_price += dea_item['price']
        param = get_transfer_deb_param(23665, dea_item, target_type=3)
        #print param
        process_request(url, param)
        if transfer_price > expect_price:
            print "repurchase debt completed, the compelted price is %s"%(transfer_price)
            return
        # time.sleep(1)
    print "THE CURRENT PRICE IS %s"%(transfer_price)


if __name__ == '__main__':

    start_index = 0
    page_size = 500
    while True:
        dea_list = fetch_bo_list(975180, start_index*page_size, (start_index+1)*page_size)
        if len(dea_list) == 0 :
            print "dea is complete"
            break
        process_transfer(dea_list)
        start_index +=1
