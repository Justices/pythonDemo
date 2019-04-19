from collections import defaultdict
from nono.db_factory import DBFactory
from math import pow
from decimal import Decimal, ROUND_HALF_UP, getcontext
from dateutil import relativedelta

conn = DBFactory()
ba_va_dict = dict()


def calculate_month_rate(money, months, rate, start_month='2017-12-01'):
    month_rate = rate/100/12
    month_price = money*month_rate*pow(1+month_rate, months)/(pow(1+month_rate, months)-1)
    result_list = dict()
    for month in xrange(1, months+1):
        ba_dict = defaultdict()
        month_interest = money*month_rate*(pow(1+month_rate, months) - pow(1+month_rate, month-1))/(pow(1+month_rate, months)-1)
        month_principle = month_price - month_interest
        ba_dict['price_principle'] = format_decimal(month_principle)
        ba_dict['price_interest'] = format_decimal(month_interest)
        ba_dict['price'] = format_decimal(month_price)
        plan_time = format_date_time(start_date=start_month, intervals=month-1)
        result_list[plan_time] = ba_dict
    return result_list


def format_decimal(value):
    decimal_value = Decimal(value, getcontext())
    return decimal_value.quantize(Decimal('0.00'), rounding=ROUND_HALF_UP)


def format_date_time(start_date, intervals):
    month_intervals = relativedelta.relativedelta(months=intervals)
    return (start_date + month_intervals).strftime('%Y-%m-%d')


def fetch_repair_ba_info(va_id_list, bo_id):
    br_sql = '''SELECT count(1) as months , br.br_time, bp.bp_rate_lender as year_rate from borrows_repayment br
                INNER JOIN borrows bo
                  on br.bo_id = bo.id
                INNER JOIN borrows_prepare bp
                  on bo.bp_id = bp.id
                where bo_id = %s and br_is_repay = 0'''
    br_item = conn.query_one(sql=br_sql, param=(bo_id, ))
    start_month = br_item['br_time']
    months = br_item['months']
    year_rate = str(br_item['year_rate'])
    ba_price_sql = '''SELECT id, sum(price_principal) as total_price from XXXX_borrows_accept_unpaid where bo_id = %s and va_id = %s and is_pay = 0 '''
    ba_item_sql = '''SELECT id, plan_time from XXXX_borrows_accept_unpaid where bo_id = %s and va_id = %s and is_pay = 0'''

    for va_id in va_id_list:
        ba_result = conn.query_one(sql=ba_price_sql, param=(bo_id, va_id))
        new_ba_price = calculate_month_rate(money=float(ba_result['total_price']), months=months, rate=float(year_rate), start_month=start_month)
        ba_item_result = conn.query(sql=ba_item_sql, param=(bo_id, va_id))
        print " the price for va_id %s is %s"%(va_id, ba_result['total_price'])
        for ba_item in ba_item_result:
            ba_id = ba_item['id']
            plan_time = ba_item['plan_time'].strftime('%Y-%m-%d')
            ba_new_item = new_ba_price[plan_time]
            print '''update XXXX_borrows_accept_unpaid set price_principal = %s, price_interest = %s, price=price_interest + price_principal where id = %s  and va_id = %s;'''%(ba_new_item['price_principle'], ba_new_item['price_interest'], ba_id,va_id)

if __name__ == '__main__':
    # 传入的参数为 va id list 和 需要标的id
    fetch_repair_ba_info((628200, 834107, 953996, 953979, 953924, 953918, 1009429), 2647433)