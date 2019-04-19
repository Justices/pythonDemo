from db_factory import DBFactory
from math import pow, sqrt

factory = DBFactory()


def cal_rate(expect, expect_unit="month"):
    if expect_unit == "day":
        return pow(1+11.88/365/100, expect)
    return pow(1+11.88/12/100, expect)

dbl_sql = '''select va_id from debt_buy_log where bank_flag=1  and status = 1 and va_id > 0 and  update_time >= date_sub(now(), INTERVAL 1 hour)'''

result_list = factory.query(dbl_sql)
va_list = [va_id_item['va_id'] for va_id_item in result_list]

va_sql = '''select sum(amount) as price from vip_account where id in %s'''
va_amount = factory.query_one(va_sql,  param=(tuple(va_list), ))

fa_sql = '''select sum(balance) as price from finance_account where owner_id in %s and role_id = 13'''
fa_amount = factory.query_one(sql=fa_sql, param=(tuple(va_list), ))

dea_sql = '''SELECT sum(price) as price from debt_exchange_account where va_id in %s and status <> 0'''
dea_amount = factory.query_one(sql=dea_sql, param=(tuple(va_list), ))

rate = (fa_amount['price'] + dea_amount['price'])/va_amount['price']
print "debt transfer rate %s:"%rate

print "kinds of product rate : "

one_month_rate = cal_rate(1)
print "one month  of product rate %s: " %one_month_rate
three_month_rate = cal_rate(3)
print "three month of product rate %s: "%three_month_rate
six_month_rate = cal_rate(6)
print "six month of product rate %s: "%six_month_rate
twl_month_rate = cal_rate(12)
print "twl month of product rate %s: "%twl_month_rate
twf_month_rate = cal_rate(24)
print "twf month of product rate %s: "%twf_month_rate
fif_day_rate = cal_rate(15, expect_unit='day')
print "fif day of product rate %s: "%fif_day_rate
year_day_rate = cal_rate(365, expect_unit='day')
print "year day of product rate  %s: "%year_day_rate

avg_rate = (one_month_rate + three_month_rate + six_month_rate + twl_month_rate + twf_month_rate + fif_day_rate + year_day_rate)/7

variance_rate = pow(one_month_rate-avg_rate, 2) + pow(three_month_rate-avg_rate, 2) + pow(six_month_rate-avg_rate, 2) + pow(twl_month_rate-avg_rate, 2) + pow(twf_month_rate-avg_rate, 2) + pow(fif_day_rate-avg_rate, 2) + pow(year_day_rate-avg_rate, 2)

standard_rate = sqrt(variance_rate/7)

print standard_rate

