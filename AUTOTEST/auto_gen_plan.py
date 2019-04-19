#!/usr/bin/python
#encoding=utf-8
import random
import sys
from datetime import datetime, timedelta

from nono.db_factory import DBFactory

conn = DBFactory()
cur_date = datetime.strftime(datetime.now(), '%Y-%m-%d')

def add_save_plan(plan):
    plan_sql = '''insert into finance_plan(title, scope, price_increment, price_min, price_max, rate_min, rate_max, stop_bidding_days, auto_bid, earnings_intro, buy_limit, redeem_normal_way, redeem_other_way, publish_date, finish_date, expect, expect_unit, start_date, STATUS, show_scope, suffix, rate_show, recommend_scope,platform_type,rate_desc,protect_plan,user_level) 
    values(%(title)s,%(scope)s, 100, 100, %(price_max)s, '12','12',5,0,'xxxx','xxxx','xxxx','xxxxx',%(publish_date)s, %(finish_date)s, %(expect)s,%(expect_unit)s,%(start_date)s,4,1,'xxxx',12 ,3,1, 'xxxxxxxx','质量保障服务计划','0,1,2,3')'''
    return conn.save_data(plan_sql, plan)


def insert_stage_info(fp_id):
    stage_sql = '''insert into XXXX_finance_plan_stage_info(fp_id, publish_date, publish_time, debt_expect_price)  values (%s, %s, '00:00', %s)'''
    conn.save_data(stage_sql, (fp_id, cur_date, 100000))

def insert_debt_config(fp_id):
    config_sql = '''insert into XXXX_finance_plan_debt_config(fp_id, new_tender, choice_plan, v3_account, asset_side, remaining_maturity, new_tender_per, choice_plan_per, v3_account_per) 
values(%s, '7,6,5,4,3,2,1', '128,256,2,3,4,5', 1, '77,83,87,88,94,97,96,104,110,111', '1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37', 0 , 1,10000)'''
    conn.save_data(config_sql, (fp_id, ))


if __name__ == '__main__':
    sys_arg = sys.argv
    if len(sys_arg) == 1:
        plan_dict = None
    else:
        plan_dict = sys_arg[1]

    if plan_dict is None:
        plan_dict = dict()
        plan_dict['title'] = 'xxx' + str(random.randint(1, 100))
        plan_dict['expect'] = '3'
        plan_dict['expect_unit'] = '0'
        plan_dict['price_max'] = '1000000'
        plan_dict['scope'] = '11'

    plan_dict['publish_date'] = datetime.strftime(datetime.now(), '%Y-%m-%d 00:00:00')
    if plan_dict['scope'] == 15 or plan_dict['scope'] == 26:
        plan_dict['start_date'] = datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')
    else:
        plan_dict['start_date'] = datetime.strftime(datetime.now(), '%Y-%m-%d')

    plan_dict['finish_date'] = datetime.strftime(datetime.now(), '%Y-%m-%d')
    fp_id = add_save_plan(plan_dict)
    insert_stage_info(fp_id)
    insert_debt_config(fp_id)
