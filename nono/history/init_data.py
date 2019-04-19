# coding:utf-8

import MySQLdb
from MySQLdb.cursors import DictCursor
import time
from DBUtils.PooledDB import PooledDB



print "脚本说明: 历史债权信息补充"

XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.4.136', db='XXXX', user='u_XXXX', passwd='XXXX@2018', port=3306,charset='utf8', blocking=True)

max_id = 246748

br_sql = '''select br.id, br.bo_id, br.plan_time from XXXX_history_br_info br LEFT JOIN XXXX.XXXX_history_repay_bo bo  
 on br.bo_id = bo.bo_id
 where br.id > %s and bo.id is null  ORDER by br.id asc limit 100;'''


insert_sql = '''INSERT INTO XXXX_history_debt_info(
    `user_id`,
    `va_id` ,
    `bo_id` ,
    `price` ,
    `price_principal`,
    `price_interest`,
    `plan_time`,
    `seri_no`,
    `owner_rate` ,
    `is_lender` ,
    `expect_num`)
    SELECT  user_id,
    va_id ,
    bo_id ,
    price ,
    price_principal,
    price_interest,
    plan_time,
    seri_no,
    owner_rate,
    is_lender ,
    expect_num
FROM XXXX_borrows_accept_paid 
WHERE  is_pay=1 and seri_no <> '' and price >0  and bo_id = %s and plan_time = %s and seri_no is not NULL;'''


def insert_data(sql, param=None):
    conn = XXXX_pool.connection()
    dbc = conn.cursor(cursorclass=DictCursor)
    try:
        dbc.execute(sql, param)
        conn.commit()
        dbc.close()
        conn.close()
    except Exception, e:
        print "data roll back %s"%(str(param))
        conn.rollback()


def query(sql, param=None):
    conn = XXXX_pool.connection()
    dbc = conn.cursor(cursorclass=DictCursor)
    dbc.execute(sql, param)
    result = dbc.fetchall()
    dbc.close()
    conn.close()
    return result


def insert_debt_info(br_list):
    for br_item in br_list:
        bo_id = br_item['bo_id']
        plan_time = br_item['plan_time']
        insert_data(insert_sql, (bo_id, plan_time))

if __name__ == '__main__':
    br_list = query(br_sql, (max_id, ))
    while len(br_list) >0:
        print "the current id is %s "%(max_id)
        max_id = br_list[-1]['id']
        insert_debt_info(br_list)
        br_list = query(br_sql, (max_id, ))
        time.sleep(0.5)









