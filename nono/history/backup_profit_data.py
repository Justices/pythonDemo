# coding:utf-8

import MySQLdb
from MySQLdb.cursors import DictCursor
import time
from DBUtils.PooledDB import PooledDB



print "收益表数据备份开始"

XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.4.136', db='XXXX', user='u_XXXX', passwd='XXXX@2018', port=3306,charset='utf8', blocking=True)

max_id = 0

debt_sql = '''SELECT  dea.id, isqf.va_id, dea.bo_id from XXXX.XXXX_stage_quit_form isqf 
inner join XXXX.debt_exchange_account dea 
on isqf.va_id = dea.va_id 
where isqf.quit_status = 2 and isqf.quit_mode <> 3 and isqf.id > 613850 and dea.id >%s ORDER BY dea.id asc limit 1000'''

profit_data = '''UPDATE XXXX.XXXX_profit_detail
        SET is_delete = 1
        where trans_id = %s and va_id = %s and bo_id =  %s and is_delete= 0  ;
'''

trans_sql = '''select trans_id from XXXX.XXXX_profit_calc_task where bo_id = %s'''


def update_status(sql, param=None):
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


def update_profit_status(profit_list):
    for br_item in profit_list:
        bo_id = br_item['bo_id']
        va_id = br_item['va_id']
        trans_result = query(trans_sql, (bo_id, ))
        if len(trans_result) == 0:
            continue
        for trans_item in trans_result:
            trans_id = trans_item['trans_id']
            update_status(profit_data, (trans_id, va_id, bo_id))


if __name__ == '__main__':
    br_list = query(debt_sql, (max_id, ))
    while len(br_list) >0:
        print "the current id is %s "%(max_id)
        max_id = br_list[-1]['id']
        update_profit_status(br_list)
        br_list = query(debt_sql, (max_id, ))
        time.sleep(0.5)









