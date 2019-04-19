# coding:utf-8
import MySQLdb
from DBUtils.PooledDB import PooledDB
from MySQLdb.cursors import DictCursor

print 'start to update the status of the profit status ...'

XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.4.136', db='XXXX', user='u_XXXX', passwd='XXXX@2018', port=3306,charset='utf8', blocking=True)

minId = 0

debt_sql = '''SELECT  dea.id, isqf.va_id, ct.bo_id, ct.trans_id from XXXX.XXXX_stage_quit_form isqf 
inner join XXXX.debt_exchange_account dea 
on isqf.va_id = dea.va_id 
where isqf.quit_status = 2 and isqf.quit_mode <> 3 and isqf.id > 612324 ORDER BY ct.id asc'''

ds_sql = '''UPDATE XXXX.XXXX_profit_detail
        SET is_delete = 1
        where trans_id = %s and va_id = %s and bo_id %s ;
'''


def update(sql, param=None):
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


def update():
    debt_info_list = query(debt_sql)
    if len(debt_info_list) == 0:
        print ' have no data to update'
        return
    for id_item in debt_info_list:
        ds_id = id_item['id']
        update(ds_sql, (ds_id, ))

if __name__ == '__main__':
    update_debt_sale()