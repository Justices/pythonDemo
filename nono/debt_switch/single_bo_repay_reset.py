from DBUtils.PooledDB import PooledDB
import MySQLdb
from MySQLdb.cursors import DictCursor


XXXX_pool = PooledDB(MySQLdb, 4, host='db-XXXX-max-monitor.prod.com', db='XXXX', user='u_XXXX_monitor', passwd='NZCXp0HGwl96zQmAcPOe', port=3306, charset='utf8',blocking=True)
# XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.3.253', db='XXXX_XXXX', user='u_XXXX', passwd='XXXX@2018', port=3306, charset='utf8',blocking=True)


def conn_dbnono():
    db = XXXX_pool.connection()
    # return db


def fetch_all_with_dict(sql, param_tuple=None, conn=None):
    if conn is None:
        conn = conn_dbnono()
    dict_cur = conn.cursor(cursorclass=DictCursor)
    try:
        dict_cur.execute(sql, param_tuple)
        return dict_cur.fetchall()
    except Exception, e:
        print "there're exception for the jdbc connection"
        raise e
    finally:
        dict_cur.close()
        conn.close()


def fetch_bo_list():
    single_bo = '''SELECT bo_id, plan_time FROM XXXX_single_debt_bo where repay_date is null and status< 10 and create_time > '2018-07-05' order by id asc'''
    single_list = fetch_all_with_dict(single_bo)
    if len(single_list) == 0:
        return
    for single_item in single_list:
        param = dict()
        param["boId"] = single_item['bo_id']
        param["planTime"] =  single_item['plan_time'].strftime('%Y-%m-%d')
        print ''' curl -i -X POST -H "Content-Type:application/json" -d ' ''' + str(param) + ''' ' 'http://XXXX.pre.com/XXXX-XXXX/ass/resetRepayDate' '''


if __name__ == '__main__':
    fetch_bo_list()

