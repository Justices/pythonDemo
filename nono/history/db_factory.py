from DBUtils.PooledDB import PooledDB
import MySQLdb
from MySQLdb.cursors import DictCursor


XXXX_pool = PooledDB(MySQLdb, 4, host='192.168.3.253', db='XXXX', user='u_XXXX', passwd='XXXX@2018', port=3306,charset='utf8', blocking=True)
loan_pool = PooledDB(MySQLdb, 4, host='192.168.3.155', db='XXXX', user='yanfa', passwd='yanfa#123', port=3306,charset='utf8', blocking=True)


def query(sql, param=None, XXXX='XXXX'):
    conn = get_conn(XXXX)
    dbc = conn.cursor(cursorclass=DictCursor)
    dbc.execute(sql, param)
    result = dbc.fetchall()
    dbc.close()
    conn.close()
    return result


def get_conn( XXXX):
    if XXXX == 'XXXX':
        return XXXX_pool.connection()
    return loan_pool.connection()


def insert_data( sql, param=None, XXXX='XXXX'):
    conn = get_conn(XXXX)
    dbc = conn.cursor(cursorclass=DictCursor)
    try:
        dbc.execute(sql, param)
        conn.commit()
        dbc.close()
        conn.close()
    except:
        print "data roll back"
        conn.rollback()

