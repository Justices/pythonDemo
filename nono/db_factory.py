from DBUtils.PooledDB import PooledDB
import MySQLdb
from MySQLdb.cursors import DictCursor


class DBFactory:
    pool = PooledDB(MySQLdb, 2, host='192.168.3.253', db='XXXX_XXXX', user='u_XXXX', passwd='XXXX@2018',
    #                 port=3306)
    # pool = PooledDB(MySQLdb, 2, host = '172.16.0.101', port = 3306, user = 'dev', passwd = 'beX5kFn4', db = 'XXXX')
    # pool = PooledDB(MySQLdb, 2, host='192.168.4.13', db='XXXX', user='yanfa', passwd='yanfa#123',
                     port=3306)
    def __init__(self):
        print 'DB factory'

    def get_conn(self):
        return self.pool.connection()

    def query(self, sql, param=None):
        conn = self.get_conn()
        cursor = conn.cursor(cursorclass=DictCursor)
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    def query_with_tuple(self, sql, param=None):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result

    def save_data(self, sql, param):
        conn = self.get_conn()
        cursor = conn.cursor(cursorclass=DictCursor)
        cursor.execute(sql, param)
        row_id = cursor.lastrowid
        conn.commit()
        cursor.close()
        conn.close()
        return row_id

    def save_many_data(self, sql, param):
        conn = self.get_conn()
        cursor = conn.cursor(cursorclass=DictCursor)
        try:
            cursor.executemany(sql, param)
            conn.commit()
        except Exception, e:
            conn.rollback()
        cursor.close()
        conn.close()

    def query_one(self, sql, param=None):
        conn = self.get_conn()
        cursor = conn.cursor(cursorclass=DictCursor)
        cursor.execute(sql, param)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        if len(result) == 0:
            return dict()
        return result[0]
