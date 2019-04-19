from MySQLdb.cursors import DictCursor
from smtplib import SMTP_SSL
from email.header import Header
from email.mime.text import  MIMEText
import smtplib

class XXXXMonitor(object):

    conn = None

    def __init__(self,conn):
        # if isinstance(conn, Connection.__class__):
        self.conn = conn
        # raise TypeError


    def queryPendingTask(self,sql):
        cur = self.conn.cursor(cursorclass=DictCursor)
        cur.execute(sql)
        task_list = cur.fetchall()
        if task_list is None:
            print 'there is no pending task '

            return
        cache = XXXXRedisCache()
        cache.set_data('pending_prod_task_list', task_list)
        cur.close()


    def getFinancePlanMatchRate(self,sql):
        pass

    def close_conn(self):
        self.conn.close()



class XXXXRedisCache(object):

    def __init__(self):
        if not hasattr(XXXXRedisCache,'pool'):
            XXXXRedisCache.create_pool()
        self._connection =  Redis(connection_pool=XXXXRedisCache.pool)

    @staticmethod
    def create_pool():
        XXXXRedisCache.pool = ConnectionPool(host='192.168.3.111',
                                             port=29000,
                                             password='redis_test')


    def get_data(self,key):
        '''
        GET THE DATA BY KEY 
        :param key: 
        :return: 
        '''
        return self._connection.get(key)

    def set_data(self,key,value):
        '''
        SET THE DATA WITH (KEY,VALUE)
        :param key: 
        :param value: 
        :return: 
        '''

        self._connection.set(key,value,px=24*60*60)

    def del_data(self,key):
        self._connection.delete(key)
        return

class EmailModule(object):

    smtp_server = 'smtp.163.com'
    smtp_user = 'garyliu123@163.com'
    smtp_pass = 'citi0309'

    send_user = 'garyliu123@163.com'
    receivers = ['liujiaping@nonobank.com']



    def get_smtp(self):
        try:
            moniotr_smtp = SMTP_SSL(host=self.smtp_server, port=465)
            moniotr_smtp.login(self.smtp_user, self.smtp_pass)
            return moniotr_smtp
        except smtplib.SMTPException:
            print "can't login the smtp server "

    def send_message(self, message,subject):
        message = MIMEText(message,'plain','utf-8')
        message['From'] = Header('Test','utf-8')
        message['To'] = Header('test','utf-8')
        message['Subject'] = Header(subject,'utf-8')
        try:
            monitor_smtp = self.get_smtp()
            monitor_smtp.sendmail(self.send_user, self.receivers, message.as_string())
            monitor_smtp.quit()
        except smtplib.SMTPException:
            print 'send email failed'



