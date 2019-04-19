# coding:utf-8
import MySQLdb
import time


print "脚本说明: 历史债权恢复表"
# 数据连接
db = MySQLdb.connect("192.168.1.90","yanfa","yanfa#123","XXXX")
cursor = db.cursor()
sql_count="SELECT count(DISTINCT bo_id) from debt_exchange_account where va_id = 0 and  bank_flag = 1 and status >0 and price>0"
cursor.execute(sql_count)
count=cursor.fetchone()

sql_count2="SELECT count(1) from "
cursor.execute(sql_count2)
count2=cursor.fetchone()
print "数据量共： %s 条,已完成 %s 条 " %(count[0],count2[0])


sql_count="SELECT count(DISTINCT bo_id) from debt_exchange_account where va_id = 0 and  bank_flag = 1 and status >0 and price>0 and create_time<'2018-04-25'"
cursor.execute(sql_count)
count=cursor.fetchone()

sql_count2="SELECT count(DISTINCT bo_id) from borrows_loan_msg"
cursor.execute(sql_count2)
count2=cursor.fetchone()
print "数据量共： %s 条,已完成 %s 条 " %(count[0],count2[0])

# 每次1000条
sql_select="SELECT DISTINCT bo_id from debt_exchange_account where va_id = 0 and  bank_flag = 1 and status >0 and price>0 and  bo_id not in (select distinct bo_id from borrows_loan_msg) and create_time<'2018-04-25' limit 1000 ";
sql_insert="insert into borrows_loan_msg (bo_id,status,version) values(%s,0,1)";

print "begin",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,"\n"

flag_continue=True
# 批次号
batch_num=0
# 总数据量
total_count=0
while flag_continue:
    batch_num=batch_num+1
    cursor.execute(sql_select)
    datas = cursor.fetchall()
    if len(datas)>0:
        time.sleep(0.5)
        total_count=total_count+len(datas)
        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) ,"batch",batch_num,":", len(datas)
        for data in datas:
            cursor.execute(sql_insert,(str(data[0]),))
            db.commit()
    else:
        flag_continue=False


db.close()


print "\n","total num:",total_count
print "done!",time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

