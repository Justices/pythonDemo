from copy import deepcopy
from nono.db_factory import DBFactory
import threadpool
from threadpool import makeRequests
import time

factory = DBFactory()
pool = threadpool.ThreadPool(10)


def fetch_dea_list(fp_id):
    if fp_id is None:
        return
    va_sql = "select * from vip_account where fp_id = %s"
    param_tuple = (fp_id,)
    va_list = factory.query(va_sql, param_tuple)
    result = list()
    dea_dict = dict()
    for va_item in va_list:
        va_id = va_item['id']
        # fetch_ba_by_va(va_item['id'])
        dea_sql = '''select * from debt_exchange_account where va_id = %s'''
        dea_list = factory.query(dea_sql, (va_id,))
        result.extend(list(dea_list))

    if len(result) == 0:
        return dict()
    dea_dict[1] = result[::4]
    dea_dict[2] = result[1::4]
    dea_dict[3] = result[2::4]
    dea_dict[4] = result[3::4]
    return dea_dict


def process_dea_update(fp_id):
    dea_dict = fetch_dea_list(fp_id)
    if len(dea_dict) == 0:
        print 'the fp id %s have no dea list', fp_id
        return
    requests = makeRequests(callable_=process_ba_list_update, args_list=dea_dict.values())
    [pool.putRequest(req) for req in requests]
    # t.join()


def process_ba_list_update(dea_list):
    for dea_item in dea_list:
        process_ba_update(dea_item)


def process_ba_update(dea_item):
    va_id = dea_item['va_id']
    bo_id = dea_item['bo_id']
    ba_sql = "select DISTINCT plan_time from borrows_accept where  bo_id = %s and is_pay=0 and price>0"
    param_tuple = (bo_id,)
    ba_list = factory.query(ba_sql, param_tuple)
    for ba_item in ba_list:
        plan_time = ba_item['plan_time']
        if plan_time is None:
            continue
        if ba_item['plan_time'].isoformat() == '2016-03-09':
            update_ba_plan_time(bo_id)

    unpaid_sql = "update XXXX_borrows_accept_unpaid set is_delete = 1 where va_id = %s and plan_time=%s AND  bo_id = %s"
    factory.save_data(unpaid_sql, (va_id, '2016-03-09', bo_id))

    ba_sql = "select * from borrows_accept where va_id = %s and bo_id = %s and is_pay=0 and price>0"
    param_tuple = (va_id, bo_id)
    ba_list = factory.query(ba_sql, param_tuple)
    for ba_item in ba_list:
        va_id = ba_item['va_id']
        bo_id = ba_item['bo_id']
        seri_no = ba_item['seri_no']
        plan_time = ba_item['plan_time']
        if check_unpaid_record(va_id, bo_id, seri_no, plan_time):
            continue
        unpaid_item = deepcopy(ba_item)
        del unpaid_item['id']
        is_transfer = ba_item['ba_is_transfer']
        if is_transfer is None:
            is_transfer = 0

        unpaid_item['ba_is_transfer'] = is_transfer
        save_item(unpaid_item)


def check_unpaid_record(va_id, bo_id, seri_no, plan_time):
    un_paid_sql = 'select * from XXXX_borrows_accept_unpaid where va_id = %s and bo_id = %s and is_delete = 0 and plan_time = %s'
    if seri_no is None:
        un_paid_sql = un_paid_sql + ' and seri_no is null'
        param_tuple = (va_id, bo_id, plan_time.isoformat())
    elif seri_no.strip() == '':
        param_tuple = (va_id, bo_id, plan_time.isoformat())
    else:
        un_paid_sql = un_paid_sql + ' and seri_no = %s'
        param_tuple = (va_id, bo_id, plan_time.isoformat(), seri_no)
        print un_paid_sql
    un_paid_list = factory.query(un_paid_sql, param_tuple)
    if len(un_paid_list) == 0:
        return False
    return True





def save_item(ba_item):
    ba_sql = '''insert into XXXX_borrows_accept_unpaid (user_id, bo_id,is_vip, va_id, original_principal, price_principal, price_interest, price, price_punish, plan_time, success_time, is_pay, owner_rate, is_lender,  expect_num, ba_transfer, ba_is_transfer,seri_no ) values (%(user_id)s,%(bo_id)s,%(is_vip)s,%(va_id)s,%(original_principal)s,%(price_principal)s,%(price_interest)s,%(price)s,%(price_punish)s,%(plan_time)s,%(success_time)s,%(is_pay)s,%(owner_rate)s,%(is_lender)s,%(expect_num)s,%(ba_transfer)s,%(ba_is_transfer)s,%(seri_no)s )'''
    # param_tuple = tuple((k, v)for k,v in ba_item.iteritems())
    factory.save_data(ba_sql, ba_item)


def update_ba_plan_time(bo_id):
    bo_sql = 'UPDATE borrows_accept ba SET plan_time=(SELECT br.br_time FROM borrows_repayment br WHERE br.br_expect_num=ba.expect_num AND br.bo_id=%s) WHERE ba.bo_id=%s'
    param_tuple = (bo_id, bo_id)
    factory.save_data(bo_sql, param_tuple)


def fetch_pending_plan():
    fp_sql = '''select id
                    from finance_plan
                    where expect_unit=0
                      AND id  <>140 AND id <> 6932
                      AND DATE_ADD(start_date, INTERVAL expect MONTH) < date_add(now(), INTERVAL 6 day)
                      AND DATE_ADD(start_date, INTERVAL expect MONTH) > date_add(now(), INTERVAL 1 day)
                    AND support_stage_pack IN (0,2)
                    union all
                    select id
                    from finance_plan
                    where expect_unit=1
                      AND id  <>140 AND id <> 6932
                      AND DATE_ADD(start_date, INTERVAL expect DAY) < date_add(now(), INTERVAL 6 day)
                      AND DATE_ADD(start_date, INTERVAL expect DAY) > date_add(now(), INTERVAL 1 day)
                    AND support_stage_pack IN (0,2)'''
    fp_list = factory.query(fp_sql)
    return fp_list


def fetch_dea_item(start, limit):
    sql= '''select * from debt_exchange_account dea
              where va_id= 23665
              and user_id=920272
              and hold_num > 0
              and price > 0
              and status <>0 and bank_flag = 1 limit %s, %s'''
    return factory.query(sql, (start, limit))

if __name__ == '__main__':
    # fp_list = fetch_pending_plan()
    # for fp_id in fp_list:
    #     process_dea_update(fp_id['id'])


    page_size = 500
    start_index = 0
    while True:
        dea_list = fetch_dea_item(start=start_index*page_size, limit = (start_index+1)*page_size)
        if len(dea_list) == 0 :
            break
        start_index = start_index + 1
        process_ba_list_update(dea_list)
    # pool.wait()

    # def job():
    #     fp_list = fetch_pending_plan()
    #     for fp_id in fp_list:
    #         process_dea_update(fp_id['id'])
    #     pool.wait()
    #
    # schedule.every().day.at("01:00").do(job_func=job)
    #
    # while True:
    #     if time.localtime().tm_hour < 11:
    #         schedule.run_pending()
