from db_factory import DBFactory

from nono.vip_plan.RepaireBa import fetch_pending_plan

conn = DBFactory()



def fetch_dea_list():
    plan_list = fetch_pending_plan()
    dea_set = list()
    for plan in plan_list:
        va_sql = '''select id from vip_account where fp_id = %s and is_cash = 0 '''
        vip_list = conn.query(va_sql, (plan['id'],))
        for va_id in vip_list:
            dea_sql = '''select * from debt_exchange_account where va_id = %s and hold_num>0 and locking_num=0 and status = 1'''
            dea_list = conn.query(dea_sql, (va_id['id'],))
            dea_set.extend(list(dea_list))
    return dea_set


def is_bo_over_due(bo_id):
    bo_sql = '''select 1 from XXXX_overdue_bo_tmp where bo_id = %s'''
    flag = conn.query(bo_sql, (bo_id,))
    if len(flag)== 0:
        return False
    return True


def calc_ba_debt_value():
    dea_list = fetch_dea_list()
    debt_price = 0
    for dea in dea_list:
        bo_id = dea['bo_id']
        flag = is_bo_over_due(bo_id)
        if flag:
            continue
        va_id = dea['va_id']
        ba_sql = '''select sum(price_principal) as price from borrows_accept where va_id = %s and bo_id = %s and is_pay = 0 '''
        price  = conn.query_one(ba_sql, (va_id, bo_id))
        if price['price'] is None:
            debt_price +=0
        else:
            debt_price +=price['price']

    return debt_price


def calc_unpaid_ba_debt_value():
    dea_list = fetch_dea_list()
    debt_price = 0
    for dea in dea_list:
        bo_id = dea['bo_id']
        flag = is_bo_over_due(bo_id)
        if flag:
            continue
        va_id = dea['va_id']
        ba_sql = '''select sum(price_principal) as price from XXXX_borrows_accept_unpaid where va_id = %s and bo_id = %s and is_pay = 0  and is_delete = 0 '''
        price = conn.query(ba_sql, (va_id, bo_id))
        if price['price'] is None:
            debt_price += 0
        else:
            debt_price += price['price']

    return debt_price

if __name__ == '__main__':
    import sys
    use_ba = sys.argv[1]
    if use_ba:
        debt_value = calc_ba_debt_value()
        print "the current pending packaged debt value from  ba is %l", debt_value/10000
    else:
        debt_value = calc_unpaid_ba_debt_value()
        print "the current pending packaged debt value from unpaid is %l", debt_value/10000
