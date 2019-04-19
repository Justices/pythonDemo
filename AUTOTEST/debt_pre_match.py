import sys
from decimal import Decimal
from functools import partial

from  AUTOTEST.po.finance_plan import fetch_vip_form_by_fp
from nono.db_factory import DBFactory

conn = DBFactory()


def is_debt_matched_not_started(vf_id):
    sql = 'select * from XXXX_finance_plan_match_form where vf_id = %s  '
    match_form = conn.query_one(sql, (vf_id,))
    if match_form is None or match_form['status'] == 1:
        print ("the form %s have no match form record" % vf_id)
        return True
    return False


def check_debt_match(vf_id, amount, scope):
    if is_debt_matched_not_started(vf_id=vf_id):
        print ("the form %s for the plan %s didn't start to mach debt " % (vf_id, fp_id))
        return
    if scope == 11:
        match_sql = "select * from XXXX_intimate_debt_match where vf_id = %s"
    if scope == 1:
        match_sql = "select * from XXXX_xinke_debt_match where vf_id = %s"
    if scope == 15 or scope == 26:
        match_sql = "select * from XXXX_stage_debt_match where vf_id = %s"

    debt_match_list = conn.query(match_sql, (vf_id, ))
    sum_price = reduce(calc_price, debt_match_list, {'price':Decimal(0.00)})
    if sum_price > amount:
        print ("there is some error sum_price %s , amount %s"%(sum_price, amount))

    new_borrow_list = filter(partial(filter_type_package, 4), map(partial(fetch_package_by_id, scope), debt_match_list))
    check_new_borrow_match(new_borrow_list, vf_id)
    return sum_price


def filter_type_package(package_type, package_item):
    return package_type == package_item['type']


def calc_price(item_pre, item_aft):

    if isinstance(item_pre, Decimal):
        return item_pre + item_aft['price']
    return  item_pre['price'] + item_aft['price']


def cal_package_price(package_pre, package_aft):
    if isinstance(package_pre, Decimal):
        return package_pre + package_aft['use_price']
    return package_pre['use_price'] + package_aft['use_price']


def check_new_borrow_match(match_list, vf_id):
    if len(match_list) == 0:
        return

    for item in match_list:
        amount = item['price']
        if amount%100 > 0:
            print ("the match record %s with the form %s are invalid"%(item[['id'], item['vf_id']]))
            return

    print ("the %s for the  match of the new borrows are ok "%vf_id)


def fetch_package_by_id(scope, package):
    package_id = package['package_id']
    if scope == 11:
        package_sql = "select * from XXXX_intimate_debt_package where id = %s"
    if scope == 1:
        package_sql = "select * from XXXX_xinke_debt_package where id = %s"
    if scope == 15 or scope == 26:
        package_sql = "select * from XXXX_stage_debt_package where id = %s"

    return conn.query_one(package_sql, (package_id,))


def fetch_package_by_fp_id(fp_id, scope):
    if scope == 11:
        package_sql = "select * from XXXX_intimate_debt_package where fp_id = %s"
    if scope == 1:
        package_sql = "select * from XXXX_xinke_debt_package where fp_id = %s"
    if scope == 15 or scope == 26:
        package_sql = "select * from XXXX_stage_debt_package where fp_id = %s"

    package_list = conn.query(package_sql, (fp_id, ))
    return package_list


if __name__ == '__main__':
    args = sys.argv
    if len(args) >1:
        fp_id = args[1]
    else:
        fp_id = 8004

    form_list = fetch_vip_form_by_fp(conn, fp_id)
    total_amount = 0
    total_match_price = 0
    scope = 11
    for form_item in form_list:
        vf_match_price = check_debt_match(form_item["id"], form_item['amount'], form_item['scope'])
        total_amount += form_item['amount']
        total_match_price += vf_match_price
        scope = form_item['scope']

    used_package_price = reduce(cal_package_price, fetch_package_by_fp_id(fp_id=fp_id, scope = scope))

    print ("the vf amount is %s, matched price is %s, used package price is %s"%(total_amount, total_match_price, used_package_price))


