from datetime import datetime, timedelta
# from db_factory import DBFactory
from urllib2 import Request
from nono.base import Connection
import urllib2
import json

conn = Connection()


def get_over_time_match_task():
    task_sql = '''SELECT id, from_id as va_id , bo_id, seri_no
        from XXXX_debt_sale_task where
        from_id in (SELECT
        va_id
        from XXXX_stage_quit_form where
        quit_balance_date >=%s and quit_balance_date <= %s and quit_status not in (2, 5) and quit_mode = 2
        ) AND
        status not in (5, 6, 99) and status = 3 and task_type = 1'''
    start_date = datetime.now() + timedelta(days=1)
    end_date = datetime.now() + timedelta(days=3)
    print "start_date " + start_date.strftime("%Y-%m-%d") + ' , end_date ' + end_date.strftime("%Y-%m-%d")
    task_list = conn.query(task_sql, (start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")))
    return task_list


def is_no_match_recording(item):
    bo_id = item['bo_id']
    va_id = item['va_id']
    seri_no = item['seri_no']
    intimate_sql = '''SELECT count(1) as count
    FROM XXXX_intimate_debt_match dm LEFT JOIN
    XXXX_intimate_debt_package dp on dm.package_id = dp.id
    where dm.bo_id = %s and dp.va_id = %s and dp.seri_no = %s and dm.service_status = 2 '''

    count = conn.query_one(intimate_sql, (bo_id, va_id, seri_no))
    if count['count'] == 0:
        return True

    xinke_sql = '''SELECT count(1) as count
    FROM XXXX_xinke_debt_match dm LEFT JOIN
    XXXX_xinke_debt_package dp on dm.package_id = dp.id
    where dm.bo_id = %s and dp.va_id = %s and dp.seri_no = %s and dm.service_status = 2 '''

    count = conn.query_one(xinke_sql, (bo_id, va_id, seri_no))
    if count['count'] == 0:
        return True

    stage_sql = '''SELECT count(1) as count
    FROM XXXX_stage_debt_match dm LEFT JOIN
    XXXX_stage_debt_package dp on dm.package_id = dp.id
    where dm.bo_id = %s and dp.va_id = %s and dp.seri_no = %s and dm.service_status = 2 '''

    count = conn.query_one(stage_sql, (bo_id, va_id, seri_no))
    if count['count'] == 0:
        return True

    return False


def process_task(task_item):
    url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/updateDebtSaleTaskFailed'
    req = Request(url=url)
    req.add_header('Content-type', 'application/json')
    item = json.dumps({'taskId': str(task_item['id'])})
    urllib2.urlopen(req, item)
    print item


def process_task_list():
    task_list = get_over_time_match_task()
    task_tuple = filter(is_no_match_recording, task_list)
    for item in task_tuple:
        process_task(item)


if __name__ == '__main__':
    print process_task_list()
