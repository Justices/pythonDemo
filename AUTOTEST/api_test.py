from random import randint
from threadpool import ThreadPool, makeRequests
from nono.base import process_request, process_get_request
import datetime

pool = ThreadPool(15)

debt_pc_url = 'http://192.168.3.35:8082/XXXX-XXXX/debtSaleView/getUserDebtBuyLog'
debt_app_url = 'http://192.168.3.35:8082/XXXX-XXXX/debtSaleView/getUserDebtBuyRecord'
debt_finance_url = 'http://192.168.3.35:8082/XXXX-XXXX/debtSaleView/getFinancePlanDebtList'


user_ids = [116333, 5649219, 875057, 3459821, 910980, 912905, 1067434, 5002078, 5014286, 1261553, 4958561, 1162737, 71297, 3181387, 92419, 1339079, 1057794, 885050, 3379885,
            1067520, 7443843, 103268, 1336329, 7676500, 1186375, 851335, 1289590, 1332489, 1181835, 102141, 115914, 1408725, 913640,  92196, 1301888, 1366400, 100525, 5268164,
            1377113, 3370526, 89572, 281497, 905602, 1237224, 95233, 122956]
va_ids = [290479,87720,87855,87882,15540,87857,69700,88563,89114,280713,88969,65117,66444,66363,66434,66591,299386,279215,64979,300167,65164,66581,330158,328924]

status_list = [0, 1, 2]

url_list = [debt_pc_url, debt_app_url, debt_finance_url]


def build_debt_param(user_id):
    param = dict()
    param['userId'] = user_id
    param['pageSize'] = 10
    param['pageIndex'] = 1
    param['status'] = status_list[randint(0,2)]
    return param


def build_finance_param(va_id):
    param = dict()
    param['vaId'] = va_id
    param['pageSize'] = 10
    param['pageIndex'] = 1
    return param


def process_deb_pc_request(user_id):
    param = build_debt_param(user_id)
    before = datetime.datetime.now()
    process_get_request(debt_pc_url, param)
    print "the user %s get debt buy log from pc take %s ms" %(user_id, (datetime.datetime.now()-before).microseconds/1000)


def process_deb_app_request(user_id):
    param = build_debt_param(user_id)
    before = datetime.datetime.now()
    process_request(debt_pc_url, param)
    print "the user %s get debt buy log from XXXX take %s ms" %(user_id, (datetime.datetime.now()-before).microseconds/1000)


def process_finance_debt(va_id):
    param = build_finance_param(va_id)
    before = datetime.datetime.now()
    process_get_request(debt_finance_url, param)
    print "the va  %s list the finance debt  %s ms" %(va_id, (datetime.datetime.now()-before).microseconds/1000)


if __name__ == '__main__':
    pc_request = makeRequests(process_deb_pc_request, user_ids)
    app_request = makeRequests(process_deb_app_request, user_ids)
    finance_request = makeRequests(process_finance_debt, va_ids)
    [pool.putRequest(req) for req in pc_request]
    [pool.putRequest(req) for req in app_request]
    [pool.putRequest(req) for req in finance_request]
    pool.wait()