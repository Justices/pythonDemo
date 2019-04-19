from nono.base import process_request, Connection

path = 'http://XXXX.prod.com/XXXX-XXXX/financePlan/batchCreateAutoBidTask'
conn = Connection()

def process_swith_bidder(fp_item):
    ''' path: http://XXXX.prod.com/XXXX-XXXX/financePlan/batchCreateAutoBidTask
        data : {
        "fpId": 8932,
        "autoBid": 1
    }'''
    param = dict()
    param["fpId"] = fp_item['id']
    param['autoBid'] = 1
    print "the fp is %s"%(fp_item["id"])
    process_request(path, param)


def get_fp_list(start_date, end_date):
    fp_sql = '''SELECT * from finance_plan where start_date >=%s and start_date <=%s and finance_plan.platform_type = 2 and finance_plan.scope = 11 and auto_bid = 0'''
    fp_result_list  = conn.query(fp_sql, (start_date, end_date))
    return fp_result_list

if __name__ == '__main__':
    start_date = '2017-11-19'
    end_date = '2017-11-26'
    fp_list = get_fp_list(start_date, end_date)
    if len(fp_list) == 0:
        print "no plan need to switch auto bidder"

    for fp_item in fp_list:
        process_swith_bidder(fp_item)



