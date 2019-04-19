from functools import partial

from threadpool import ThreadPool, makeRequests

from nono.base import process_request
from nono.db_factory import DBFactory

pool = ThreadPool(10)

factory = DBFactory()

url = 'http://XXXX.prod.com/XXXX-XXXX/debtSale/transfer'


def fetch_debt_list(start, end):
    dea_sql = '''SELECT
                        sum(unpaid.price_principal) price,
                        unpaid.bo_id,
                        unpaid.user_id,
                        seri_no,
                        va_id
                FROM XXXX_borrows_accept_unpaid unpaid 
                INNER JOIN XXXX_borrows_info bo 
                  ON unpaid.bo_id = bo.bo_id
                WHERE bo.p_id = 87 AND unpaid.is_delete = 0 AND unpaid.va_id = 23665
                    GROUP BY unpaid.bo_id, seri_no,va_id
                    HAVING count(unpaid.id) = 6
                    ORDER BY sum(unpaid.price_principal) DESC
            limit %s , %s '''

    result_list = factory.query(dea_sql, (start, end))
    return result_list


def filter_by_expect(dea_item):
    bo_id = dea_item['bo_id']
    seri_no = dea_item['seri_no']
    if dea_item['price'] <= 0 :
        return False

    ba_sql = ''' SELECT 1
            FROM debt_package_borrows dpb
            WHERE dpb.bo_id = %s AND dpb.seri_no = %s
                  AND is_delete = 0  '''
    count = factory.query_one(ba_sql, (bo_id, seri_no))
    if len(count) == 0:
        return True
    return False


def process_debt_transfer(target_id, dea_list):
    filter_list = list(filter(filter_by_expect, dea_list))
    for dea_item in filter_list:
        param = dict()
        param['pid'] = dea_item["bo_id"]
        param['userId'] = dea_item["user_id"]
        param['vaId'] = dea_item['va_id']
        param['targetFpId'] = 0
        param['targetVaId'] = target_id
        param['fromType'] = 1
        param['seriNo'] = dea_item["seri_no"]
        process_request(url, param)
        print param


def fetch_pool_dict(part_list):
    dea_list_dict = dict()
    if len(part_list) > 4:
        dea_list_dict[0] = part_list[::1]
        dea_list_dict[1] = part_list[::2]
        dea_list_dict[2] = part_list[::3]
        dea_list_dict[3] = part_list[::4]
    if len(part_list) > 3:
        dea_list_dict[0] = part_list[::1]
        dea_list_dict[1] = part_list[::2]
        dea_list_dict[2] = part_list[::3]
    elif len(part_list) > 2:
        dea_list_dict[0] = part_list[::1]
        dea_list_dict[1] = part_list[::2]
    elif len(part_list) > 1:
        dea_list_dict[0] = part_list[::1]
    else:
        dea_list_dict[0] = part_list
    return dea_list_dict


def main():
    page_size = 50
    start_index = 0
    one_list_col = dict()
    method_partial = partial(process_debt_transfer, 959087)
    flag = True
    while flag:
        one_list = fetch_debt_list(start_index * page_size, (start_index + 1) * page_size)
        one_list_col[start_index] = one_list
        start_index = start_index + 1
        if len(one_list) > 0:
            request = makeRequests(method_partial, fetch_pool_dict(list(one_list)).values())
            [pool.putRequest(req) for req in request]
            continue
        flag = False

    pool.wait()


if __name__ == '__main__':
    main()
