
from nono.base import process_request, Connection

db_conn = Connection()
v3_sql = '''select id, bo_id from debt_exchange_account where va_id = 23665 and price >0 and status >0  and id > %s limit 500 '''

url = '''http://192.168.3.35:8082/XXXX-XXXX/ass/deleteCache'''


def fetch_result(max_id=0L):
    result = db_conn.query(v3_sql, param=(max_id, ))
    return result


def clear_cache(dea_list):
    for item in dea_list:
        bo_id = item["bo_id"]
        cache_key = "da_fang_dong_"+str(bo_id)
        process_request(path=url, data={"key":cache_key})


if __name__ == '__main__':

    result_list = fetch_result(0L)
    while len(result_list) >0 :
        max_id = result_list[-1]
        clear_cache(result_list)
        result_list = fetch_result(max_id["bo_id"])

