from nono.base import Connection

db_factory = Connection()


def fetch_lend_bo():
    dea_sql = '''SELECT DISTINCT bo_id from debt_exchange_account where bo_id in (
            SELECT bo_id from debt_exchange_account
            where va_id =0 and status >0 and price >0 and bank_flag = 1
            ) and va_id >0 and status >0 and price>0 and bank_flag = 1;
            '''
    return db_factory.query_tuple(dea_sql)

with open("bo.text", "aw") as f:
    bo_list = map(lambda x: str(x[0]), fetch_lend_bo())
    f.writelines(bo_list)
