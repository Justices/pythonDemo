from nono.db_factory import DBFactory

conn = DBFactory()


def fetch_shell_off_task(fp_id):
    task_sql = 'select * from XXXX_finance_plan_off_shelf_task where fp_id=%s'
    return conn.query(task_sql, (fp_id, ))


def check_match_record(task_item):
    pass


def check_package_record(task_item):
    pass


def check_package_borrows(fp_id):
    pass


def check_match_form(match_form):
    pass