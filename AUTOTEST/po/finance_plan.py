from base import fetch_entity_by_id


def fetch_plan_by_id(conn, fp_id):
    return fetch_entity_by_id(conn, "finance_plan", {"id":fp_id})


def fetch_stage_info_by_id(conn, fp_id):
    return fetch_entity_by_id(conn, "XXXX_finance_plan_stage_info", {"fp_id": fp_id})


def fetch_vip_form_by_fp(conn, fp_id):
    sql = 'select  vf.id,amount, fp.* from vip_form vf left join finance_plan fp on vf.fp_id = fp.id  where fp_id = %s'
    form_list = conn.query(sql, (fp_id,))
    return form_list


def fetch_match_form_by_vf(conn, vf_id):
    return fetch_entity_by_id(conn, "XXXX_finance_plan_match_form", {"vf_id":vf_id})


