def fetch_entity_by_id(conn, table_name, field_list):
    str_sql = "select * from " + table_name + "where 1 = 1 "
    if not isinstance(field_list, dict):
        print "parameter error ,the field are not correct format"
        return

    for key in field_list.keys():
        str_sql += "and " + key + " =%("+key+")s"
    return conn.query(str_sql, field_list)