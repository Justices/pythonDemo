import csv
from collections import defaultdict
from nono.base import get_transfer_deb_param, process_request

path = "http://XXXX.pre.com/XXXX-XXXX/debtSale/transfer"


def read_data_from_csv(file_name="debt_switch.csv"):
    csv_reader = csv.reader(open(file_name))
    data_list = list()
    for row in csv_reader:
        data_item = defaultdict()
        data_item['bo_id'] = row[0]
        data_item['user_id'] = row[1]
        data_item['seri_no'] = row[2]
        data_item['va_id'] = row[3]
        data_list.append(data_item)
    return data_list


def transfer_debt(param_list):
    for dea_item in param_list:
        param = get_transfer_deb_param(0, dea_item, target_type=0)
        # print param
        process_request(path, param)

if __name__ == '__main__':
    data_list = read_data_from_csv()
    transfer_debt(data_list)
