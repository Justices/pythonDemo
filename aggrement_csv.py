#encoding:utf-8
import csv
import urllib2
import os

file_path = "/Users/liujiaping/Downloads/location.csv"
base_dir = '/Users/liujiaping/Downloads/agreenment'
base_url = "http://www.nonobank.com"
dir_location = "file_location.csv"

def parse_location(csv_path='location.csv'):
    data = csv.reader(open(csv_path))
    result = list()
    for row in data:
        location_dict = dict()
        location_dict["user_id"] = row[0]
        location_dict['location'] = row[1]
        result.append(location_dict)
    return result


def down_load(url, fileName):
    f = urllib2.urlopen(url)
    data = f.read()
    with open(fileName, "wb") as code:
        code.write(data)


def make_file_dir(file_path):
    dir_data = csv.reader(open(file_path))
    for row in dir_data:
        os.chdir(base_dir)
        for item in row:
            dir_path = os.path.join(base_dir, item)
            os.makedirs(dir_path, mode=0777)
            os.chdir(dir_path)


def download_file():
    file_list = parse_location(file_path)
    for item  in file_list:
        file_name = "电子签章_"+item["user_id"]+".pdf"
        file_url = base_url + item["location"]
        try :
            down_load(file_url, file_name)
        except :
            print file_url
            print file_name


if __name__ == '__main__':
    make_file_dir(dir_location)