#encoding:utf-8
import os
base_dir = '/Users/liujiaping/Downloads/11.4签署的协议的副本-V1.1'

file = open("user_id.text", "rw")

def fetch_user_id():
    for subdir in os.listdir(base_dir):
        if subdir == '.DS_Store':
            continue
        for dir in os.listdir(os.path.join(base_dir, subdir)):
            print dir

if __name__ == '__main__':
    fetch_user_id()