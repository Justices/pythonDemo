#!/usr/bin/env python
# coding=utf-8
"""
Author: Squall
Last modified: 2011-10-18 16:50
Filename: pool.py
Description: a simple sample for pool class
"""

from multiprocessing import Pool
from time import sleep
from math import pow
from threading import Thread, current_thread


def f(x):
    for i in range(10):
        print '%s --- %s ' % (i, x)
        sleep(1)


def main():
    pool = Pool(processes=3)  # set the processes max number 3
    for i in range(11, 20):
        result = pool.apply_async(f, (i,))
    pool.close()
    pool.join()
    # if result.successful():
    #     print 'successful'


def thread_demo(index):
    print "the current thread %s is the %s thread"%(current_thread().getName(), index)


if __name__ == "__main__":
    debt_list = list()
    for x in xrange(0, 10):
        thread = Thread(target=thread_demo, args=(x, ))
        thread.setDaemon(True)
        thread.start()
        debt_list.append(thread)

    is_thread_running = True

    while is_thread_running:
        debt_result = [thread.is_alive() for thread in debt_list]
        print debt_result
        if True in debt_result:
            continue
        else:
            is_thread_running = False

