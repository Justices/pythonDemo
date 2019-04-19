from multiprocessing import  Pool
import time


def print_index(index):
    time.sleep(1)
    print index

thread_pool = Pool(processes=10)


def main(limit):
    thread_pool.apply_async(print_index, [limit])
    thread_pool.map(print_index, range(limit))
    thread_pool.close()
    thread_pool.join()


if __name__ == '__main__':
    main(10)