from threading import BoundedSemaphore
import threading
import time
NUM = 10

def func(lock):
    global NUM
    lock.acquire()
    # lock.acquire()
    NUM -= 1
    time.sleep(2)
    print(NUM)
    # lock.release()
    lock.release()

Lock = threading.Lock()   #
# RLock = threading.RLock()   #

# for i in range(10):
#     t = threading.Thread(target=func,args=(Lock,))
#     t.start()


se_lock = BoundedSemaphore(5)

NUM = 10


def sem_func(i, lock):
    global NUM
    lock.acquire()
    NUM -=1
    time.sleep(4)
    print('NUM:', str(NUM), 'i:', i)
    lock.release()


for i in range(30):
    t = threading.Thread(target=sem_func, args=(i, se_lock, ))
    t.start()