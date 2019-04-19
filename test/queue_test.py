from Queue import Queue

import threading

q = Queue(2)


def set_to_que(index):
    q.put(index)
    print  "input the value %s"%index


def get_from_que():
    print "\n"
    print "get the value is %s"%q.get()


threads = []


if __name__ == '__main__':
    index = 0
    while True:
        t1 = threading.Thread(target=set_to_que, args=(index, ))
        t1.start()
        if index % 2 == 0 :
            t2 = threading.Thread(target=get_from_que)
            t2.start()
        index = index + 1
        threads.append(t1)
        threads.append(t2)
        if index == 20:
            break
    print "test size %s" %q.qsize()


