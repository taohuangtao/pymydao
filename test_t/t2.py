
import sys
import threading
import time
from gevent import monkey

monkey.patch_all()
class myThread(threading.Thread):
    def __init__(self, threadID, name, counter,s):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
        self.s = s

    def run(self):
        print("开始线程：" + self.name)
        # print_time(self.name, self.counter, 5)
        while True:
            self.counter.dd = self.threadID
            if self.s>0:
                time.sleep(self.s)

            tt = threading.current_thread()
            print("tt {}".format(tt))
            print(self.name+" s  "+str(self.counter.dd) +"ttttttt")
            if self.counter.dd != self.threadID:
                print("error sdfjlskdjflskf")
                sys.exit(-1)
        print("退出线程：" + self.name)


if __name__ == "__main__":
    # t2()
    print(11)
    # time.sleep(1000)
    print(22)
    # 创建新线程
    lo = threading.local()
    tt = threading.current_thread()
    print("id {}".format(lo))
    print("tt {}".format(tt))
    thread1 = myThread(1, "Thread-1", lo,0.003)
    thread2 = myThread(2, "Thread-2", lo,0.001)
    thread3 = myThread(3, "Thread-3", lo,0.001)
    thread4 = myThread(4, "Thread-4", lo,0.004)
    lo.dd = 9999
    # 开启新线程
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    print("退出主线程")
