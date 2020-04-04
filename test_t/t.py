from pymydao import db_helper
import time
from gevent import monkey
monkey.patch_all()
import threading


def t2(tid):
    print(1)
    dbh = db_helper.DbHelper("172.16.2.95", "yuqing_v1", "yuqing_v1", "yuqing")
    um = dbh.get_model_instance("user")
    ll = um.select("SELECT * FROM `user` LIMIT 1")
    print(ll)
    # dbh = db_helper.DbHelper("172.16.1.78", "root", "new-password", "yuqing")
    um = dbh.get_model_instance("user")
    ll = um.select("SELECT * FROM `user` LIMIT 1")
    print(ll)
    db = dbh.get_db()
    # db.begin()
    # ll = db.select("SELECT * FROM `user` LIMIT 1 FOR UPDATE")
    while True:
        ll = db.select("SELECT * FROM `user` LIMIT 1 FOR UPDATE")
        db.close()
        # ll = um.select("SELECT * FROM `user` LIMIT 1")
        print(tid)
        print(ll)
        time.sleep(1)
    print(ll)


exitFlag = 0


class myThread(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        print("开始线程：" + self.name)
        # print_time(self.name, self.counter, 5)
        print("退出线程：" + self.name)
        t2(self.threadID)


def print_time(threadName, delay, counter):
    while counter:
        if exitFlag:
            threadName.exit()
        time.sleep(delay)
        print("%s: %s" % (threadName, time.ctime(time.time())))
        counter -= 1


if __name__ == "__main__":

    # t2()
    print(11)
    # time.sleep(1000)
    print(22)
    # 创建新线程
    thread1 = myThread(1, "Thread-1", 1)
    thread2 = myThread(2, "Thread-2", 2)

    # 开启新线程
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    print("退出主线程")
