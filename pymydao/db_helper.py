from .model import Model, Db
from functools import wraps
import logging
import threading
import warnings

logger = logging.getLogger(__name__)


class DbHelper(object):
    def __init__(self, host, username, password, dbname, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port
        # 按线程ID保存连接，每个线程只用当前线程的连接。gevent 的协程也可正常工作
        self.__local = threading.local()
        # 记录事务栈，处理事务重入，重入事务在同一个事务内
        self.__transaction = []

    def get_model_instance(self, table=None):
        return Model(self.__get_db(), table)

    def get_db(self):
        """
        过时的，未来版本中将会删除
        :return:
        """
        warnings.warn("The 'get_db' method is deprecated", DeprecationWarning, 2)
        return self.__get_db()

    class __Db(Db):
        def set_db_helper(self, db_helper):
            pass
            # self.__db_helper = db_helper

        def close(self):
            super().close()
            # thread_id = threading.get_ident()
            # self.__db_helper.__db[thread_id] = None

    def __get_db(self):
        # 在多线程或协程时
        # try:
        #     if self.__local.db is None:
        #         raise AttributeError()
        # except AttributeError as e:
        #     self.__local.db = Db(host=self.host, username=self.username, password=self.password,
        #                               dbname=self.dbname,
        #                               port=self.port)
        db = Db(host=self.host, username=self.username, password=self.password,
                             dbname=self.dbname,
                             port=self.port)
        return db

    def begin(self):
        self.__get_db().begin()

    def commit(self):
        self.__get_db().commit()

    def rollback(self):
        self.__get_db().rollback()

    def transactional(self, func):
        """
        事务处理修饰器,加到对应的函数上面，为函数内所有数据库操作包装在一个事务内
        :return:
        """

        @wraps(func)
        def transaction_processing(*args, **kwargs):
            self.__transaction.append(1)
            logger.debug(func.__name__ + " was called")
            self.__get_db().begin()
            ex = None
            try:
                result = func(*args, **kwargs)
                self.__get_db().commit()
                return result
            except BaseException as e:
                ex = e
                logger.exception("错误进行回滚", e)
                self.__get_db().rollback()
            self.__transaction.pop()
            if len(self.__transaction) == 0:
                self.__get_db().close()
            if ex is not None and len(self.__transaction) != 0:
                # 如果上层还有事务注解，再次将事务上抛
                raise ex

        return transaction_processing
        pass
