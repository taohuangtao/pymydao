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

        self.__db = None

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
        if self.__db is None:
            self.__db = self.__Db(host=self.host, username=self.username, password=self.password,
                             dbname=self.dbname,
                             port=self.port)
        return self.__db

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
            logger.debug(func.__name__ + " was called")
            self.begin()
            try:
                result = func(*args, **kwargs)
                self.commit()
                return result
            except BaseException as e:
                logger.exception("错误进行回滚", e)
                self.rollback()
                raise e

        return transaction_processing
