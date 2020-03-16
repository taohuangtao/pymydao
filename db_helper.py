from .model import Model, Db
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class DbHelper(object):
    def __init__(self, host, username, password, dbname, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port

        self._db = None

    def get_model_instance(self, table=None):
        return Model(self.get_db(), table)

    def get_db(self):
        if self._db is None:
            self._db = Db(host=self.host, username=self.username, password=self.password, dbname=self.dbname,
                          port=self.port)
        return self._db

    def begin(self):
        self.get_db().begin()

    def commit(self):
        self.get_db().commit()

    def rollback(self):
        self.get_db().rollback()

    def transactional(self, func):
        """
        事务处理修饰器,加到对应的函数上面，为函数内所有数据库操作包装在一个事务内
        :return:
        """

        @wraps(func)
        def transaction_processing(*args, **kwargs):
            print(func.__name__ + " was called")
            self.get_db().begin()
            try:
                result = func(*args, **kwargs)
                self.get_db().commit()
                return result
            except BaseException as e:
                logger.exception("错误进行回滚", e)
                self.get_db().rollback()

        return transaction_processing
        pass
