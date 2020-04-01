from pymysql.connections import Connection
from pymysql.connections import Cursor
import logging
import warnings

logger = logging.getLogger(__name__)


class Db(object):

    def __init__(self, host, username, password, dbname, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port

        self._connect = None

    def begin(self):
        self.get_connect().begin()
        pass

    def commit(self):
        self.get_connect().commit()
        pass

    def rollback(self):
        self.get_connect().rollback()
        pass

    class _Connect(Connection):
        def __init__(self, host, user, password, db, port, use_unicode, charset):
            super().__init__(host=host, user=user, password=password,
                  database=db, port=port,
                  use_unicode=use_unicode, charset=charset)
            # 事务，同一个连接，多次开启事务，实际只有第一个有效
            self._transaction = []

        def begin(self):
            if len(self._transaction) == 0:
                super().begin()
            self._transaction.append(1)

        def commit(self):
            if len(self._transaction) == 1:
                super().commit()
            self._transaction.pop()

        def rollback(self):
            if len(self._transaction) == 1:
                super().rollback()
            self._transaction.pop()

        def close(self):
            """
            有事务未处理，不能关闭连接
            :return:
            """
            if len(self._transaction) == 0:
                super().close()

    def get_connect(self):
        if self._connect is None:
            self._connect = self._Connect(host=self.host, user=self.username, password=self.password, db=self.dbname,
                                          port=self.port, use_unicode=True, charset="utf8")
        return self._connect

    def close(self):
        """
        关闭连接
        :return:
        """
        if self._connect is not None:
            self._connect.close()
            self._connect = None

    def execute(self, sql, args=None):
        logger.debug(sql)
        logger.debug(args)

        cursor = None
        try:
            db = self.get_connect()
            cursor = db.cursor()
            data = cursor.execute(sql, args)
            db.commit()
            return data
        except BaseException as e:
            logger.debug("sql %s" % sql)
            logger.debug("args %s" % args)
            raise e
        finally:
            if cursor is not None:
                cursor.close()

    def executemany(self, sql, args):
        logger.debug(sql)
        logger.debug(args)
        cursor = None
        try:
            db = self.get_connect()
            cursor = db.cursor()
            data = cursor.executemany(sql, args)
            db.commit()
            return data
        except BaseException as e:
            # logger.exception(e)
            logger.debug("sql %s" % sql)
            logger.debug("args %s" % args)
            raise e
        finally:
            if cursor is not None:
                cursor.close()

    def select(self, sql, args=None):
        logger.debug(sql)
        logger.debug(args)
        cursor = None
        try:
            db = self.get_connect()
            cursor = db.cursor()
            cursor.execute(sql, args)

            col = cursor.description
            list_n = cursor.fetchall()
            list_dat = []
            for row in list_n:
                rm = {}
                for i in range(len(col)):
                    # logger.debug(col[i][0])
                    rm[col[i][0]] = row[i]

                list_dat.append(rm)

            return list_dat
        except BaseException as e:
            # logger.exception(e)
            logger.debug("sql %s" % sql)
            logger.debug("args %s" % str(args))
            raise e
        finally:
            if cursor is not None:
                cursor.close()

    def get_model_instance(self, table=None):
        return Model(self, table)


class Model(object):

    def __init__(self, db, table=None):
        self._db = db
        self._table = table
        self._last_insert_id = 0

    def __str__(self):
        return self._table

    def close(self):
        """
        手动关闭数据库连接
        过时的，未来版本会删除
        :return:
        """
        warnings.warn("The 'close' method is deprecated, "
                      "use 'warning' instead", DeprecationWarning, 2)
        self._db.close()

    def _insert(self, data):
        """
        单条插入
        :param data:
        :return:
        """
        # 拼接sql
        cols = []
        ds = []
        dat_list = []
        for (k, v) in data.items():
            cols.append("`" + k + "`")
            ds.append("%s")
            dat_list.append(v)
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (self._table, ",".join(cols), ",".join(ds))

        return self._db.execute(insert_sql, dat_list)

    def _insert_batch(self, data_list):
        """
        批量插入
        :param data_list:
        :return:
        """
        logger.debug(data_list[0])
        # 拼接sql
        cols_sql = []
        cols = []
        ds = []
        for (k, v) in data_list[0].items():
            cols_sql.append("`" + k + "`")
            cols.append(k)
            ds.append("%s")
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (self._table, ",".join(cols_sql), ",".join(ds))
        # 执行插入
        tuple_list = []
        for row in data_list:
            d = []
            for c in cols:
                d.append(row[c])
            tuple_list.append(d)
        return self._db.executemany(insert_sql, tuple_list)

    def insert(self, data):
        """
        插入数据，支持批量插入和单条插入
        :param data: dict or list(dict)
        :return: int
        """
        result = None
        if isinstance(data, list):
            result = self._insert_batch(data)
        elif isinstance(data, dict):
            result = self._insert(data)
        else:
            pass
        # 插入内容，保存自增主键，每次插入都获取自增ID，因为要多一次查询(实际上不一定每次都需要自增ID)在高并发情况下会降低并发度
        # 但每次操作完成需要关闭连接，后期考虑如何优化
        self._get_insert_id()
        self._db.close()
        return result

    def _get_insert_id(self):
        """
        将自增ID保存到当前对象中
        :return:
        """
        info = self._db.select("SELECT LAST_INSERT_ID()")
        self._last_insert_id = info[0]["LAST_INSERT_ID()"]

    def get_insert_id(self):
        """
        单条插入可通过此方法获取自增主键
        :return: id
        """
        last_insert_id = self._last_insert_id
        self._last_insert_id = 0
        return last_insert_id

    def select(self, sql, args=None):
        result = self._db.select(sql, args)
        self._db.close()
        return result

    def execute(self, sql, args=None):
        result = self._db.execute(sql, args)
        self._db.close()
        return result

    def update(self, data, where):
        """
        更新数据库记录
        :param data: dict
        :param where: dict
        :return:
        """
        sets = []
        argv = []
        for (k, v) in data.items():
            sets.append("`%s` = %%s" % k)
            argv.append(v)
        whes = []
        for (k, v) in where.items():
            whes.append("`%s` = %%s" % k)
            argv.append(v)
        sql = "UPDATE  " + self._table + " SET " + " , ".join(sets) + " WHERE " + " AND ".join(whes)
        logger.debug("update sql : %s" % sql)
        result = self.execute(sql, argv)
        self._db.close()
        return result
