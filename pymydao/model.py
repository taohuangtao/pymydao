import logging
import warnings

from pymysql.connections import Connection

try:
    from greenlet import getcurrent as get_ident
except ImportError:
    try:
        from thread import get_ident
    except ImportError:
        from _thread import get_ident
logger = logging.getLogger(__name__)


class Db(object):
    """
    事务重入将在一个事务内
    代理连接，在开启事务后，所有操作都在一个连接内，只有事务回滚或者提交才可关闭连接
    """

    def __init__(self, host, username, password, dbname, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port

        # 本地对象，当前线程或协程对象，每个线程或协程是独立的连接，互不干扰
        self.__local = {}

    def begin(self):
        if len(self.__get_local()['transaction']) == 0:
            self.__get_connect().begin()
        self.__get_local()['transaction'].append(1)

    def commit(self):
        if len(self.__get_local()['transaction']) == 1:
            self.__get_connect().commit()
        self.__get_local()['transaction'].pop()

    def rollback(self):
        if len(self.__get_local()['transaction']) == 1:
            self.__get_connect().rollback()
        self.__get_local()['transaction'].pop()

    def __get_local(self):
        """
        获取本地对象，每个线程或协程对象独立
        :return:
        """
        thread_id = get_ident()
        try:
            self.__local[thread_id]
        except KeyError as e:
            self.__local[thread_id] = {
                'transaction': [],
                'connect': None
            }
        return self.__local[thread_id]

    def __del_local(self):
        """
        删除本地对象
        :return:
        """
        thread_id = get_ident()
        try:
            del self.__local[thread_id]
        except KeyError as e:
            pass

    def close(self):
        """
        关闭连接
        :return:
        """
        if len(self.__get_local()['transaction']) != 0:
            # 有事务，不关闭连接
            return
        if self.__get_local()['connect'] is not None:
            self.__get_local()['connect'].close()
            self.__get_local()['connect'] = None
        # 删除本地对象，避免内存溢出
        self.__del_local()

    class __Connect(Connection):
        def close(self):
            super().close()

    def __get_connect(self):
        if self.__get_local()['connect'] is None:
            self.__get_local()['connect'] = self.__Connect(host=self.host, user=self.username, password=self.password,
                                                           db=self.dbname,
                                                           port=self.port, use_unicode=True, charset="utf8",
                                                           autocommit=True)
        return self.__get_local()['connect']

    def execute(self, sql, args=None):
        logger.debug(sql)
        logger.debug(args)

        cursor = None
        try:
            db = self.__get_connect()
            cursor = db.cursor()
            data = cursor.execute(sql, args)
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
            db = self.__get_connect()
            cursor = db.cursor()
            data = cursor.executemany(sql, args)
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
            db = self.__get_connect()
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
        warnings.warn("The 'get_model_instance' method is deprecated", DeprecationWarning, 2)
        return Model(self, table)


class Model(object):

    def __init__(self, db, table=None):
        self.__db = db
        self.__table = table
        self.__last_insert_id = 0

    def __str__(self):
        return self.__table

    def close(self):
        """
        手动关闭数据库连接
        过时的，未来版本会删除
        :return:
        """
        warnings.warn("The 'close' method is deprecated, "
                      "The connection is closed automatically", DeprecationWarning, 2)
        self.__db.close()

    def __insert(self, data):
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
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (self.__table, ",".join(cols), ",".join(ds))

        return self.__db.execute(insert_sql, dat_list)

    def __insert_batch(self, data_list):
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
        insert_sql = "INSERT INTO `%s`(%s) VALUES(%s)" % (self.__table, ",".join(cols_sql), ",".join(ds))
        # 执行插入
        tuple_list = []
        for row in data_list:
            d = []
            for c in cols:
                d.append(row[c])
            tuple_list.append(d)
        return self.__db.executemany(insert_sql, tuple_list)

    def insert(self, data):
        """
        插入数据，支持批量插入和单条插入
        :param data: dict or list(dict)
        :return: int
        """
        result = None
        if isinstance(data, list):
            result = self.__insert_batch(data)
        elif isinstance(data, dict):
            result = self.__insert(data)
        else:
            pass
        # 插入内容，保存自增主键，每次插入都获取自增ID，因为要多一次查询(实际上不一定每次都需要自增ID)在高并发情况下会降低并发度
        # 但每次操作完成需要关闭连接，后期考虑如何优化
        self.__get_insert_id()
        self.__db.close()
        return result

    def __get_insert_id(self):
        """
        将自增ID保存到当前对象中
        :return:
        """
        info = self.__db.select("SELECT LAST_INSERT_ID()")
        self.__last_insert_id = info[0]["LAST_INSERT_ID()"]

    def get_insert_id(self):
        """
        单条插入可通过此方法获取自增主键
        :return: id
        """
        last_insert_id = self.__last_insert_id
        self.__last_insert_id = 0
        return last_insert_id

    def select(self, sql, args=None):
        result = self.__db.select(sql, args)
        self.__db.close()
        return result

    def execute(self, sql, args=None):
        result = self.__db.execute(sql, args)
        self.__db.close()
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
        sql = "UPDATE  " + self.__table + " SET " + " , ".join(sets) + " WHERE " + " AND ".join(whes)
        logger.debug("update sql : %s" % sql)
        result = self.__db.execute(sql, argv)
        self.__db.close()
        return result
