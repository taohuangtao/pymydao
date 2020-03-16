from pymysql.connections import Connection
import logging

logger = logging.getLogger(__name__)


class Db(object):

    def __init__(self, host, username, password, dbname, port=3306):
        self.host = host
        self.username = username
        self.password = password
        self.dbname = dbname
        self.port = port

        self._connect = None

        self._auto_commit = True

    def begin(self):
        self._auto_commit = False
        pass

    def commit(self):
        self.get_connect().commit()
        self._auto_commit = True
        pass

    def rollback(self):
        self.get_connect().rollback()
        self._auto_commit = True
        pass

    class _Connect(Connection):
        def close(self):
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
            if self._auto_commit:
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
            if self._auto_commit:
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

    def __str__(self):
        return self._table

    def close(self):
        """
        手动关闭数据库连接
        :return:
        """
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
        if isinstance(data, list):
            return self._insert_batch(data)
        elif isinstance(data, dict):
            return self._insert(data)
        else:
            pass
        pass

    def get_insert_id(self):
        """
        单条插入可通过此方法获取自增主键
        :return: id
        """

        info = self.select("SELECT LAST_INSERT_ID()")
        return info[0]["LAST_INSERT_ID()"]

    def select(self, sql, args=None):
        return self._db.select(sql, args)

    def execute(self, sql, args=None):
        return self._db.execute(sql, args)

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
        logger.info("update sql : %s" % sql)
        return self.execute(sql, argv)
