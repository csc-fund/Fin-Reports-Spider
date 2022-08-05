#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :mysql_tool.py
# @Time      :2022/8/1 10:19
# @Author    :Colin
# @Note      :None
import numpy as np

from tools.settings import *
import mysql.connector
import pandas as pd


class MysqlDao:
    # 初始化
    def __init__(self, host=MYSQL_HOST, userName=MYSQL_NAME, userPsd=MYSQL_PASSWORD, dataBase=MYSQL_DATABASE):
        self.host = host
        self.userName = userName
        self.userPsd = userPsd
        self.dataBase = dataBase

        # 建立连接
        self.cnx = mysql.connector.connect(user=self.userName, password=self.userPsd,
                                           host=self.host,
                                           database=self.dataBase)

        # 获取游标
        self.cur = self.cnx.cursor(buffered=True)

        # 转换后的保存在游标的数据
        self.sql = None  # sql语句
        self.df_cur = None  # 游标转为df
        self.tup_todb = None  # 准备入库的tup数据

    # 关闭连接
    def close_cnx(self):
        self.cnx.close()

    # 通用的执行语句
    def excute_sql(self, sql, method: str = 'one', tups=None) -> pd.DataFrame:
        self.sql = sql

        try:
            # 插入和更新
            if method == 'many':
                self.cur.executemany(self.sql, tups)

                self.cnx.commit()
            # 查询
            elif method == 'one':
                if tups is not None:
                    self.cur.execute(self.sql, tups)
                    return self.cur_to_df()
                else:
                    # 查询sql
                    self.cur.execute(self.sql)
                    return self.cur_to_df()

        except mysql.connector.Error as e:
            # logger.error(e)
            # print(self.tup_todb)

            print('mysql.connector.Error', e)

    # 重命名sql查询的df
    def cur_to_df(self) -> pd.DataFrame:
        if self.cur is not None:
            try:
                # 转换为df重命名并返回
                dict_columns = {i: self.cur.column_names[i] for i in range(len(self.cur.column_names))}
                self.df_cur = pd.DataFrame(self.cur)
                self.df_cur.rename(columns=dict_columns, inplace=True)
                return self.df_cur
            except TypeError:
                return pd.DataFrame()

        else:
            return pd.DataFrame()

    def df_to_tup(self, df):
        self.tup_todb = [tuple(xi) for xi in df.values]
        return self.tup_todb

    # 获得所有表名
    def show_tables(self) -> list:
        df_tables = self.excute_sql("SHOW TABLES")
        if not df_tables.empty:
            return self.excute_sql("SHOW TABLES").iloc[:, 0].tolist()

    # 查询表格所有字段并返回
    def select_columns(self, table_name: str) -> list:
        def excute():
            sql = (
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = {0}".format(
                    '\'' + table_name + '\'')
            )
            df_res = self.excute_sql(sql)
            if not df_res.empty:
                return df_res['COLUMN_NAME'].tolist()
            else:
                return None

        return excute()

    # 增加表格的字段
    # 传入字段列表,默认为VARCHAR
    def alter_table(self, table_name: str, column_list: list, type_dict: dict = None):
        # 解析自定义的新增类型
        def add_update_dict(column):

            # 从列名创建对应类型的字典
            column_dict = {i: 'TEXT' for i in column}

            # 如果有自定义的字典
            if type_dict is not None:
                # 只更新那些column有的键值对
                new_dict = {i: j for i, j in type_dict.items() if i in column}
                column_dict.update(new_dict)

            colunm_list = ['ADD COLUMN ' + '`' + i + '`' + ' ' + j for i, j in column_dict.items()]
            colunm_list = ','.join(colunm_list)

            return colunm_list

        # 解析column_list
        def transform_column_list():
            # 已经有的更新
            column_old = self.select_columns(table_name)

            # 如果查到了
            if column_old is not None:

                # 遍历出新增的字段
                column_new = [i for i in column_list if i not in column_old]

                # 如果有新的字段
                if column_new is not None:
                    # 更新自定义增加的字段类型
                    # 生成sql
                    colunm_update = add_update_dict(column_new)

                    return colunm_update
                else:
                    return None

            else:
                return None

        #
        def excute():
            add_column = transform_column_list()
            # 如果该列不存在默认增加VARCHAR
            if add_column:
                sql = ("ALTER TABLE {0}".format('`' + table_name + '`' + ' ') +
                       "{0}".format(add_column)
                       )
                self.excute_sql(sql)

        excute()

    # 检查完整性和修复
    # 默认为VARCHAR(255)
    def check_repair(self, check_table: str, check_column: list, type_dict: dict = None):
        def repair_attr():
            # 如果没有该collumn则创建一个
            self.alter_table(check_table, check_column, type_dict)

        def repair_table():
            table_list = self.show_tables()
            # 如果没有这个Table就创建一个
            if (table_list is None) or (check_table not in table_list):
                column_dict = {i: 'TEXT' for i in check_column}
                if type_dict is not None:
                    column_dict.update(type_dict)
                self.create_table(check_table, column_dict)


            # if check_table not in table_list:
            #     column_dict = {i: 'VARCHAR(255)' for i in check_column}
            #     if type_dict is not None:
            #         column_dict.update(type_dict)
            #     self.create_table(check_table, column_dict)

            # 如果有这个Table就创建新的字段
            else:
                # 修复字段完整性
                repair_attr()

        repair_table()

    # 按照表格名查找数据
    def select_table(self, table_name: str, select_column: list, filter_dict: dict = None,
                     select_count: bool = False) -> pd.DataFrame:

        def transform_list():
            # 查询全部
            if select_column == ['*']:
                colum_str = '*'

            # 查询行数
            elif select_count:
                colum_str = ['COUNT(`' + i + '`)' for i in select_column]
                colum_str += ['COUNT(*)']
                colum_str = ','.join(colum_str)

            # 字段查询
            else:
                # colum_strs
                colum_str = ['`' + i + '`' for i in select_column]
                colum_str = ','.join(colum_str)

            filter_str = filter_dict

            if filter_dict is not None:
                filter_str_1 = ['`' + i + '`' + ' = ' + str(j) for i, j in filter_dict.items() if
                                j not in ['NULL', 'NOT NULL'] and i != 'LIMIT' and not isinstance(j, list)]

                filter_str_2 = ['`' + i + '`' + ' IS ' + str(j) for i, j in filter_dict.items() if
                                str(j) in ['NULL', 'NOT NULL'] and not isinstance(j, list)]

                filter_str_3 = ['`' + i + '`' + ' BETWEEN ' + j[0] + ' AND ' + j[1] for i, j in filter_dict.items() if
                                isinstance(j, list) and len(j) == 2]

                filter_str = filter_str_1 + filter_str_2 + filter_str_3

                filter_str = ' AND '.join(filter_str)

                if filter_str != '':
                    filter_str = ' WHERE ' + filter_str

                filter_limit = [' LIMIT ' + str(j) for i, j in filter_dict.items() if i == 'LIMIT']
                filter_str = filter_str + ''.join(filter_limit)

            return colum_str, filter_str

        def excute():
            columns, filter_column = transform_list()
            sql = (
                "SELECT {0} FROM {1}".format(columns, '`' + table_name + '`')

            )
            if filter_column:
                sql += filter_column

            # print(sql)
            return self.excute_sql(sql)

        return excute()

    # 创建一个表格,传入表格名与字段名和类型,主键
    # create_table('testtable', {'test1': 'VARCHAR(25)', 'test2': 'VARCHAR(25)', 'PRIMARY KEY': 'test2'})
    def create_table(self, table_name: str, column_dict: dict):
        # 解析column_dict
        def transform_dict():
            # 主键处理
            pk_flag = 'PK'

            colum_list = ['`' + i + '`' + ' ' + j for i, j in column_dict.items() if i != pk_flag]

            # 主键追加NOT NULL
            if pk_flag in column_dict.keys():
                pk_column = column_dict[pk_flag]
                column_dict[pk_column] = column_dict[pk_column] + ' NOT NULL'
                colum_list += ['PRIMARY KEY ' + '(`' + column_dict[pk_flag] + '`)']

            colum_str = '(' + ','.join(colum_list) + ')'

            # 去掉连续逗号
            # print(colum_str)
            colum_str = colum_str.replace(',,', ',')

            return colum_str

        def excute():
            sql_column = transform_dict()

            sql = ("CREATE TABLE IF NOT EXISTS {0} {1}".format('`' + table_name + '`', sql_column)
                   )
            self.excute_sql(sql)

        excute()

    # 需要传入df
    def insert_table(self, table_name: str, df_values: pd.DataFrame, type_dict: dict = None, check_flag=True):

        def transform_df():
            df = pd.DataFrame(df_values)
            # 去除nan
            df = df.astype(object).where(pd.notnull(df), None)
            df = df.astype(object).where(pd.notna(df), None)
            df = df.astype(object).where(df != 'nan', None)

            # print(df['previous_create_date'], type(df['previous_create_date'].loc[6]))

            # column_str
            column_str = ['`' + i + '`' for i in df.columns]
            column_str = '(' + ','.join(column_str) + ')'

            # values_str
            values_str = ['%s' for _ in range(len(df.columns))]
            values_str = '(' + ','.join(values_str) + ')'

            # tup_values
            values_tup = self.df_to_tup(df)

            return column_str, values_str, values_tup

        def execute():
            # 获取要插入的数据
            column, values, tups = transform_df()

            # 创建sql语句
            sql = ("INSERT IGNORE INTO " + '`' + table_name + '`' + column +
                   " VALUES " + values
                   )

            # 执行sql语句
            self.excute_sql(sql, 'many', tups)

        if df_values.empty:
            # logging.logger.warning('INSERT {0} EMPTY DATAFRAME'.format(table_name))
            print(' EMPTY DATAFRAME')
            # logger.warn('INSERT {0} EMPTY DATAFRAME'.format(table_name))
            return

        if check_flag:
            self.check_repair(table_name, df_values.columns.tolist(), type_dict)

        execute()

    # 需要传入df
    def update_table(self, table_name: str, df_values: pd.DataFrame, type_dict: dict = None):
        # 转换插入的df
        def transform_df():
            df = pd.DataFrame(df_values)

            # update_str
            update_str = ['`' + i + '`' + '=%s' for i in df.columns]
            # 切分where
            where_str = update_str[-1:]
            where_str = ','.join(where_str)

            update_str = update_str[:-1]
            update_str = ','.join(update_str)

            # tup_values
            values_tup = self.df_to_tup(df)

            # 默认最后一列为where列

            return update_str, where_str, values_tup

        def execute():
            # 获取要插入的数据
            update_str, where_str, tups = transform_df()

            # 创建sql语句
            sql = ("UPDATE " + '`' + table_name + '` ' +
                   "SET " + update_str + " " +
                   "WHERE " + where_str)
            # 执行
            self.excute_sql(sql, 'many', tups)

        if df_values.empty:
            # logger.warn('UPDATE {0} EMPTY DATAFRAME'.format(table_name))
            return
        self.check_repair(table_name, df_values.columns.tolist(), type_dict)
        execute()
