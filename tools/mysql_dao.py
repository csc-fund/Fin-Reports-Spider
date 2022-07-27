#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :mysql_dao.py
# @Time      :2022/6/20 09:06
# @Author    :Colin
# @Note      :None


import pandas as pd
from tools import __config as gv
from log_rec.log import Logger

logger = Logger(logname=gv.LOG_PATH + __name__ + '.log', loggername=__name__).getlog()


def df_to_tup(df):
    return [tuple(xi) for xi in df.values]


# 获得所有表名
def show_tables():
    def excute():
        sql = (
            "SHOW TABLES"
        )
        return excute_sql(sql).iloc[:, 0].tolist()

    return excute()


# 重命名sql查询的df
def query_to_df(cursor_query) -> pd.DataFrame:
    if cursor_query is not None:
        # 转换为df重命名并返回
        dict_columns = {i: cursor_query.column_names[i] for i in range(len(cursor_query.column_names))}
        df_cur = pd.DataFrame(cursor_query)
        df_cur.rename(columns=dict_columns, inplace=True)
        return df_cur
    else:
        return pd.DataFrame()


# 通用的执行语句
def excute_sql(sql, method: str = 'one', tups=None) -> pd.DataFrame:
    import mysql.connector

    cnx = mysql.connector.connect(user='root', password='',
                                  host='127.0.0.1',
                                  database='wechat_offacc')
    cur = cnx.cursor(buffered=True)

    # 查询以外的sql
    try:
        # 插入和更新
        if method == 'many':
            cur.executemany(sql, tups)
            cnx.commit()
        #     查询
        elif method == 'one':
            if tups is not None:
                cur.execute(sql, tups)
                return query_to_df(cur)
            else:
                # 查询sql
                cur.execute(sql)
                return query_to_df(cur)

    except mysql.connector.Error as e:
        logger.error(e)
    finally:
        debug_str = gv.DEBUG_STR.format(str(cur.statement), str(cur.rowcount))
        # logging.info(debug_str)
        # logging.set_stream_level(debug_str)
        # print(logging.logger.level)
        # print(logging.lo)
        # logger = Logger('log_file/SQL_Execute' + ".log", __name__).getlog()
        logger.info(debug_str)

        if gv.DEBUG_MODE:
            pass
            # print(debug_str)
        cur.close()
        cnx.close()


# 查询表格所有字段并返回
def select_columns(table_name: str) -> list:
    def excute():
        sql = (
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = {0}".format('\'' + table_name + '\'')
        )
        df_res = excute_sql(sql)
        if not df_res.empty:
            return df_res['COLUMN_NAME'].tolist()
        else:
            return None

    return excute()


# 弃用
def select_table_count(table_name: str, select_column: list):
    def select_all_row():
        sql = 'SELECT COUNT(*) FROM {0}'.format(table_name)
        return excute_sql(sql)

    def transform_list():
        pass

    def excute():
        count = select_all_row()
        print(count)

    excute()


# 按照表格名查找数据
#
def select_table(table_name: str, select_column: list, filter_dict: dict = None,
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
        return excute_sql(sql)

    return excute()


# 增加表格的字段
# 传入字段列表,默认为VARCHAR
def alter_table(table_name: str, column_list: list, type_dict: dict = None):
    # 解析自定义的新增类型
    def add_update_dict(column):

        # 从列名创建对应类型的字典
        column_dict = {i: 'VARCHAR(255)' for i in column}

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
        column_old = select_columns(table_name)

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
            excute_sql(sql)

    excute()


# 检查完整性和修复
# 默认为VARCHAR(255)
def check_repair(check_table: str, check_column: list, type_dict: dict = None):
    def repair_attr():
        # 如果没有该collumn则创建一个
        alter_table(check_table, check_column, type_dict)

    def repair_table():
        table_list = show_tables()
        # 如果没有这个Table就创建一个

        if check_table not in table_list:
            print(check_table, table_list)
            column_dict = {i: 'VARCHAR(255)' for i in check_column}
            if type_dict is not None:
                column_dict.update(type_dict)
            create_table(check_table, column_dict)

        # 如果有这个Table就创建新的字段
        else:
            # 修复字段完整性
            repair_attr()

    repair_table()


# 创建一个表格,传入表格名与字段名和类型,主键
# 例如
# create_table('testtable', {'test1': 'VARCHAR(25)', 'test2': 'VARCHAR(25)', 'PRIMARY KEY': 'test2'})
def create_table(table_name: str, column_dict: dict):
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
        excute_sql(sql)

    excute()


# 需要传入df
def insert_table(table_name: str, df_values: pd.DataFrame, type_dict: dict = None, check_flag=True):
    # 转换插入的df
    def transform_df():
        df = pd.DataFrame(df_values)

        # column_str
        column_str = ['`' + i + '`' for i in df.columns]
        column_str = '(' + ','.join(column_str) + ')'

        # values_str
        values_str = ['%s' for i in range(len(df.columns))]
        values_str = '(' + ','.join(values_str) + ')'

        # tup_values
        values_tup = df_to_tup(df)

        return column_str, values_str, values_tup

    def execute():
        # 获取要插入的数据
        column, values, tups = transform_df()

        # 创建sql语句
        sql = ("INSERT IGNORE INTO " + '`' + table_name + '`' + column +
               " VALUES " + values
               )

        # 执行sql语句
        excute_sql(sql, 'many', tups)

    if df_values.empty:
        # logging.logger.warning('INSERT {0} EMPTY DATAFRAME'.format(table_name))
        print('\n')
        logger.warn('INSERT {0} EMPTY DATAFRAME'.format(table_name))
        return

    if check_flag:
        check_repair(table_name, df_values.columns.tolist(), type_dict)

    execute()


# 需要传入df
def update_table(table_name: str, df_values: pd.DataFrame, type_dict: dict = None):
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
        values_tup = df_to_tup(df)

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
        excute_sql(sql, 'many', tups)

    if df_values.empty:
        logger.warn('UPDATE {0} EMPTY DATAFRAME'.format(table_name))
        return
    check_repair(table_name, df_values.columns.tolist(), type_dict)
    execute()


#
def test_demo():
    """
    :return:用于测试
    :note:
    """
    from data_down import tushare_api
    tu = tushare_api.TuShareGet('20120101', '20220601')
    # 获取的指数
    df_kline = pd.DataFrame(tu.get_index('000001.SH'))

    insert_table('test_long', df_kline,
                 {'PK': 'trade_date', 'ts_code': 'VARCHAR(40)', 'date_ts': 'INT', 'trade_date': 'INT'})

    # update_table('test1', )


# test_demo()
# create_table('testtable2', {'test1': 'VARCHAR(25)', 'test2': 'VARCHAR(25)', 'PK': 'test1'})
#
# data = {'test1': ['21', '21', '21'], 'test1111': ['1', '2', '3']
#         }
# insert_table('testtable2', data)

# data = {'test1111': ['111', '222', '333'], 'test1': ['1', '2', '3']
#         }
# update_table('testtable2', data)
# print(select_columns('testtable2'))
# alter_table('testtable2', ['testfloat', ])

def test_add():
    alter_table('000001.SH', ['eee1', 'eee2', 'eee3'], {'eee1': 'FLOAT', 'eee2': 'FLOAT', 'eee3': 'FLOAT', })


def test_select():
    df = select_table('test', ['*'], {'id': '002a737b91f464ddcaf6da7c417981b7', 'LIMIT': 10})
    print(df)
