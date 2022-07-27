#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :tools.py
# @Time      :2022/6/16 23:32
# @Author    :Colin
# @Note      :None


# date类型转ts
def date_to_ts(date_type):
    from datetime import datetime
    dt = datetime.combine(date_type, datetime.min.time())
    # datetime.fromtimestamp(p_date)
    return int(dt.timestamp())


# 获取数据库连接

def conn_to_db():
    import mysql.connector
    return mysql.connector.connect(user='root', password='',
                                   host='127.0.0.1',
                                   database='wechat_offacc')


# 转换df为元组
def df_to_tup(df):
    return [tuple(xi) for xi in df.values]


# 重命名sql查询的df
def query_to_df(cursor_query):
    import pandas as pd
    # 转换为df重命名并返回
    dict_columns = {i: cursor_query.column_names[i] for i in range(len(cursor_query.column_names))}
    df_cur = pd.DataFrame(cursor_query)
    df_cur.rename(columns=dict_columns, inplace=True)
    return df_cur
