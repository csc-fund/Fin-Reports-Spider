import pandas as pd

from tools.mysql_tool import MysqlDao
from wind_data.settings import *

import hashlib


class WindData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.df_from_db = pd.DataFrame()
        self.table_name = ''

    def add_code_column(self, table_name, code_column, id_column):
        self.df_from_db = self.SqlObj.select_table(table_name,
                                                   [code_column, id_column])
        self.df_from_db['CODE_NUM'] = self.df_from_db[code_column].apply(
            lambda x: str(x).split('.')[0]
        )
        self.df_from_db = self.df_from_db[['S_INFO_WINDCODE', 'S_INFO_CODE', 'OBJECT_ID']]
        self.SqlObj.update_table(table_name, self.df_from_db, {'S_INFO_CODE': 'VARCHAR(50)'})

    def select_table(self, table_name, code_column: str, date_column: str):
        self.table_name = table_name
        self.df_from_db = self.SqlObj.select_table(self.table_name, [code_column, date_column])
        self.df_from_db.rename(columns={code_column: 'CODE', date_column: 'DATE'}, inplace=True)

    # 生成用于对比的表
    def gen_compare_table(self):
        # 转换ID
        self.df_from_db['ID_MD5'] = self.df_from_db[['CODE', 'DATE']].apply(
            lambda x: str(x['CODE']).strip() + str(x['DATE']).strip(), axis=1)
        self.df_from_db['ID_MD5'] = self.df_from_db['ID_MD5'].apply(
            lambda x: hashlib.md5(x.encode('UTF-8')).hexdigest())

        self.SqlObj.insert_table(self.table_name + '_md5', self.df_from_db, INSERT_STRUCT)

    def create_compare_table(self):
        for table_name, column in TABLE_LIST.items():
            # print(table_name, column)
            self.select_table(table_name, column[0], column[1])
            self.gen_compare_table()

        # self.select_table('ashareprofitnotice',[])
        # self.gen_compare_table(['CODE', 'DATE'])

    def add_md5_column(self):
        for table_name, column in TABLE_LIST.items():
            # ---------------- 初始化---------------#
            code_column, date_column, id_column = column[0], column[1], column[2]
            self.df_from_db = self.SqlObj.select_table(table_name, column)

            # ---------------- 转为短代码----------------#
            self.df_from_db['CODE'] = self.df_from_db[code_column].apply(
                lambda x: str(x).split('.')[0] if '.' in str(x) else x
            )
            # ---------------- 转为MD5----------------#
            self.df_from_db['ID_MD5'] = self.df_from_db[[code_column, date_column]].apply(
                lambda x: str(x[code_column]).strip() + str(x[date_column]).strip(), axis=1)

            self.df_from_db['ID_MD5'] = self.df_from_db['ID_MD5'].apply(
                lambda x: hashlib.md5(x.encode('UTF-8')).hexdigest())
            # ---------------- 入库----------------#
            self.SqlObj.update_table(table_name, self.df_from_db, INSERT_STRUCT)


# WindData().add_code_column('ashareprofitnotice')
WindData().add_md5_column()
