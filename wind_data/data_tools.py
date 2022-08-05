from tools.mysql_tool import MysqlDao
from wind_data.settings import *

import hashlib


class WindData:
    def __init__(self):
        self.SqlObj = MysqlDao()
        self.yjyg_from_db = self.SqlObj.select_table(TABLE_YJYG, YJYG_COLUMN, {"LIMIT": "10"})

    # 生成用于对比的表
    def gen_compare_table(self):
        self.yjyg_from_db[MD5_NAME] = self.yjyg_from_db[MD5_COLUMN].apply(
            lambda x: hashlib.md5(str(str(x[0]) + str(x[1])).encode('UTF-8')).hexdigest(), axis=1)

        self.yjyg_from_db['S_INFO_CODE'] = self.yjyg_from_db['S_INFO_WINDCODE'].apply(
            lambda x: str(x).split('.')[0]
        )
        self.SqlObj.insert_table(INSERT_TABLE, self.yjyg_from_db, INSERT_STRUCT)


WindData().gen_compare_table()
