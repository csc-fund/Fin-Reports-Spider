import time

from mysql_tool import MysqlDao
from tushare_spider.settings import *
import tushare as ts
import pandas as pd
import hashlib


class TushareSpider:
    # 初始化参数
    def __init__(self):
        # 参数设置
        self.TUSHARE_VIPAPI = TUSHARE_VIPAPI  # API列表

        # 爬取状态
        self.TRACK_DATE = None
        self.TRACK_BOARD = None

        # 实例化Tushare对象
        self.TuShare = ts.pro_api('56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd')

        # 实例化数据库操作对象
        self.SqlObj = MysqlDao()

        # 保存的查询结果
        self.df_ts = None

    # 获取财务数据
    def get_financial_data(self, period):
        for api in self.TUSHARE_VIPAPI:
            # ----------------初始化状态----------------#
            self.TRACK_BOARD = api

            # ----------------执行查询----------------#
            time.sleep(1)  # 1s一次可以
            self.df_ts = self.TuShare.query(api_name=api, period=period, )
            if self.df_ts.empty:  # 判断为空
                continue

            # ---------------- 入库----------------#
            try:
                # 生成主键
                self.df_ts['ID'] = self.df_ts[['ts_code', 'ann_date']].apply(
                    lambda x: hashlib.md5(str(x['ts_code'] + x['ann_date']).encode('UTF-8')).hexdigest(), axis=1)
                self.SqlObj.insert_table(api, self.df_ts,
                                         {'INSERT_DATETIME': 'DATETIME NULL DEFAULT CURRENT_TIMESTAMP',
                                          'UPDATE_DATETIME': 'DATETIME NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                                          'ID': 'VARCHAR(255)',
                                          'PK': 'ID', 'ann_date': 'DATE', })
            except KeyError:
                print('KeyError')
                print(self.df_ts.columns)
            finally:
                # ----------------输出状态----------------#
                print('[{}/{}] {} {}  '.format(self.SqlObj.cur.rowcount, self.df_ts.shape[0],
                                               self.TRACK_DATE, self.TRACK_BOARD, ))

    # 获取历史信息
    def get_historical_data(self):
        date_list = [str(i).replace(' 00:00:00', '') for i in
                     (pd.date_range(start='20000331', end='20221231', freq='3M').format(date_format="%Y%m%d"))]
        for date in date_list:
            self.TRACK_DATE = date
            self.get_financial_data(period=self.TRACK_DATE)


app = TushareSpider()
app.get_historical_data()
app.SqlObj.close_cnx()
