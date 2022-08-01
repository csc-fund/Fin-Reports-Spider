from mysql_tool import MysqlDao
from settings import *
import tushare as ts
import pandas as pd
import hashlib


class TushareSpider:
    # 初始化参数
    def __init__(self):
        # 参数设置

        # 实例化Tushare对象
        self.TuShare = ts.pro_api('56a12424870cd0953907cde2c660b498c8fe774145b7f17afdc746dd')

        # 实例化数据库操作对象
        self.SqlObj = MysqlDao()

        # 保存的查询结果
        self.df_ts = None

    # 通用Tushare查询
    def get_query(self, api_name, fields='', **kwargs) -> pd.DataFrame:
        self.df_ts = self.TuShare.query(api_name=api_name, fields=fields, params=kwargs)
        return self.df_ts

    # 财务数据
    def get_financial_reports(self, period: str):
        self.df_ts = self.TuShare.query(api_name='forecast_vip', period=period,
                                        fields='ts_code,ann_date,end_date,type,'
                                               'p_change_min,p_change_max,'
                                               'net_profit_min,net_profit_max,'
                                               'last_parent_net,first_ann_date,'
                                               'summary,change_reason')
        # 建立主键
        # Tushare有重复的业绩预告,内容不完全一只,或者更新引起的
        self.df_ts['ID'] = self.df_ts[['ts_code', 'ann_date']].apply(
            lambda x: hashlib.md5(str(x['ts_code'] + x['ann_date']).encode('UTF-8')).hexdigest(), axis=1)
        # df_dup = self.df_ts[self.df_ts.duplicated('ID')]
        # print(df_dup.shape)
        # 入库
        self.SqlObj.insert_table('yjyg', self.df_ts,
                                 {'PK': 'ID', 'ann_date': 'DATE', 'end_date': 'DATE', 'first_ann_date': 'DATE', })
        self.SqlObj.close_cnx()
    # print(msd.cur.statement, msd.cur.rowcount, )


app = TushareSpider()
app.get_financial_reports('20181231')
print(app.SqlObj.cur.statement)
